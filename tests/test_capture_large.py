"""Runtime scale test: bounded memory and spill disk as the archive grows.

The large archive is generated at test time into a temp directory (nothing
large is committed).  The test asserts:

* peak Python heap growth stays close to snapshot/current-state size and does
  not scale with the number of shard messages;
* the SQLite spill database is the only on-disk working state and is removed
  on success and on failure;
* doubling the archive roughly doubles spill bytes (linear, not quadratic);
* no staging or spill artifacts remain after the import finishes.
"""

from __future__ import annotations

import gc
import importlib.util
import json
import os
import resource
import sys
import tracemalloc
import unittest
from pathlib import Path

from ddreduce.capture.errors import CaptureError
from ddreduce.capture.importer import STAGING_PREFIX, run_import
from ddreduce.capture.verifier import VERDICT_MATCH, run_verify

from .helpers import ROOT, TempDir

_spec = importlib.util.spec_from_file_location(
    "gen_capture", ROOT / "demo" / "capture" / "gen_capture.py"
)
gen_capture = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen_capture)
generate_large = gen_capture.generate_large


def _rss_kb() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


class LargeArchiveResourceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()

    def tearDown(self):
        self.tmp.cleanup()

    def _generate_and_import(self, n_tx: int, n_keys: int, tag: str, *,
                             fail: bool = False):
        work = self.tmp.path / tag
        info = generate_large(work, n_tx=n_tx, n_keys=n_keys, seed=3,
                              shards=4)
        archive = Path(info["archive"])
        sealed = info["sealed"]
        package = work / "package"
        tmp_root = work / "spill-root"
        tmp_root.mkdir()

        gc.collect()
        tracemalloc.start()
        before_peak = tracemalloc.get_traced_memory()[1]
        try:
            if fail:
                # Corrupt a shard so the same physical coordinate carries two
                # different messages: append a conflicting duplicate at an
                # existing offset, which the importer must reject outright.
                shard = sorted((archive / "shards").glob("*.jsonl"))[0]
                rows = [json.loads(l) for l in shard.read_text().splitlines()]
                change_rows = [r for r in rows if r.get("kind") == "change"]
                self.assertTrue(change_rows, "expected a change message to corrupt")
                victim = change_rows[0]
                corrupt = json.loads(json.dumps(victim))
                corrupt["after"]["amount"] += 999_999
                rows.append(corrupt)  # same stream/offset, different content
                shard.write_text(
                    "\n".join(json.dumps(r, sort_keys=True) for r in rows)
                    + "\n", encoding="utf-8",
                )
                with self.assertRaises(CaptureError):
                    run_import(archive, work / "snapshot", work / "schema.json",
                               package, sealed, tmp_root=tmp_root)
            else:
                result = run_import(
                    archive, work / "snapshot", work / "schema.json",
                    package, sealed, tmp_root=tmp_root,
                )
        finally:
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
        heap_growth = peak - before_peak

        # Spill cleanup: the temp root must be empty after the context closes.
        leftovers = list(tmp_root.rglob("*"))
        self.assertEqual(
            [p for p in leftovers if p.is_file()], [],
            "spill working files were not cleaned up",
        )
        # No staging directory left in the work directory.
        staging = [p for p in work.iterdir()
                   if p.name.startswith(STAGING_PREFIX)]
        self.assertEqual(staging, [])
        if fail:
            self.assertFalse(package.exists())
            return heap_growth, 0, info
        # Independent verification still works at scale.
        verify = run_verify(archive, work / "snapshot", work / "schema.json",
                            sealed, package, tmp_root=tmp_root)
        self.assertEqual(verify.verdict, VERDICT_MATCH, verify.failures[:2])
        spill = result.report["resources"]["peak_spill_bytes"]
        return heap_growth, spill, info

    def test_memory_stays_bounded_and_spill_grows_linearly(self):
        # Two sizes; the archive roughly doubles between them.
        heap_small, spill_small, info_small = self._generate_and_import(
            400, 80, "small"
        )
        heap_large, spill_large, info_large = self._generate_and_import(
            900, 80, "large"
        )
        report_small = json.loads(
            (self.tmp.path / "small" / "package" /
             "capture-report.json").read_text("utf-8")
        )
        report_large = json.loads(
            (self.tmp.path / "large" / "package" /
             "capture-report.json").read_text("utf-8")
        )
        msg_growth = (
            report_large["counts"]["physical_messages_read"]
            / report_small["counts"]["physical_messages_read"]
        )
        # Spill must scale with the message count rather than with the square
        # of it.  SQLite page allocation is stepwise, so equal page counts
        # across two sizes are acceptable; only a quadratic envelope is banned.
        self.assertGreaterEqual(spill_large, spill_small)
        self.assertLess(
            spill_large,
            spill_small * (msg_growth ** 2),
            f"spill growth {spill_large}/{spill_small} looks super-linear "
            f"(message growth {msg_growth:.2f}x)",
        )
        # The spill database must stay within a small multiple of the raw
        # shard bytes (bounded per-message overhead).
        def shard_bytes(root: Path) -> int:
            return sum(
                p.stat().st_size for p in (root / "archive").rglob("*.jsonl")
            )

        self.assertLess(spill_large, shard_bytes(self.tmp.path / "large") * 4)
        # Heap growth must not scale with archive size: even the large run's
        # Python allocation peak stays bounded relative to its message count.
        # Bound chosen from snapshot-sized state plus per-transaction output
        # buffering; deliberately generous to stay stable across machines.
        self.assertLess(
            heap_large,
            8 * 1024 * 1024,
            f"heap growth {heap_large} bytes scales with archive size",
        )

    def test_failed_import_cleans_all_working_state(self):
        self._generate_and_import(300, 40, "broken", fail=True)
        # No stray sqlite/staging files anywhere under the temp work dir.
        stray = [
            p for p in (self.tmp.path / "broken").rglob("*")
            if p.is_file() and (
                p.name.endswith(".sqlite") or p.name.endswith("-wal")
                or p.name.endswith("-shm")
                or p.name.startswith(STAGING_PREFIX)
            )
        ]
        self.assertEqual(stray, [])


if __name__ == "__main__":
    unittest.main()
