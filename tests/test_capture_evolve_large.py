"""Runtime scale test for schema-evolving archives.

The archive is generated at test time (nothing large is committed) and spans
three schema versions with a rename, an int -> float widening and two added
columns.  The test asserts bounded heap use and linear spill growth, plus
spill/staging cleanup on success and failure - the same guarantees capture-v1
already provides.
"""

from __future__ import annotations

import gc
import importlib.util
import json
import tracemalloc
import unittest
from pathlib import Path

from ddreduce.capture.errors import CaptureError
from ddreduce.capture.importer import STAGING_PREFIX, run_import
from ddreduce.capture.verifier import VERDICT_MATCH, run_verify

from .helpers import ROOT, TempDir

_spec = importlib.util.spec_from_file_location(
    "gen_evolving", ROOT / "demo" / "capture" / "gen_evolving.py"
)
gen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen)


class EvolvingLargeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()

    def tearDown(self):
        self.tmp.cleanup()

    def _run(self, n_tx: int, n_keys: int, tag: str, *, fail=False):
        work = self.tmp.path / tag
        info = gen.generate_large(work, n_tx, n_keys, seed=5, shards=4)
        archive = Path(info["archive"])
        history = archive / "schema-history.json"
        package = work / "package"
        spill_root = work / "spill"
        spill_root.mkdir()

        gc.collect()
        tracemalloc.start()
        before = tracemalloc.get_traced_memory()[1]
        try:
            if fail:
                shard = sorted((archive / "shards").glob("*.jsonl"))[0]
                rows = [json.loads(line) for line in
                        shard.read_text("utf-8").splitlines()]
                victim = next(r for r in rows if r.get("kind") == "change")
                bad = json.loads(json.dumps(victim))
                bad["schema_version"] = 99
                rows.append(bad)
                shard.write_text(
                    "".join(json.dumps(r, sort_keys=True) + "\n" for r in rows),
                    encoding="utf-8",
                )
                with self.assertRaises(CaptureError):
                    run_import(archive, work / "snapshot", work / "schema.json",
                               package, info["sealed"], tmp_root=spill_root,
                               history_path=history)
            else:
                result = run_import(
                    archive, work / "snapshot", work / "schema.json", package,
                    info["sealed"], tmp_root=spill_root,
                    history_path=history,
                )
        finally:
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()

        leftover_files = [p for p in spill_root.rglob("*") if p.is_file()]
        self.assertEqual(leftover_files, [])
        staging = [p for p in work.iterdir()
                   if p.name.startswith(STAGING_PREFIX)]
        self.assertEqual(staging, [])
        if fail:
            self.assertFalse(package.exists())
            return peak - before, 0
        verify = run_verify(archive, work / "snapshot", work / "schema.json",
                            info["sealed"], package, tmp_root=spill_root,
                            history_path=history)
        self.assertEqual(verify.verdict, VERDICT_MATCH, verify.failures[:2])
        overhead = sum(
            p.stat().st_size for p in (archive / "shards").glob("*.jsonl")
        )
        return peak - before, result.report["resources"]["peak_spill_bytes"]

    def test_bounded_heap_and_linear_spill(self):
        heap_small, spill_small = self._run(200, 40, "small")
        heap_large, spill_large = self._run(500, 40, "large")
        report_large = json.loads(
            (self.tmp.path / "large" / "package" / "capture-report.json")
            .read_text("utf-8")
        )
        # Every transaction was version-checked at its commit boundary.
        self.assertEqual(report_large["counts"]["commits_version_checked"], 500)
        self.assertEqual(len(report_large["schema_history"]["versions"]), 3)
        self.assertGreaterEqual(spill_large, spill_small)
        self.assertLess(spill_large, spill_small * 20)
        self.assertLess(heap_large, 8 * 1024 * 1024)

    def test_failure_cleans_working_state(self):
        self._run(120, 30, "broken", fail=True)
        stray = [p for p in (self.tmp.path / "broken").rglob("*")
                 if p.is_file() and (p.name.endswith(".sqlite")
                                     or p.name.startswith(STAGING_PREFIX))]
        self.assertEqual(stray, [])


if __name__ == "__main__":
    unittest.main()
