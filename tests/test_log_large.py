"""Long generated CDC logs: deterministic generation and bounded disk usage.

The generator (demo/incremental/gen_log.py) is committed; the generated data
is a runtime artifact produced into per-test temp directories and never
committed.  These tests generate a longer log, reduce it, and observe the
staging directory and cache sizes stay bounded.
"""

from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

from .helpers import ROOT, TempDir, run_cli, write_json

GEN = ROOT / "demo" / "incremental" / "gen_log.py"
DEMO_CMD = ROOT / "demo" / "incremental" / "incr_order_total.py"


def generate(out: Path, n_tx: int, n_keys: int, seed: int) -> dict:
    proc = subprocess.run(
        [sys.executable, "-I", str(GEN), "--out", str(out),
         "--tx", str(n_tx), "--keys", str(n_keys), "--seed", str(seed)],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


class GeneratorTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()

    def tearDown(self):
        self.tmp.cleanup()

    def test_generation_is_deterministic(self):
        a = self.tmp.path / "a"
        b = self.tmp.path / "b"
        info_a = generate(a, 60, 20, 7)
        info_b = generate(b, 60, 20, 7)
        self.assertEqual(info_a, info_b)
        for name in ("schema.json", "snapshot/orders.jsonl", "changes.jsonl"):
            self.assertEqual(
                (a / name).read_text(), (b / name).read_text(), name
            )

    def test_generated_log_is_valid_and_embeds_one_fault_pair(self):
        data = self.tmp.path / "gen"
        info = generate(data, 60, 20, 7)
        # The embedded pair is reported; the full log must pass validation.
        result = run_cli(
            "log-replay",
            "--snapshot", str(data / "snapshot"),
            "--schema", str(data / "schema.json"),
            "--changes", str(data / "changes.jsonl"),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["tx_count"], 60)
        self.assertEqual(len(report["committed_tx_ids"]), 60)


class LargeLogResourceTest(unittest.TestCase):
    N_TX = 160
    N_KEYS = 60

    def setUp(self):
        self.tmp = TempDir()
        self.data = self.tmp.path / "data"
        self.info = generate(self.data, self.N_TX, self.N_KEYS, 7)
        self.oracle_path = write_json(
            self.tmp.path / "oracle.json",
            {
                "command": [
                    sys.executable, "-I", str(DEMO_CMD),
                    "{candidate_dir}", "{report_file}",
                ],
                "timeout_seconds": 20.0,
                "retest_runs": 1,
                "confirm_runs": 2,
                "max_calls": 400,
            },
        )
        self.workdir = self.tmp.path / "work"

    def tearDown(self):
        self.tmp.cleanup()

    def test_reduce_long_log_and_bounded_disk(self):
        result = run_cli(
            "log-reduce",
            "--snapshot", str(self.data / "snapshot"),
            "--schema", str(self.data / "schema.json"),
            "--changes", str(self.data / "changes.jsonl"),
            "--oracle-config", str(self.oracle_path),
            "--workdir", str(self.workdir),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        cert = json.loads((self.workdir / "certificate.json").read_text())
        # Exactly the embedded delete + same-key rebuild pair survives.
        self.assertEqual(
            cert["result"]["kept_tx_ids"],
            [self.info["fault_delete_tx"], self.info["fault_rebuild_tx"]],
        )
        self.assertEqual(cert["input"]["tx_count"], self.N_TX)
        self.assertLessEqual(cert["calls_used"], 400)

        # No live candidate directories remain; the staging area is empty.
        self.assertEqual(list((self.workdir / "candidates").glob("*")), [])

        # Cache stores one small JSON per distinct candidate; nothing
        # approaches the size of the generated log itself.
        cache_files = list((self.workdir / "cache").rglob("*.json"))
        self.assertGreater(len(cache_files), 0)
        cache_total = sum(f.stat().st_size for f in cache_files)
        log_size = (self.data / "changes.jsonl").stat().st_size
        self.assertLess(cache_total, log_size)
        biggest = max(f.stat().st_size for f in cache_files)
        self.assertLess(biggest, 256 * 1024)

        # The result package is tiny compared to the original log: the fixed
        # snapshot plus two transaction lines plus the evidence copy.
        result_files = [
            f for f in (self.workdir / "result").rglob("*") if f.is_file()
        ]
        result_total = sum(f.stat().st_size for f in result_files)
        self.assertLess(result_total, 4 * 1024 * 1024)
        kept_lines = (self.workdir / "result" / "changes.jsonl").read_text().strip()
        self.assertEqual(len(kept_lines.splitlines()), 2)

        # Independent verification of the long-log certificate.
        verify = run_cli(
            "log-verify",
            "--workdir", str(self.workdir),
            "--oracle-config", str(self.oracle_path),
        )
        self.assertEqual(verify.returncode, 0, verify.stderr)
        report = json.loads((self.workdir / "log-verify-report.json").read_text())
        self.assertEqual(report["verdict"], "VERIFIED_MINIMAL")
        self.assertEqual(len(report["neighbors"]), 2)


if __name__ == "__main__":
    unittest.main()
