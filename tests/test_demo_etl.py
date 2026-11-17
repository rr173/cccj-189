"""End-to-end demo: the real multi-table ETL quality gate and its defect."""

from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

from .helpers import ROOT, TempDir

DEMO = ROOT / "demo"


class DemoETLTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        self.workdir = self.tmp.path / "work"
        # The committed oracle config uses a repo-relative command path; run
        # the CLI with cwd=ROOT so it resolves exactly as documented.
        self.config_path = DEMO / "config" / "oracle.json"

    def tearDown(self):
        self.tmp.cleanup()

    def _run(self, *args):
        return subprocess.run(
            [
                sys.executable, "-P", "-m", "ddreduce.cli", *args,
                "--input", str(DEMO / "data" / "tables"),
                "--schema", str(DEMO / "data" / "schema.json"),
                "--oracle-config", str(self.config_path),
                "--workdir", str(self.workdir),
            ],
            capture_output=True,
            text=True,
            cwd=str(ROOT),
            env={"PYTHONPATH": str(ROOT / "src"), "PATH": "/usr/bin:/bin"},
        )

    def test_demo_reproduces_then_shrinks_to_minimal_closure(self):
        # Sanity: the original database fails with the zero-price signature.
        reduce_run = self._run("reduce")
        self.assertEqual(reduce_run.returncode, 0, reduce_run.stderr)
        cert = json.loads((self.workdir / "certificate.json").read_text())
        self.assertEqual(cert["target_signature"], "E_ZERO_PRICE")
        # Minimal fault closure: the bad line L2, its product P2, order O2
        # and customer C2 (required FK chain). Address A2 is NOT required
        # because customers.address_id is nullable.
        self.assertEqual(
            sorted(cert["result"]["row_ids"]),
            [
                'customers/str:"C2"',
                'order_lines/str:"L2"',
                'orders/str:"O2"',
                'products/str:"P2"',
            ],
        )
        # The nullable-FK parent A2 must not have been dragged in.
        self.assertFalse(
            any(rid.startswith("addresses/") for rid in cert["result"]["row_ids"])
        )

        # Independent verification of the demo certificate.
        verify_run = self._run("verify")
        self.assertEqual(verify_run.returncode, 0, verify_run.stderr)
        report = json.loads((self.workdir / "verify-report.json").read_text())
        self.assertEqual(report["verdict"], "VERIFIED_MINIMAL")
        # Every strict neighbor independently re-run by the verifier passes.
        self.assertTrue(report["neighbors"])
        self.assertEqual(
            {n["verdict"] for n in report["neighbors"]}, {"PASS"}
        )

    def test_demo_result_package_runs_standalone(self):
        self._run("reduce")
        # Point the transform directly at the reduced package.
        report_path = self.tmp.path / "standalone-report.json"
        proc = subprocess.run(
            [
                sys.executable, "-I",
                str(DEMO / "transform" / "etl_quality_gate.py"),
                str(self.workdir / "result"),
                str(report_path),
            ],
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        report = json.loads(report_path.read_text())
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(report["signature"], "E_ZERO_PRICE")
        self.assertEqual(report["violations"][0]["line_id"], "L2")


if __name__ == "__main__":
    unittest.main()
