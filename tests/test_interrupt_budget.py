"""Search interruption/resume and call-budget accounting."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import unittest
from pathlib import Path

from ddreduce.model import Database
from ddreduce.oracle import OracleConfig
from ddreduce.schema import load_schema

from .helpers import ROOT, SRC, TempDir, make_fixture, run_cli

SCHEMA = {
    "tables": [
        {
            "name": "P",
            "columns": [{"name": "id", "type": "string", "nullable": False}],
            "primary_key": ["id"],
        },
        {
            "name": "C",
            "columns": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "p_id", "type": "string", "nullable": False},
            ],
            "primary_key": ["id"],
            "foreign_keys": [
                {"columns": ["p_id"], "ref_table": "P", "ref_columns": ["id"]}
            ],
        },
    ]
}


def _chain_tables(n: int) -> dict[str, list[dict]]:
    # Chain C_i -> P_i; one bad row at the far end, so reduction needs many
    # oracle calls and can be interrupted mid-search.
    return {
        "P": [{"id": f"p{i}"} for i in range(n)],
        "C": [{"id": f"c{i}", "p_id": f"p{i}"} for i in range(n)],
    }


class BudgetTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        mock = {"fail_rows": ["C/c9"], "fail_signature": "SIG-TARGET"}
        self.schema_path, self.tables_dir, self.oracle_path = make_fixture(
            self.tmp.path, SCHEMA, _chain_tables(10), mock, max_calls=5
        )
        self.workdir = self.tmp.path / "work"

    def tearDown(self):
        self.tmp.cleanup()

    def test_budget_exhausted_records_state(self):
        result = run_cli(
            "reduce",
            "--input", str(self.tables_dir),
            "--schema", str(self.schema_path),
            "--oracle-config", str(self.oracle_path),
            "--workdir", str(self.workdir),
        )
        self.assertEqual(result.returncode, 3)
        state = json.loads((self.workdir / "state.json").read_text())
        self.assertEqual(state["status"], "BUDGET_EXHAUSTED")
        self.assertLessEqual(state["calls_used"], 5)
        self.assertNotEqual(state["status"], "CERTIFIED")

    def test_resume_with_larger_budget_completes_and_never_double_charges(self):
        first = run_cli(
            "reduce",
            "--input", str(self.tables_dir),
            "--schema", str(self.schema_path),
            "--oracle-config", str(self.oracle_path),
            "--workdir", str(self.workdir),
        )
        self.assertEqual(first.returncode, 3)
        calls_after_first = json.loads(
            (self.workdir / "state.json").read_text()
        )["calls_used"]

        # Resume with a fresh, larger budget; cached calls must not be recharged.
        second = run_cli(
            "resume",
            "--workdir", str(self.workdir),
            "--max-calls", "200",
        )
        self.assertEqual(second.returncode, 0, second.stderr)
        state = json.loads((self.workdir / "state.json").read_text())
        self.assertEqual(state["status"], "CERTIFIED")
        cert = json.loads((self.workdir / "certificate.json").read_text())
        self.assertEqual(
            sorted(cert["result"]["row_ids"]),
            ['C/str:"c9"', 'P/str:"p9"'],
        )
        # Cache hits are free: previously confirmed candidates were not re-run.
        self.assertLessEqual(state["calls_used"], 200)
        traj = [
            json.loads(line)
            for line in (self.workdir / "trajectory.jsonl").read_text().splitlines()
        ]
        self.assertTrue(any(e["cache_hit"] for e in traj))
        charged = sum(e["subprocess_runs"] for e in traj)
        self.assertEqual(charged, state["calls_used"])
        self.assertGreaterEqual(calls_after_first, 1)


class InterruptTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        mock = {"fail_rows": ["C/c11"], "fail_signature": "SIG-TARGET"}
        self.schema_path, self.tables_dir, self.oracle_path = make_fixture(
            self.tmp.path,
            SCHEMA,
            _chain_tables(12),
            mock,
            max_calls=100,
            timeout=10.0,
        )
        self.workdir = self.tmp.path / "work"

    def tearDown(self):
        self.tmp.cleanup()

    def test_sigterm_mid_search_then_resume(self):
        env = dict(os.environ)
        env["PYTHONPATH"] = str(SRC)
        proc = subprocess.Popen(
            [
                sys.executable, "-P", "-m", "ddreduce.cli", "reduce",
                "--input", str(self.tables_dir),
                "--schema", str(self.schema_path),
                "--oracle-config", str(self.oracle_path),
                "--workdir", str(self.workdir),
            ],
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        # Let baseline + a few iterations run, then SIGTERM the parent.
        state_file = self.workdir / "state.json"
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if state_file.exists():
                state = json.loads(state_file.read_text())
                if state.get("calls_used", 0) >= 3:
                    break
            time.sleep(0.05)
        proc.send_signal(signal.SIGTERM)
        try:
            out, err = proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            self.fail("reducer did not shut down after SIGTERM")
        self.assertEqual(proc.returncode, 4, err)
        state = json.loads(state_file.read_text())
        self.assertEqual(state["status"], "INTERRUPTED")
        calls_at_interrupt = state["calls_used"]

        # No in-flight answer is faked: note is cleared or explicitly unknown;
        # resume continues and eventually certifies without double charging.
        resumed = run_cli("resume", "--workdir", str(self.workdir))
        self.assertEqual(resumed.returncode, 0, resumed.stderr)
        final_state = json.loads(state_file.read_text())
        self.assertEqual(final_state["status"], "CERTIFIED")
        traj = [
            json.loads(line)
            for line in (self.workdir / "trajectory.jsonl").read_text().splitlines()
        ]
        # Total charged calls are monotonic across the interruption.
        self.assertEqual(
            sum(e["subprocess_runs"] for e in traj), final_state["calls_used"]
        )
        self.assertGreaterEqual(final_state["calls_used"], calls_at_interrupt)
        # No candidate directory leaked from the interrupted run.
        self.assertEqual(list((self.workdir / "candidates").glob("*")), [])


class InputValidationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()

    def tearDown(self):
        self.tmp.cleanup()

    def test_duplicate_pk_rejected_before_search(self):
        tables = {
            "P": [{"id": "p1"}, {"id": "p1"}],
            "C": [],
        }
        schema_path, tables_dir, oracle_path = make_fixture(
            self.tmp.path, SCHEMA, tables, {"fail_rows": [], "fail_signature": "X"}
        )
        result = run_cli(
            "reduce",
            "--input", str(tables_dir),
            "--schema", str(schema_path),
            "--oracle-config", str(oracle_path),
            "--workdir", str(self.tmp.path / "work"),
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(
            "unique constraint" in result.stderr or "duplicate row id" in result.stderr,
            result.stderr,
        )

    def test_dangling_fk_rejected_before_search(self):
        tables = {"P": [], "C": [{"id": "c1", "p_id": "missing"}]}
        schema_path, tables_dir, oracle_path = make_fixture(
            self.tmp.path, SCHEMA, tables, {"fail_rows": [], "fail_signature": "X"}
        )
        result = run_cli(
            "reduce",
            "--input", str(tables_dir),
            "--schema", str(schema_path),
            "--oracle-config", str(oracle_path),
            "--workdir", str(self.tmp.path / "work"),
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("foreign key", result.stderr)

    def test_resume_refuses_changed_input(self):
        mock = {"fail_rows": ["C/c1"], "fail_signature": "SIG-TARGET"}
        schema_path, tables_dir, oracle_path = make_fixture(
            self.tmp.path,
            SCHEMA,
            {"P": [{"id": "p1"}], "C": [{"id": "c1", "p_id": "p1"}]},
            mock,
            max_calls=2,
        )
        workdir = self.tmp.path / "work"
        first = run_cli(
            "reduce",
            "--input", str(tables_dir),
            "--schema", str(schema_path),
            "--oracle-config", str(oracle_path),
            "--workdir", str(workdir),
        )
        self.assertEqual(first.returncode, 3)
        # Mutate the input behind the search's back.
        (tables_dir / "P.jsonl").write_text(
            '{"id": "p1"}\n{"id": "p99"}\n', encoding="utf-8"
        )
        resumed = run_cli("resume", "--workdir", str(workdir))
        self.assertNotEqual(resumed.returncode, 0)
        self.assertIn("input content changed", resumed.stderr)


if __name__ == "__main__":
    unittest.main()
