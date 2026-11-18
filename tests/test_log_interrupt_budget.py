"""Log-mode budget accounting, interruption/resume, unknown neighbors, log-replay."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import unittest
from pathlib import Path

from .helpers import ROOT, SRC, TempDir, ins, make_log_fixture, run_cli, tx, upd

T_SCHEMA = {
    "tables": [
        {
            "name": "T",
            "columns": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "v", "type": "int", "nullable": False},
            ],
            "primary_key": ["id"],
        },
    ]
}


def chain_changes(n: int) -> list[dict]:
    """n independent insert transactions; the fault row is inserted by t{n-1}."""
    return [
        tx(f"t{i}", ins("T", {"id": f"k{i}"}, {"id": f"k{i}", "v": i}))
        for i in range(n)
    ]


def dependent_changes(n: int) -> list[dict]:
    """A causally dependent chain: t0 inserts x, each later tx updates it."""
    txs = [tx("t0", ins("T", {"id": "x"}, {"id": "x", "v": 1}))]
    for i in range(1, n):
        txs.append(
            tx(f"t{i}", upd("T", {"id": "x"}, {"id": "x", "v": i}, {"id": "x", "v": i + 1}))
        )
    return txs


class LogBudgetTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        # The fault needs t9, which causally depends on t0..t8: every strict
        # single-transaction deletion breaks the chain and passes.  Broken
        # chains normalize to prefixes, so the empty-log candidate recurs and
        # resume must serve those evaluations from the cache.
        mock = {"fail_tx_sets": [["t9"]], "fail_signature": "SIG-TARGET"}
        self.schema_path, self.snapshot_dir, self.changes_path, self.oracle_path = (
            make_log_fixture(
                self.tmp.path, T_SCHEMA, {"T": []}, dependent_changes(10), mock,
                max_calls=5,
            )
        )
        self.workdir = self.tmp.path / "work"

    def tearDown(self):
        self.tmp.cleanup()

    def _reduce(self, *extra):
        return run_cli(
            "log-reduce",
            "--snapshot", str(self.snapshot_dir),
            "--schema", str(self.schema_path),
            "--changes", str(self.changes_path),
            "--oracle-config", str(self.oracle_path),
            "--workdir", str(self.workdir),
            *extra,
        )

    def test_budget_exhausted_records_state(self):
        result = self._reduce()
        self.assertEqual(result.returncode, 3)
        state = json.loads((self.workdir / "state.json").read_text())
        self.assertEqual(state["status"], "BUDGET_EXHAUSTED")
        self.assertEqual(state["mode"], "log")
        self.assertLessEqual(state["calls_used"], 5)

    def test_resume_with_larger_budget_completes_without_double_charging(self):
        first = self._reduce()
        self.assertEqual(first.returncode, 3)
        calls_after_first = json.loads(
            (self.workdir / "state.json").read_text()
        )["calls_used"]
        self.assertGreaterEqual(calls_after_first, 1)

        resumed = run_cli("resume", "--workdir", str(self.workdir), "--max-calls", "200")
        self.assertEqual(resumed.returncode, 0, resumed.stderr)
        state = json.loads((self.workdir / "state.json").read_text())
        self.assertEqual(state["status"], "CERTIFIED")
        cert = json.loads((self.workdir / "certificate.json").read_text())
        # The whole chain is causally required: nothing could be removed.
        self.assertEqual(
            cert["result"]["kept_tx_ids"], [f"t{i}" for i in range(10)]
        )
        traj = [
            json.loads(line)
            for line in (self.workdir / "trajectory.jsonl").read_text().splitlines()
        ]
        self.assertTrue(any(e["cache_hit"] for e in traj))
        charged = sum(e["subprocess_runs"] for e in traj)
        self.assertEqual(charged, state["calls_used"])

    def test_resume_refuses_changed_inputs(self):
        self.assertEqual(self._reduce().returncode, 3)

        def fresh_run_and_resume(mutate, needle):
            work = self.tmp.path / f"refuse-{needle[:12]}"
            # Rebuild a fresh search, then mutate one input behind its back.
            self.assertEqual(
                run_cli(
                    "log-reduce",
                    "--snapshot", str(self.snapshot_dir),
                    "--schema", str(self.schema_path),
                    "--changes", str(self.changes_path),
                    "--oracle-config", str(self.oracle_path),
                    "--workdir", str(work),
                ).returncode,
                3,
            )
            mutate()
            resumed = run_cli("resume", "--workdir", str(work))
            self.assertEqual(resumed.returncode, 2, resumed.stderr)
            self.assertIn(needle, resumed.stderr)

        changes_orig = Path(self.changes_path).read_text(encoding="utf-8")
        schema_orig = Path(self.schema_path).read_text(encoding="utf-8")
        snapshot_orig = (self.snapshot_dir / "T.jsonl").read_text(encoding="utf-8")
        oracle_orig = Path(self.oracle_path).read_text(encoding="utf-8")

        def restore():
            Path(self.changes_path).write_text(changes_orig, encoding="utf-8")
            Path(self.schema_path).write_text(schema_orig, encoding="utf-8")
            (self.snapshot_dir / "T.jsonl").write_text(snapshot_orig, encoding="utf-8")
            Path(self.oracle_path).write_text(oracle_orig, encoding="utf-8")

        try:
            def mutate_log():
                Path(self.changes_path).write_text(
                    changes_orig + json.dumps(
                        tx("tX", ins("T", {"id": "kx"}, {"id": "kx", "v": 99}))
                    ) + "\n",
                    encoding="utf-8",
                )
            fresh_run_and_resume(mutate_log, "original change log changed")
            restore()

            def mutate_schema():
                # Stays a valid log/snapshot but changes the schema hash.
                doc = json.loads(schema_orig)
                doc["tables"][0]["columns"][1]["nullable"] = True
                Path(self.schema_path).write_text(json.dumps(doc), encoding="utf-8")
            fresh_run_and_resume(mutate_schema, "schema changed")
            restore()

            def mutate_snapshot():
                (self.snapshot_dir / "T.jsonl").write_text(
                    snapshot_orig + '{"id": "snap", "v": 1}\n', encoding="utf-8"
                )
            fresh_run_and_resume(mutate_snapshot, "snapshot content changed")
            restore()

            def mutate_oracle():
                doc = json.loads(oracle_orig)
                doc["timeout_seconds"] = doc["timeout_seconds"] + 1
                Path(self.oracle_path).write_text(json.dumps(doc), encoding="utf-8")
            fresh_run_and_resume(mutate_oracle, "oracle configuration changed")
            restore()
        finally:
            restore()


class LogInterruptTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        mock = {"fail_rows": ["T/k24"], "fail_signature": "SIG-TARGET"}
        self.schema_path, self.snapshot_dir, self.changes_path, self.oracle_path = (
            make_log_fixture(
                self.tmp.path, T_SCHEMA, {"T": []}, chain_changes(25), mock,
                max_calls=100, timeout=10.0,
            )
        )
        self.workdir = self.tmp.path / "work"

    def tearDown(self):
        self.tmp.cleanup()

    def test_sigterm_mid_search_then_resume(self):
        env = dict(os.environ)
        env["PYTHONPATH"] = str(SRC)
        proc = subprocess.Popen(
            [
                sys.executable, "-P", "-m", "ddreduce.cli", "log-reduce",
                "--snapshot", str(self.snapshot_dir),
                "--schema", str(self.schema_path),
                "--changes", str(self.changes_path),
                "--oracle-config", str(self.oracle_path),
                "--workdir", str(self.workdir),
            ],
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        state_file = self.workdir / "state.json"
        deadline = time.monotonic() + 20
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
            self.fail("log reducer did not shut down after SIGTERM")
        self.assertEqual(proc.returncode, 4, err)
        state = json.loads(state_file.read_text())
        self.assertEqual(state["status"], "INTERRUPTED")
        self.assertEqual(state["mode"], "log")
        calls_at_interrupt = state["calls_used"]

        resumed = run_cli("resume", "--workdir", str(self.workdir))
        self.assertEqual(resumed.returncode, 0, resumed.stderr)
        final_state = json.loads(state_file.read_text())
        self.assertEqual(final_state["status"], "CERTIFIED")
        cert = json.loads((self.workdir / "certificate.json").read_text())
        self.assertEqual(cert["result"]["kept_tx_ids"], ["t24"])
        traj = [
            json.loads(line)
            for line in (self.workdir / "trajectory.jsonl").read_text().splitlines()
        ]
        # Charged calls are monotonic across the interruption; no in-flight
        # call was converted into a fabricated result.
        self.assertEqual(
            sum(e["subprocess_runs"] for e in traj), final_state["calls_used"]
        )
        self.assertGreaterEqual(final_state["calls_used"], calls_at_interrupt)
        self.assertEqual(list((self.workdir / "candidates").glob("*")), [])


class LogUnknownNeighborTest(unittest.TestCase):
    """A strict neighbor that hangs blocks the certificate (BLOCKED, not budget)."""

    def setUp(self):
        self.tmp = TempDir()
        # Chain: t0 inserts x, t1 updates it, t2 deletes it, t3 rebuilds it;
        # t4 is irrelevant. The fault needs t2+t3. Candidates below 4
        # transactions hang, so the strict neighbors of the 4-transaction
        # result are all UNRESOLVED.
        changes = [
            tx("t0", ins("T", {"id": "x"}, {"id": "x", "v": 1})),
            tx("t1", upd("T", {"id": "x"}, {"id": "x", "v": 1}, {"id": "x", "v": 2})),
            tx("t2", upd("T", {"id": "x"}, {"id": "x", "v": 2}, {"id": "x", "v": 3})),
            tx("t3", upd("T", {"id": "x"}, {"id": "x", "v": 3}, {"id": "x", "v": 4})),
            tx("t4", ins("T", {"id": "z"}, {"id": "z", "v": 5})),
        ]
        mock = {
            "fail_tx_sets": [["t2", "t3"]],
            "fail_signature": "SIG-PAIR",
            "timeout_always": True,
            "timeout_when_tx_below": 4,
        }
        self.schema_path, self.snapshot_dir, self.changes_path, self.oracle_path = (
            make_log_fixture(
                self.tmp.path, T_SCHEMA, {"T": []}, changes, mock,
                timeout=1.0, max_calls=80,
            )
        )
        self.workdir = self.tmp.path / "work"

    def tearDown(self):
        self.tmp.cleanup()

    def test_unknown_neighbor_blocks_certification_and_verify_reports_unknown(self):
        result = run_cli(
            "log-reduce",
            "--snapshot", str(self.snapshot_dir),
            "--schema", str(self.schema_path),
            "--changes", str(self.changes_path),
            "--oracle-config", str(self.oracle_path),
            "--workdir", str(self.workdir),
        )
        self.assertEqual(result.returncode, 2, result.stderr)
        state = json.loads((self.workdir / "state.json").read_text())
        # Distinct from BUDGET_EXHAUSTED: the neighbor is unknown.
        self.assertEqual(state["status"], "BLOCKED")
        self.assertIn("UNRESOLVED", state["note"])

        # Build the 4-transaction result package by hand (the search reached
        # it but could not certify it) and verify it independently.
        from ddreduce.logmodel import load_changelog
        from ddreduce.model import Database
        from ddreduce.replay import validate_log_inputs
        from ddreduce.schema import load_schema

        schema = load_schema(self.schema_path)
        schema_text = Path(self.schema_path).read_text(encoding="utf-8")
        snapshot = Database.load(schema, self.snapshot_dir)
        log = load_changelog(self.changes_path, schema)
        validated = validate_log_inputs(schema, schema_text, snapshot, log)
        package = self.tmp.path / "pkg"
        validated.candidate([0, 1, 2, 3]).materialize(package)
        report_path = self.tmp.path / "pkg-report.json"
        # Sanity: the package itself still fails with the target signature.
        from ddreduce.oracle import OracleConfig, OracleRunner

        runner = OracleRunner(self.tmp.path / "probe", OracleConfig.load(self.oracle_path))
        signal, _ = runner.run_once(validated.candidate([0, 1, 2, 3]))
        self.assertEqual(signal.report_status, "FAIL")
        self.assertEqual(signal.signature, "SIG-PAIR")


class LogReplayCliTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        changes = [
            tx("a", ins("T", {"id": "x"}, {"id": "x", "v": 1})),
            tx("b", upd("T", {"id": "x"}, {"id": "x", "v": 1}, {"id": "x", "v": 2})),
            tx("c", ins("T", {"id": "y"}, {"id": "y", "v": 3})),
        ]
        self.schema_path, self.snapshot_dir, self.changes_path, _ = make_log_fixture(
            self.tmp.path, T_SCHEMA, {"T": []}, changes
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _replay(self, *extra):
        return run_cli(
            "log-replay",
            "--snapshot", str(self.snapshot_dir),
            "--schema", str(self.schema_path),
            "--changes", str(self.changes_path),
            *extra,
        )

    def test_full_replay_report(self):
        result = self._replay()
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertTrue(report["valid"])
        self.assertEqual(report["committed_tx_ids"], ["a", "b", "c"])
        self.assertEqual(report["final"]["table_counts"], {"T": 2})
        statuses = {t["tx_id"]: t["status"] for t in report["transactions"]}
        self.assertEqual(statuses, {"a": "committed", "b": "committed", "c": "committed"})

    def test_selection_drop_reports_skip_reasons(self):
        sel = self.tmp.path / "sel.json"
        sel.write_text(json.dumps({"drop": ["a"]}), encoding="utf-8")
        out_dir = self.tmp.path / "out"
        result = self._replay("--selection", str(sel), "--out", str(out_dir))
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        statuses = {t["tx_id"]: t["status"] for t in report["transactions"]}
        # b loses the version written by a; c is independent and commits.
        self.assertEqual(statuses, {"a": "deselected", "b": "skipped", "c": "committed"})
        reason = [t for t in report["transactions"] if t["tx_id"] == "b"][0]["reason"]
        self.assertIn("row version written by tx position 0", reason)
        self.assertEqual(report["committed_tx_ids"], ["c"])
        # The normalized candidate package was materialized.
        kept = (out_dir / "changes.jsonl").read_text().strip().splitlines()
        self.assertEqual(len(kept), 1)
        self.assertEqual(json.loads(kept[0])["tx_id"], "c")
        self.assertTrue((out_dir / "snapshot" / "T.jsonl").exists())
        self.assertTrue((out_dir / "schema.json").exists())

    def test_invalid_log_reports_tx_and_event_without_oracle(self):
        bad = self.tmp.path / "bad"
        bad.mkdir()
        (bad / "changes.jsonl").write_text(
            Path(self.changes_path).read_text(encoding="utf-8")
            + json.dumps(tx("zz", upd("T", {"id": "x"}, {"id": "x", "v": 9},
                                     {"id": "x", "v": 10})))
            + "\n",
            encoding="utf-8",
        )
        result = run_cli(
            "log-replay",
            "--snapshot", str(self.snapshot_dir),
            "--schema", str(self.schema_path),
            "--changes", str(bad / "changes.jsonl"),
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("tx 'zz'", result.stderr)
        self.assertIn("event 0", result.stderr)


class ModeGuardTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()

    def tearDown(self):
        self.tmp.cleanup()

    def test_log_reduce_on_tables_workdir_and_vice_versa(self):
        # A tables-mode search in one workdir...
        from .helpers import make_fixture

        schema = {
            "tables": [
                {
                    "name": "T",
                    "columns": [{"name": "id", "type": "string", "nullable": False}],
                    "primary_key": ["id"],
                }
            ]
        }
        schema_path, tables_dir, oracle_path = make_fixture(
            self.tmp.path / "static", schema, {"T": [{"id": "a"}]},
            {"fail_rows": ["T/a"], "fail_signature": "SIG"}, max_calls=2,
        )
        static_work = self.tmp.path / "static-work"
        run_cli(
            "reduce", "--input", str(tables_dir), "--schema", str(schema_path),
            "--oracle-config", str(oracle_path), "--workdir", str(static_work),
        )
        # ...cannot be continued by log-reduce (mode mismatch is reported).
        _, snapshot_dir, changes_path, _ = make_log_fixture(
            self.tmp.path / "log", T_SCHEMA, {"T": []},
            [tx("t0", ins("T", {"id": "x"}, {"id": "x", "v": 1}))],
        )
        result = run_cli(
            "log-reduce",
            "--snapshot", str(snapshot_dir),
            "--schema", str(self.tmp.path / "log" / "schema.json"),
            "--changes", str(changes_path),
            "--oracle-config", str(oracle_path),
            "--workdir", str(static_work),
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("tables-mode", result.stderr)


if __name__ == "__main__":
    unittest.main()
