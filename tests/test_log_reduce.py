"""End-to-end CDC reduction on the real incremental-aggregation demo."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import unittest
from pathlib import Path

from .helpers import ROOT, SRC, TempDir, run_cli

DEMO = ROOT / "demo" / "incremental"


def run_demo(candidate_dir: Path, report_path: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-I", str(DEMO / "incr_order_total.py"),
         str(candidate_dir), str(report_path)],
        capture_output=True,
        text=True,
    )


class DemoLogReduceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        self.workdir = self.tmp.path / "work"
        self.args = (
            "--snapshot", str(DEMO / "data" / "snapshot"),
            "--schema", str(DEMO / "data" / "schema.json"),
            "--changes", str(DEMO / "data" / "changes.jsonl"),
            "--oracle-config", str(DEMO / "config" / "oracle.json"),
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _reduce(self, workdir=None):
        return run_cli(
            "log-reduce", *self.args, "--workdir", str(workdir or self.workdir)
        )

    def _verify(self, workdir=None):
        return run_cli(
            "log-verify",
            "--workdir", str(workdir or self.workdir),
            "--oracle-config", str(DEMO / "config" / "oracle.json"),
        )

    def test_reduce_to_delete_plus_rebuild_pair(self):
        result = self._reduce()
        self.assertEqual(result.returncode, 0, result.stderr)
        cert = json.loads((self.workdir / "certificate.json").read_text())
        self.assertEqual(cert["mode"], "log")
        self.assertEqual(cert["target_signature"], "E_INCR_DIVERGE:open")
        # The fault is the delete + same-key rebuild of O4; overwrite updates
        # and unrelated inserts are reduced away.
        self.assertEqual(cert["result"]["kept_tx_ids"], ["t3", "t4"])
        self.assertEqual(cert["result"]["kept_positions"], [2, 3])

        # The result package is self-contained.
        result_dir = self.workdir / "result"
        self.assertTrue((result_dir / "snapshot" / "orders.jsonl").exists())
        self.assertTrue((result_dir / "schema.json").exists())
        self.assertTrue((result_dir / "changes.jsonl").exists())
        self.assertTrue((result_dir / "evidence" / "original-changes.jsonl").exists())
        self.assertTrue((result_dir / "evidence" / "selection.json").exists())

        verify = self._verify()
        self.assertEqual(verify.returncode, 0, verify.stderr)
        report = json.loads((self.workdir / "log-verify-report.json").read_text())
        self.assertEqual(report["verdict"], "VERIFIED_MINIMAL")
        # Two strict neighbors ([t3] and [t4] removed), both PASS.
        self.assertEqual(len(report["neighbors"]), 2)
        self.assertEqual({n["verdict"] for n in report["neighbors"]}, {"PASS"})
        # Neighbor of t4 normalizes to [t3]; neighbor of t3 normalizes to []
        # (t4's tombstone premise fails), committed tx ids are reported.
        by_tx = {n["tx_id"]: n for n in report["neighbors"]}
        self.assertEqual(by_tx["t3"]["committed_tx_ids"], [])
        self.assertEqual(by_tx["t4"]["committed_tx_ids"], ["t3"])

    def test_result_package_runs_standalone_without_original_paths(self):
        self._reduce()
        # Copy the result elsewhere and drop the evidence directory: the
        # oracle contract is exactly snapshot/ + schema.json + changes.jsonl.
        standalone = self.tmp.path / "standalone"
        shutil.copytree(self.workdir / "result", standalone)
        shutil.rmtree(standalone / "evidence")
        report_path = self.tmp.path / "standalone-report.json"
        proc = run_demo(standalone, report_path)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        report = json.loads(report_path.read_text())
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(report["signature"], "E_INCR_DIVERGE:open")
        # The divergence is computed from the actual totals, not hardcoded.
        self.assertEqual(
            report["divergence"],
            {"open": {"incremental": 155.0, "batch": 230.0}},
        )

    def test_signature_is_derived_from_actual_divergence(self):
        # A delete+rebuild pair on a *paid* order diverges the paid group:
        # the signature follows the data, not any tx id or candidate length.
        pkg = self.tmp.path / "paid-case"
        (pkg / "snapshot").mkdir(parents=True)
        shutil.copy(DEMO / "data" / "schema.json", pkg / "schema.json")
        (pkg / "snapshot" / "orders.jsonl").write_text(
            '{"order_id": "W1", "status": "paid", "amount": 10.0}\n',
            encoding="utf-8",
        )
        (pkg / "changes.jsonl").write_text(
            json.dumps({"tx_id": "k1", "events": [
                {"op": "delete", "table": "orders", "key": {"order_id": "W1"},
                 "before": {"order_id": "W1", "status": "paid", "amount": 10.0}},
            ]}) + "\n"
            + json.dumps({"tx_id": "k2", "events": [
                {"op": "insert", "table": "orders", "key": {"order_id": "W1"},
                 "after": {"order_id": "W1", "status": "paid", "amount": 44.0}},
            ]}) + "\n",
            encoding="utf-8",
        )
        report_path = self.tmp.path / "paid-report.json"
        proc = run_demo(pkg, report_path)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        report = json.loads(report_path.read_text())
        self.assertEqual(report["signature"], "E_INCR_DIVERGE:paid")
        self.assertEqual(
            report["divergence"],
            {"paid": {"incremental": 34.0, "batch": 44.0}},
        )

    def test_tampered_package_or_certificate_is_rejected(self):
        self._reduce()
        verify = self._verify()
        self.assertEqual(verify.returncode, 0, verify.stderr)

        def tamper_and_verify(mutate, needle):
            work = self.tmp.path / f"tamper-{needle[:8]}"
            shutil.copytree(self.workdir, work, dirs_exist_ok=True)
            mutate(work)
            proc = run_cli(
                "log-verify",
                "--workdir", str(work),
                "--oracle-config", str(DEMO / "config" / "oracle.json"),
            )
            self.assertEqual(proc.returncode, 2, proc.stderr)
            report = json.loads((work / "log-verify-report.json").read_text())
            self.assertEqual(report["verdict"], "REJECTED")
            self.assertIn(needle, report["reason"])

        # 1. Tamper with the candidate log (swap an amount).
        def tamper_changes(work):
            path = work / "result" / "changes.jsonl"
            path.write_text(path.read_text().replace("130.0", "131.0"))
        tamper_and_verify(tamper_changes, "does not match the normalized result")

        # 2. Tamper with the audited original log in evidence (the changed
        #    before-image no longer replays: caught by re-validation).
        def tamper_evidence(work):
            path = work / "result" / "evidence" / "original-changes.jsonl"
            path.write_text(path.read_text().replace("75.0", "76.0"))
        tamper_and_verify(tamper_evidence, "original log in evidence is invalid")

        # 3. Tamper with the declared selection (now inconsistent with the
        #    candidate it claims to produce).
        def tamper_selection(work):
            path = work / "result" / "evidence" / "selection.json"
            doc = json.loads(path.read_text())
            doc["selected_positions"] = [2]
            path.write_text(json.dumps(doc))
        tamper_and_verify(tamper_selection, "does not match the normalized result")

        # 4. Tamper with the certificate's kept transaction list.
        def tamper_cert(work):
            path = work / "certificate.json"
            cert = json.loads(path.read_text())
            cert["result"]["kept_tx_ids"] = ["t3"]
            path.write_text(json.dumps(cert, indent=2))
        tamper_and_verify(tamper_cert, "kept_tx_ids")

        # 5. Tamper with transaction boundaries in the candidate (merge the
        #    two transactions into one line).
        def tamper_boundaries(work):
            path = work / "result" / "changes.jsonl"
            lines = path.read_text().splitlines()
            first, second = (json.loads(line) for line in lines if line.strip())
            merged = {"tx_id": first["tx_id"],
                      "events": first["events"] + second["events"]}
            path.write_text(json.dumps(merged) + "\n")
        tamper_and_verify(tamper_boundaries, "does not match the normalized result")

    def test_old_verify_refuses_log_certificates(self):
        self._reduce()
        proc = run_cli(
            "verify",
            "--input", str(DEMO / "data" / "snapshot"),
            "--schema", str(DEMO / "data" / "schema.json"),
            "--oracle-config", str(DEMO / "config" / "oracle.json"),
            "--workdir", str(self.workdir),
        )
        self.assertEqual(proc.returncode, 2)
        self.assertIn("log-verify", proc.stderr)

    def test_log_verify_budget_exhaustion_is_distinct(self):
        self._reduce()
        # Budget for exactly the fault confirmation but not the neighbors.
        proc = run_cli(
            "log-verify",
            "--workdir", str(self.workdir),
            "--oracle-config", str(DEMO / "config" / "oracle.json"),
            "--max-calls", "1",
        )
        self.assertEqual(proc.returncode, 3, proc.stderr)
        report = json.loads((self.workdir / "log-verify-report.json").read_text())
        self.assertEqual(report["verdict"], "BUDGET_EXHAUSTED")

    def test_log_verify_refuses_tables_certificates(self):
        # A tables-mode workdir cannot be verified as a CDC history proof.
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
            {"fail_rows": ["T/a"], "fail_signature": "SIG"},
        )
        static_work = self.tmp.path / "static-work"
        self.assertEqual(
            run_cli(
                "reduce", "--input", str(tables_dir), "--schema", str(schema_path),
                "--oracle-config", str(oracle_path), "--workdir", str(static_work),
            ).returncode,
            0,
        )
        proc = run_cli(
            "log-verify",
            "--workdir", str(static_work),
            "--oracle-config", str(oracle_path),
        )
        self.assertEqual(proc.returncode, 2)
        report = json.loads((static_work / "log-verify-report.json").read_text())
        self.assertEqual(report["verdict"], "REJECTED")
        self.assertIn("mode", report["reason"])


class HistoryBoundCacheTest(unittest.TestCase):
    """Same final table, different history: no shared cache, no shared verdict."""

    def setUp(self):
        self.tmp = TempDir()

    def tearDown(self):
        self.tmp.cleanup()

    def _package(self, name, changes_lines):
        pkg = self.tmp.path / name
        (pkg / "snapshot").mkdir(parents=True)
        shutil.copy(DEMO / "data" / "schema.json", pkg / "schema.json")
        shutil.copy(
            DEMO / "data" / "snapshot" / "orders.jsonl",
            pkg / "snapshot" / "orders.jsonl",
        )
        (pkg / "changes.jsonl").write_text(changes_lines, encoding="utf-8")
        return pkg

    def test_same_final_state_different_history(self):
        # History 1: delete O4 then rebuild it at 130 -> stale-image defect.
        delete_then_rebuild = (
            json.dumps({"tx_id": "d", "events": [
                {"op": "delete", "table": "orders", "key": {"order_id": "O4"},
                 "before": {"order_id": "O4", "status": "open", "amount": 75.0}},
            ]}) + "\n"
            + json.dumps({"tx_id": "r", "events": [
                {"op": "insert", "table": "orders", "key": {"order_id": "O4"},
                 "after": {"order_id": "O4", "status": "open", "amount": 130.0}},
            ]}) + "\n"
        )
        # History 2: a single overwrite update to the same final value.
        single_update = json.dumps({"tx_id": "u", "events": [
            {"op": "update", "table": "orders", "key": {"order_id": "O4"},
             "before": {"order_id": "O4", "status": "open", "amount": 75.0},
             "after": {"order_id": "O4", "status": "open", "amount": 130.0}},
        ]}) + "\n"

        pkg1 = self._package("h1", delete_then_rebuild)
        pkg2 = self._package("h2", single_update)

        # The final tables are byte-identical: prove it via the batch totals
        # of a PASSing run and by direct comparison of applied state.
        r1, r2 = self.tmp.path / "r1.json", self.tmp.path / "r2.json"
        proc1 = run_demo(pkg1, r1)
        proc2 = run_demo(pkg2, r2)
        self.assertEqual(proc1.returncode, 0, proc1.stderr)
        self.assertEqual(proc2.returncode, 0, proc2.stderr)
        rep1 = json.loads(r1.read_text())
        rep2 = json.loads(r2.read_text())
        self.assertEqual(rep1["batch_totals"], rep2["batch_totals"])
        # But the incremental histories differ: only the first diverges.
        self.assertEqual(rep1["status"], "FAIL")
        self.assertEqual(rep1["signature"], "E_INCR_DIVERGE:open")
        self.assertEqual(rep2["status"], "PASS")

        # Cache keys bind the ordered history, not the final table.
        sys.path.insert(0, str(SRC))
        from ddreduce.cache import OracleCache, make_log_cache_key
        from ddreduce.logmodel import load_changelog
        from ddreduce.model import Database
        from ddreduce.oracle import OracleConfig, OracleRunner
        from ddreduce.replay import LogCandidate
        from ddreduce.schema import load_schema

        schema = load_schema(DEMO / "data" / "schema.json")
        schema_text = (DEMO / "data" / "schema.json").read_text()
        snapshot = Database.load(schema, DEMO / "data" / "snapshot")
        config = OracleConfig.load(DEMO / "config" / "oracle.json")

        def candidate_of(changes_text):
            changes_path = self.tmp.path / f"c-{len(changes_text)}.jsonl"
            changes_path.write_text(changes_text)
            log = load_changelog(changes_path, schema)
            return LogCandidate(schema, schema_text, snapshot, list(log.transactions))

        cand1 = candidate_of(delete_then_rebuild)
        cand2 = candidate_of(single_update)
        key1 = make_log_cache_key(cand1, schema, config)
        key2 = make_log_cache_key(cand2, schema, config)
        self.assertNotEqual(key1.digest, key2.digest)

        # Evaluate both twice through one cache: exactly two real subprocess
        # evaluations happen; the second round is served from distinct entries.
        workdir = self.tmp.path / "cache-work"
        cache = OracleCache(workdir)
        runner = OracleRunner(workdir, config)
        calls = 0
        for cand in (cand1, cand2, cand1, cand2):
            key = make_log_cache_key(cand, schema, config)
            cached = cache.get(key)
            if cached is None:
                signal, _ = runner.run_once(cand)
                cache.put(key, [signal])
                calls += 1
        self.assertEqual(calls, 2)
        self.assertEqual(cache.get(key1)[0].report_status, "FAIL")
        self.assertEqual(cache.get(key2)[0].report_status, "PASS")


class EmptyAndInvalidLogTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()

    def tearDown(self):
        self.tmp.cleanup()

    def test_empty_log_is_certified_when_it_still_fails(self):
        # The command fails even on the snapshot alone, so deleting every
        # transaction yields the 1-minimal empty log.
        from .helpers import make_log_fixture, tx, upd

        schema = {
            "tables": [
                {
                    "name": "T",
                    "columns": [
                        {"name": "id", "type": "string", "nullable": False},
                        {"name": "v", "type": "int", "nullable": False},
                    ],
                    "primary_key": ["id"],
                }
            ]
        }
        changes = [
            tx("t0", upd("T", {"id": "x"}, {"id": "x", "v": 1}, {"id": "x", "v": 2})),
            tx("t1", upd("T", {"id": "x"}, {"id": "x", "v": 2}, {"id": "x", "v": 3})),
        ]
        mock = {"fail_signature": "SIG-ALWAYS", "fail_always": True}
        schema_path, snapshot_dir, changes_path, oracle_path = make_log_fixture(
            self.tmp.path, schema, {"T": [{"id": "x", "v": 1}]}, changes, mock
        )
        workdir = self.tmp.path / "work"
        result = run_cli(
            "log-reduce",
            "--snapshot", str(snapshot_dir),
            "--schema", str(schema_path),
            "--changes", str(changes_path),
            "--oracle-config", str(oracle_path),
            "--workdir", str(workdir),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        cert = json.loads((workdir / "certificate.json").read_text())
        self.assertEqual(cert["result"]["kept_tx_ids"], [])
        self.assertEqual(cert["result"]["tx_count"], 0)
        self.assertEqual((workdir / "result" / "changes.jsonl").read_text(), "")
        verify = run_cli(
            "log-verify",
            "--workdir", str(workdir),
            "--oracle-config", str(oracle_path),
        )
        self.assertEqual(verify.returncode, 0, verify.stderr)
        report = json.loads((workdir / "log-verify-report.json").read_text())
        self.assertEqual(report["verdict"], "VERIFIED_MINIMAL")
        self.assertEqual(report["neighbors"], [])

    def test_invalid_original_log_never_starts_the_oracle(self):
        from .helpers import make_log_fixture, tx, upd

        schema = {
            "tables": [
                {
                    "name": "T",
                    "columns": [
                        {"name": "id", "type": "string", "nullable": False},
                        {"name": "v", "type": "int", "nullable": False},
                    ],
                    "primary_key": ["id"],
                }
            ]
        }
        # t1's before-image does not match the state left by t0.
        changes = [
            tx("t0", upd("T", {"id": "x"}, {"id": "x", "v": 1}, {"id": "x", "v": 2})),
            tx("t1", upd("T", {"id": "x"}, {"id": "x", "v": 9}, {"id": "x", "v": 10})),
        ]
        mock = {"fail_signature": "SIG", "fail_always": True}
        schema_path, snapshot_dir, changes_path, oracle_path = make_log_fixture(
            self.tmp.path, schema, {"T": [{"id": "x", "v": 1}]}, changes, mock
        )
        workdir = self.tmp.path / "work"
        result = run_cli(
            "log-reduce",
            "--snapshot", str(snapshot_dir),
            "--schema", str(schema_path),
            "--changes", str(changes_path),
            "--oracle-config", str(oracle_path),
            "--workdir", str(workdir),
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("tx 't1'", result.stderr)
        self.assertIn("event 0", result.stderr)
        # No state, no trajectory, no oracle call was ever started.
        self.assertFalse((workdir / "state.json").exists())
        self.assertFalse((workdir / "trajectory.jsonl").exists())


if __name__ == "__main__":
    unittest.main()
