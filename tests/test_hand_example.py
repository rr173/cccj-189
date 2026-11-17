"""Hand-computed small example + exhaustive legal-deletion enumeration.

P has p and q; C has bad row b depending on p and unrelated g depending on q.
The target FAIL is triggered only by b.  Expected result: keep p and b.
Deleting p cascades to b and yields PASS; deleting b alone yields PASS;
deleting q cascades to g and the fault remains, so q/g must disappear.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from ddreduce.model import Database, all_legal_deletions
from ddreduce.oracle import OracleConfig
from ddreduce.schema import load_schema

from .helpers import TempDir, make_fixture, run_cli

SCHEMA = {
    "tables": [
        {
            "name": "P",
            "columns": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "label", "type": "string", "nullable": True},
            ],
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

TABLES = {
    "P": [{"id": "p"}, {"id": "q"}],
    "C": [{"id": "b", "p_id": "p"}, {"id": "g", "p_id": "q"}],
}

MOCK = {"fail_rows": ["C/b"], "fail_signature": "SIG-BAD-ROW"}


class HandExampleTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        self.schema_path, self.tables_dir, self.oracle_path = make_fixture(
            self.tmp.path, SCHEMA, TABLES, MOCK
        )
        self.workdir = self.tmp.path / "work"

    def tearDown(self):
        self.tmp.cleanup()

    def test_closure_math_by_hand(self):
        schema = load_schema(self.schema_path)
        db = Database.load(schema, self.tables_dir)
        # Delete p => b cascades; delete q => g cascades; cycle-free closure.
        self.assertEqual(
            db.deletion_closure(['P/str:"p"']), {'P/str:"p"', 'C/str:"b"'}
        )
        self.assertEqual(
            db.deletion_closure(['P/str:"q"']), {'P/str:"q"', 'C/str:"g"'}
        )
        self.assertEqual(db.deletion_closure(['C/str:"b"']), {'C/str:"b"'})
        # Exhaustive enumeration of every distinct legal deletion.
        deletions = all_legal_deletions(db)
        closures = {frozenset(d) for d in deletions}
        self.assertIn(frozenset(), closures)
        self.assertIn(frozenset({'P/str:"p"', 'C/str:"b"'}), closures)
        self.assertIn(frozenset({'P/str:"q"', 'C/str:"g"'}), closures)
        # Removing b without p directly is legal; removing b and q cascades g.
        self.assertIn(frozenset({'C/str:"b"'}), closures)
        self.assertIn(
            frozenset({'C/str:"b"', 'P/str:"q"', 'C/str:"g"'}), closures
        )
        # Every legal subset is still constraint-valid.
        for removed in deletions:
            db.apply_deletion(set(removed)).validate()

    def test_reduce_cli_and_certificate(self):
        result = run_cli(
            "reduce",
            "--input", str(self.tables_dir),
            "--schema", str(self.schema_path),
            "--oracle-config", str(self.oracle_path),
            "--workdir", str(self.workdir),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        with open(self.workdir / "certificate.json", encoding="utf-8") as fh:
            cert = json.load(fh)
        self.assertEqual(cert["target_signature"], "SIG-BAD-ROW")
        self.assertEqual(
            sorted(cert["result"]["row_ids"]), ['C/str:"b"', 'P/str:"p"']
        )
        # Result package on disk contains exactly p and b.
        p_rows = [
            json.loads(line)
            for line in (self.workdir / "result" / "P.jsonl").read_text().splitlines()
        ]
        c_rows = [
            json.loads(line)
            for line in (self.workdir / "result" / "C.jsonl").read_text().splitlines()
        ]
        self.assertEqual([r["id"] for r in p_rows], ["p"])
        self.assertEqual([r["id"] for r in c_rows], ["b"])

    def test_verify_independently_confirms_minimality(self):
        run_cli(
            "reduce",
            "--input", str(self.tables_dir),
            "--schema", str(self.schema_path),
            "--oracle-config", str(self.oracle_path),
            "--workdir", str(self.workdir),
        )
        result = run_cli(
            "verify",
            "--input", str(self.tables_dir),
            "--schema", str(self.schema_path),
            "--oracle-config", str(self.oracle_path),
            "--workdir", str(self.workdir),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(
            (self.workdir / "verify-report.json").read_text(encoding="utf-8")
        )
        self.assertEqual(report["verdict"], "VERIFIED_MINIMAL")
        # Two strict neighbors (seed P/p removes p+b; seed C/b removes b),
        # both PASS independently re-tested.
        neighbor_verdicts = sorted(n["verdict"] for n in report["neighbors"])
        self.assertEqual(neighbor_verdicts, ["PASS", "PASS"])
        # The verifier did not read a minimality boolean from the searcher.
        self.assertTrue(all("verdict" in n for n in report["neighbors"]))

    def test_trajectory_records_calls_and_acceptance(self):
        run_cli(
            "reduce",
            "--input", str(self.tables_dir),
            "--schema", str(self.schema_path),
            "--oracle-config", str(self.oracle_path),
            "--workdir", str(self.workdir),
        )
        traj = [
            json.loads(line)
            for line in (self.workdir / "trajectory.jsonl").read_text().splitlines()
        ]
        # The baseline is one aggregated entry charging confirm_runs (2)
        # subprocess calls, then the empty probe, then reduction candidates.
        charged = [e for e in traj if e["subprocess_runs"] > 0]
        baseline = charged[0]
        self.assertEqual(baseline["subprocess_runs"], 2)
        self.assertEqual(baseline["verdict"], "FAIL")
        self.assertEqual(baseline["signature"], "SIG-BAD-ROW")
        # Every trajectory entry is one of the contract verdicts.
        self.assertTrue(
            all(e["verdict"] in {"PASS", "FAIL", "UNRESOLVED", "SAME"} for e in traj)
        )
        self.assertEqual(
            sum(e["subprocess_runs"] for e in traj),
            json.loads((self.workdir / "state.json").read_text())["calls_used"],
        )


if __name__ == "__main__":
    unittest.main()
