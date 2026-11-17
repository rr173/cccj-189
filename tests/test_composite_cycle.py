"""Composite typed primary keys and cyclic foreign keys handled as a closure."""

from __future__ import annotations

import json
import unittest

from ddreduce.model import Database
from ddreduce.schema import load_schema

from .helpers import TempDir, make_fixture, run_cli

# Node has a composite PK (zone:int, code:string) and a self-referencing,
# NOT-NULL FK to another node: cycles are legal and must be removed as a whole
# closure. A separate Detail table uses a composite FK back into Node.
SCHEMA = {
    "tables": [
        {
            "name": "node",
            "columns": [
                {"name": "zone", "type": "int", "nullable": False},
                {"name": "code", "type": "string", "nullable": False},
                {"name": "peer_zone", "type": "int", "nullable": False},
                {"name": "peer_code", "type": "string", "nullable": False},
            ],
            "primary_key": ["zone", "code"],
            "foreign_keys": [
                {
                    "columns": ["peer_zone", "peer_code"],
                    "ref_table": "node",
                    "ref_columns": ["zone", "code"],
                }
            ],
        },
        {
            "name": "detail",
            "columns": [
                {"name": "detail_id", "type": "int", "nullable": False},
                {"name": "node_zone", "type": "int", "nullable": False},
                {"name": "node_code", "type": "string", "nullable": False},
                {"name": "weight", "type": "float", "nullable": True},
            ],
            "primary_key": ["detail_id"],
            "foreign_keys": [
                {
                    "columns": ["node_zone", "node_code"],
                    "ref_table": "node",
                    "ref_columns": ["zone", "code"],
                }
            ],
        },
    ]
}

# n1 <-> n2 form a required-FK cycle; n3 -> n1 (not cyclic); detail rows d1,d2.
TABLES = {
    "node": [
        {"zone": 1, "code": "n1", "peer_zone": 1, "peer_code": "n2"},
        {"zone": 1, "code": "n2", "peer_zone": 1, "peer_code": "n1"},
        {"zone": 1, "code": "n3", "peer_zone": 1, "peer_code": "n1"},
    ],
    "detail": [
        {"detail_id": 10, "node_zone": 1, "node_code": "n2", "weight": 1.5},
        {"detail_id": 20, "node_zone": 1, "node_code": "n3", "weight": 2.0},
    ],
}

# Fault is attached to the cycle via a predicate on composite-keyed node n2.
MOCK = {
    "fail_predicates": [
        {"table": "node", "column": "code", "equals": "n2", "label": "node-n2"}
    ],
    "fail_signature": "SIG-CYCLE",
}


class CompositeCycleTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        self.schema_path, self.tables_dir, self.oracle_path = make_fixture(
            self.tmp.path, SCHEMA, TABLES, MOCK
        )
        self.workdir = self.tmp.path / "work"

    def tearDown(self):
        self.tmp.cleanup()

    def test_cycle_closure_is_whole(self):
        schema = load_schema(self.schema_path)
        db = Database.load(schema, self.tables_dir)
        n1 = "node/int:1/str:\"n1\""
        n2 = "node/int:1/str:\"n2\""
        n3 = "node/int:1/str:\"n3\""
        d10 = "detail/int:10"
        d20 = "detail/int:20"
        self.assertEqual(
            db.deletion_closure([n1]),
            {n1, n2, n3, d10, d20},
        )
        # Seeding with n2 alone reaches the same full closure.
        self.assertEqual(db.deletion_closure([n2]), {n1, n2, n3, d10, d20})
        # A detail-only deletion stays local.
        self.assertEqual(db.deletion_closure([d20]), {d20})

    def test_reducer_keeps_the_cycle_and_its_bad_detail(self):
        result = run_cli(
            "reduce",
            "--input", str(self.tables_dir),
            "--schema", str(self.schema_path),
            "--oracle-config", str(self.oracle_path),
            "--workdir", str(self.workdir),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        cert = json.loads((self.workdir / "certificate.json").read_text())
        kept = set(cert["result"]["row_ids"])
        # n1,n2 cycle must survive together; n3/d20 (no role in the fault) gone.
        self.assertIn("node/int:1/str:\"n1\"", kept)
        self.assertIn("node/int:1/str:\"n2\"", kept)
        self.assertNotIn("node/int:1/str:\"n3\"", kept)
        self.assertNotIn("detail/int:20", kept)
        # Result still validates and still fails with the target signature.
        verify = run_cli(
            "verify",
            "--input", str(self.tables_dir),
            "--schema", str(self.schema_path),
            "--oracle-config", str(self.oracle_path),
            "--workdir", str(self.workdir),
        )
        self.assertEqual(verify.returncode, 0, verify.stderr)
        report = json.loads((self.workdir / "verify-report.json").read_text())
        self.assertEqual(report["verdict"], "VERIFIED_MINIMAL")


if __name__ == "__main__":
    unittest.main()
