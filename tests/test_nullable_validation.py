"""Nullable FK semantics: no hard dependency, dangling references stay legal."""

from __future__ import annotations

import json
import unittest

from ddreduce.model import Database
from ddreduce.schema import load_schema
from ddreduce.values import DataError

from .helpers import TempDir, make_fixture, run_cli

SCHEMA = {
    "tables": [
        {
            "name": "P",
            "columns": [
                {"name": "id", "type": "string", "nullable": False},
            ],
            "primary_key": ["id"],
        },
        {
            "name": "C",
            "columns": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "p_id", "type": "string", "nullable": True},
            ],
            "primary_key": ["id"],
            "foreign_keys": [
                {"columns": ["p_id"], "ref_table": "P", "ref_columns": ["id"]}
            ],
        },
    ]
}


class NullableFkTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        tables = {
            "P": [{"id": "p1"}, {"id": "p2"}],
            "C": [
                {"id": "c1", "p_id": "p1"},
                {"id": "c2", "p_id": "p2"},
                {"id": "c3", "p_id": None},
            ],
        }
        mock = {"fail_rows": ["C/c3"], "fail_signature": "SIG-NULLABLE"}
        self.schema_path, self.tables_dir, self.oracle_path = make_fixture(
            self.tmp.path, SCHEMA, tables, mock
        )
        self.workdir = self.tmp.path / "work"

    def tearDown(self):
        self.tmp.cleanup()

    def test_nullable_fk_creates_no_edge(self):
        schema = load_schema(self.schema_path)
        db = Database.load(schema, self.tables_dir)
        # Deleting a parent does NOT cascade to its nullable-FK child.
        removed = db.deletion_closure(['P/str:"p1"'])
        self.assertEqual(removed, {'P/str:"p1"'})
        remaining = db.apply_deletion(removed)
        # The dangling nullable reference is accepted by the contract.
        remaining.validate()
        kept_ids = set(remaining.rows)
        self.assertIn('C/str:"c1"', kept_ids)
        self.assertEqual(remaining.rows['C/str:"c1"'].data["p_id"], "p1")

    def test_partial_null_composite_nullable_fk_is_ignored(self):
        # A nullable composite FK where only some columns are NULL must also
        # behave as "no dependency" (validated in schema/value tests too).
        schema = {
            "tables": [
                {
                    "name": "K",
                    "columns": [
                        {"name": "a", "type": "int", "nullable": False},
                        {"name": "b", "type": "string", "nullable": False},
                    ],
                    "primary_key": ["a", "b"],
                },
                {
                    "name": "D",
                    "columns": [
                        {"name": "id", "type": "int", "nullable": False},
                        {"name": "ka", "type": "int", "nullable": True},
                        {"name": "kb", "type": "string", "nullable": True},
                    ],
                    "primary_key": ["id"],
                    "foreign_keys": [
                        {
                            "columns": ["ka", "kb"],
                            "ref_table": "K",
                            "ref_columns": ["a", "b"],
                        }
                    ],
                },
            ]
        }
        tables = {
            "K": [{"a": 1, "b": "x"}],
            "D": [{"id": 7, "ka": None, "kb": "x"}],
        }
        base = self.tmp.path / "composite"
        schema_path, tables_dir, _ = make_fixture(
            base, schema, tables, {"fail_rows": [], "fail_signature": "X"}
        )
        loaded = Database.load(load_schema(schema_path), tables_dir)
        self.assertEqual(
            loaded.deletion_closure(['K/int:1/str:"x"']),
            {'K/int:1/str:"x"'},
        )

    def test_reducer_keeps_nullable_child_without_parent(self):
        result = run_cli(
            "reduce",
            "--input", str(self.tables_dir),
            "--schema", str(self.schema_path),
            "--oracle-config", str(self.oracle_path),
            "--workdir", str(self.workdir),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        cert = json.loads((self.workdir / "certificate.json").read_text())
        # Only the nullable-FK bad row remains; both parents and other kids gone.
        self.assertEqual(cert["result"]["row_ids"], ['C/str:"c3"'])
        c_lines = (self.workdir / "result" / "C.jsonl").read_text().strip().splitlines()
        self.assertEqual(len(c_lines), 1)
        self.assertIsNone(json.loads(c_lines[0])["p_id"])


class TypeAndUniqueValidationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()

    def tearDown(self):
        self.tmp.cleanup()

    def test_bool_is_not_int_and_duplicate_unique_rejected(self):
        schema = {
            "tables": [
                {
                    "name": "T",
                    "columns": [
                        {"name": "id", "type": "int", "nullable": False},
                        {"name": "flag", "type": "bool", "nullable": True},
                        {"name": "tag", "type": "string", "nullable": True},
                    ],
                    "primary_key": ["id"],
                    "unique_keys": [["tag"]],
                }
            ]
        }
        base = self.tmp.path / "u"
        schema_path, tables_dir, oracle_path = make_fixture(
            base,
            schema,
            {"T": [
                {"id": 1, "flag": True, "tag": "a"},
                {"id": 2, "flag": False, "tag": "a"},
            ]},
            {"fail_rows": [], "fail_signature": "X"},
        )
        result = run_cli(
            "reduce",
            "--input", str(tables_dir),
            "--schema", str(schema_path),
            "--oracle-config", str(oracle_path),
            "--workdir", str(base / "work"),
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unique constraint", result.stderr)

        # bool rejected where int is required.
        bad_dir = self.tmp.path / "bad"
        schema_path2, tables_dir2, oracle_path2 = make_fixture(
            bad_dir,
            schema,
            {"T": [{"id": True, "flag": True, "tag": "a"}]},
            {"fail_rows": [], "fail_signature": "X"},
        )
        result2 = run_cli(
            "reduce",
            "--input", str(tables_dir2),
            "--schema", str(schema_path2),
            "--oracle-config", str(oracle_path2),
            "--workdir", str(bad_dir / "work"),
        )
        self.assertNotEqual(result2.returncode, 0)
        self.assertIn("expected int", result2.stderr)


if __name__ == "__main__":
    unittest.main()
