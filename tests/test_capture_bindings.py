"""capture-v1 bindings: source-to-target table and column renaming.

The archive may name tables and columns differently from the target schema.
Bindings must rename every image and key, enforce the one-to-one coverage
rules, and the restored package must use target names end to end while the
map retains source names for traceability.
"""

from __future__ import annotations

import json
import unittest

from ddreduce.capture.errors import CaptureArchiveError
from ddreduce.capture.importer import run_import
from ddreduce.capture.verifier import VERDICT_MATCH, run_verify
from ddreduce.logmodel import load_changelog
from ddreduce.model import Database
from ddreduce.replay import validate_log_inputs
from ddreduce.schema import load_schema

from .capture_helpers import change, commit, write_archive
from .helpers import TempDir

# Target schema uses different table and column names.
TARGET_SCHEMA = {
    "tables": [
        {
            "name": "accounts",
            "columns": [
                {"name": "account_key", "type": "string", "nullable": False},
                {"name": "balance", "type": "int", "nullable": False},
                {"name": "memo", "type": "string", "nullable": True},
            ],
            "primary_key": ["account_key"],
            "unique_keys": [["memo"]],
        }
    ]
}

RENAME_BINDINGS = {
    "tables": {
        "acct": {
            "target": "accounts",
            "columns": {
                "k": "account_key",
                "amt": "balance",
                "txt": "memo",
            },
        }
    }
}


class RenamingBindingsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()

    def tearDown(self):
        self.tmp.cleanup()

    def test_source_names_are_renamed_through_bindings(self):
        base = self.tmp.path / "renamed"
        # Source-level messages use acct/k/amt/txt names.
        messages = [
            change("s1", 0, "t1", 1, "acct", "update", {"k": "x"},
                   before={"amt": 1}, after={"amt": 2}),
            change("s1", 1, "t1", 2, "acct", "update", {"k": "x"},
                   before={"txt": "note"}, after={"txt": None}),
            commit("s1", 2, "t1", 1, 2, {"acct": 2}),
            change("s1", 3, "t2", 1, "acct", "insert", {"k": "y"},
                   after={"k": "y", "amt": 9, "txt": "fresh"}),
            commit("s1", 4, "t2", 2, 1, {"acct": 1}),
        ]
        snapshot = {"accounts": [
            {"account_key": "x", "balance": 1, "memo": "note"},
        ]}
        archive, snap, spath = write_archive(
            base, messages, schema=TARGET_SCHEMA, snapshot=snapshot,
            bindings=RENAME_BINDINGS,
        )
        result = run_import(archive, snap, spath, base / "pkg", 2)

        # Output changes use target names only.
        lines = [
            json.loads(line)
            for line in (base / "pkg" / "changes.jsonl").read_text().splitlines()
        ]
        self.assertEqual(lines[0]["events"][0]["table"], "accounts")
        self.assertEqual(lines[0]["events"][0]["key"], {"account_key": "x"})
        self.assertEqual(
            lines[0]["events"][0]["after"],
            {"account_key": "x", "balance": 2, "memo": "note"},
        )
        self.assertEqual(
            lines[0]["events"][1]["after"],
            {"account_key": "x", "balance": 2, "memo": None},
        )
        self.assertEqual(lines[1]["events"][0]["after"],
                         {"account_key": "y", "balance": 9, "memo": "fresh"})

        # The map keeps both source-level and target-level naming.
        map_rows = [
            json.loads(line)
            for line in (base / "pkg" / "capture-map.jsonl").read_text().splitlines()
        ]
        first = map_rows[0]
        self.assertEqual(first["source"]["source_table"], "acct")
        self.assertEqual(first["output_table"], "accounts")
        self.assertEqual(first["output_key"], {"account_key": "x"})

        # The restored package is a valid target-schema log and verifies.
        schema = load_schema(spath)
        db = Database.load(schema, snap)
        log = load_changelog(base / "pkg" / "changes.jsonl", schema)
        validated = validate_log_inputs(
            schema, spath.read_text("utf-8"), db, log
        )
        final = validated.normalize({0, 1}).final_database()
        self.assertEqual(
            final.rows['accounts/str:"x"'].data,
            {"account_key": "x", "balance": 2, "memo": None},
        )
        verify = run_verify(archive, snap, spath, 2, base / "pkg")
        self.assertEqual(verify.verdict, VERDICT_MATCH, verify.failures)
        self.assertEqual(
            result.report["bindings"], RENAME_BINDINGS,
        )

    def test_unbound_source_table_in_message_is_rejected(self):
        base = self.tmp.path / "unbound"
        messages = [
            change("s1", 0, "t1", 1, "other", "insert", {"k": "z"},
                   after={"k": "z", "amt": 1, "txt": None}),
            commit("s1", 1, "t1", 1, 1, {"other": 1}),
        ]
        # The archive can be written, but importing it must refuse the
        # unbound source table during the structural message scan.
        archive, snap, spath = write_archive(
            base, messages, schema=TARGET_SCHEMA,
            snapshot={"accounts": []}, bindings=RENAME_BINDINGS,
        )
        with self.assertRaises(CaptureArchiveError):
            run_import(archive, snap, spath, base / "pkg", 1)

    def test_type_rules_apply_after_renaming(self):
        base = self.tmp.path / "typed"
        # A boolean pretending to be an int balance must be rejected.
        messages = [
            change("s1", 0, "t1", 1, "acct", "update", {"k": "x"},
                   before={"amt": 1}, after={"amt": True}),
            commit("s1", 1, "t1", 1, 1, {"acct": 1}),
        ]
        archive, snap, spath = write_archive(
            base, messages, schema=TARGET_SCHEMA,
            snapshot={"accounts": [
                {"account_key": "x", "balance": 1, "memo": None},
            ]},
            bindings=RENAME_BINDINGS,
        )
        with self.assertRaises(Exception):
            run_import(archive, snap, spath, base / "pkg", 1)


if __name__ == "__main__":
    unittest.main()
