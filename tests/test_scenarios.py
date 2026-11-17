"""Orphan bad row, empty-database failure, alternative fault groups, signature drift."""

from __future__ import annotations

import json
import unittest

from .helpers import TempDir, make_fixture, run_cli

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
        {
            "name": "Solo",
            "columns": [{"name": "id", "type": "string", "nullable": False}],
            "primary_key": ["id"],
        },
    ]
}


class OrphanBadRowTest(unittest.TestCase):
    """A standalone bad row with no dependencies reduces to itself."""

    def setUp(self):
        self.tmp = TempDir()
        tables = {
            "P": [{"id": "p1"}, {"id": "p2"}],
            "C": [{"id": "c1", "p_id": "p1"}, {"id": "c2", "p_id": "p2"}],
            "Solo": [{"id": "s1"}, {"id": "s2"}],
        }
        mock = {"fail_rows": ["Solo/s2"], "fail_signature": "SIG-ORPHAN"}
        self.schema_path, self.tables_dir, self.oracle_path = make_fixture(
            self.tmp.path, SCHEMA, tables, mock
        )
        self.workdir = self.tmp.path / "work"

    def tearDown(self):
        self.tmp.cleanup()

    def test_reduces_to_single_orphan_row(self):
        result = run_cli(
            "reduce",
            "--input", str(self.tables_dir),
            "--schema", str(self.schema_path),
            "--oracle-config", str(self.oracle_path),
            "--workdir", str(self.workdir),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        cert = json.loads((self.workdir / "certificate.json").read_text())
        self.assertEqual(cert["result"]["row_ids"], ['Solo/str:"s2"'])


class EmptyStillFailsTest(unittest.TestCase):
    """When the command fails even on the empty DB, the empty DB is 1-minimal."""

    def setUp(self):
        self.tmp = TempDir()
        tables = {
            "P": [{"id": "p1"}],
            "C": [{"id": "c1", "p_id": "p1"}],
            "Solo": [{"id": "s1"}],
        }
        # No trigger rows at all: every candidate, including empty, fails.
        mock = {
            "fail_predicates": [
                {"table": "P", "column": "id", "equals": "__never__", "label": "x"}
            ],
            "fail_signature": "SIG-EMPTY",
            "fail_always": True,
        }
        # Simpler: add an unconditional marker understood via an unmatched set
        # is not supported; instead rely on a dedicated flag below.
        mock = {"fail_signature": "SIG-EMPTY", "fail_always": True}
        self.schema_path, self.tables_dir, self.oracle_path = make_fixture(
            self.tmp.path, SCHEMA, tables, mock
        )
        self.workdir = self.tmp.path / "work"

    def tearDown(self):
        self.tmp.cleanup()

    def test_empty_package_is_certified(self):
        result = run_cli(
            "reduce",
            "--input", str(self.tables_dir),
            "--schema", str(self.schema_path),
            "--oracle-config", str(self.oracle_path),
            "--workdir", str(self.workdir),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        cert = json.loads((self.workdir / "certificate.json").read_text())
        self.assertEqual(cert["result"]["row_count"], 0)
        report = json.loads((self.workdir / "verify-report.json").read_text()) if (
            self.workdir / "verify-report.json"
        ).exists() else None
        # Empty package has no neighbors: verify also succeeds.
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
        self.assertEqual(report["neighbors"], [])


class AlternativeFaultGroupsTest(unittest.TestCase):
    """Two independent alternative fault groups: result keeps one group."""

    def setUp(self):
        self.tmp = TempDir()
        tables = {
            "P": [{"id": "pA"}, {"id": "pB"}],
            "C": [
                {"id": "bA", "p_id": "pA"},
                {"id": "bB", "p_id": "pB"},
            ],
            "Solo": [],
        }
        mock = {
            "fail_rows": ["C/bA", "C/bB"],
            "fail_signature": "SIG-ALT",
        }
        self.schema_path, self.tables_dir, self.oracle_path = make_fixture(
            self.tmp.path, SCHEMA, tables, mock
        )
        self.workdir = self.tmp.path / "work"

    def tearDown(self):
        self.tmp.cleanup()

    def test_keeps_one_complete_group(self):
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
        # Exactly one complete alternative group survives (parent + its bad
        # child); which one is fully determined by the fixed lexicographic
        # tie-break and chunk order of ddmin.
        self.assertIn(
            kept,
            [
                {'C/str:"bA"', 'P/str:"pA"'},
                {'C/str:"bB"', 'P/str:"pB"'},
            ],
        )
        self.assertEqual(len(kept), 2)
        # The remaining strict neighbors both PASS, independently verified.
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


class SignatureDriftTest(unittest.TestCase):
    """Same FAIL/trigger but a changed signature blocks the target certificate."""

    def setUp(self):
        self.tmp = TempDir()
        tables = {
            "P": [{"id": "p1"}, {"id": "p2"}],
            "C": [{"id": "c1", "p_id": "p1"}, {"id": "c2", "p_id": "p2"}],
            "Solo": [{"id": "s1"}],
        }
        # The same trigger row c1 keeps failing, but once the package shrinks
        # below 4 rows the command emits a different signature (e.g. its
        # summary format changed). The target signature can never anchor a
        # 1-minimal package.
        mock = {
            "fail_rows": ["C/c1"],
            "fail_signature": "SIG-TARGET",
            "shrink_signature": {
                "row_count_below": 4,
                "signature": "SIG-DRIFT",
            },
        }
        self.schema_path, self.tables_dir, self.oracle_path = make_fixture(
            self.tmp.path, SCHEMA, tables, mock
        )
        self.workdir = self.tmp.path / "work"

    def tearDown(self):
        self.tmp.cleanup()

    def test_different_signature_neighbor_blocks_certificate(self):
        result = run_cli(
            "reduce",
            "--input", str(self.tables_dir),
            "--schema", str(self.schema_path),
            "--oracle-config", str(self.oracle_path),
            "--workdir", str(self.workdir),
        )
        # Not certified: every strict smaller FAILing neighbor has drifted to
        # another signature, which neither proves retention nor disappearance.
        self.assertNotEqual(result.returncode, 0)
        state = json.loads((self.workdir / "state.json").read_text())
        self.assertEqual(state["status"], "BLOCKED")


if __name__ == "__main__":
    unittest.main()
