"""CDC change-log contract: structural and replay-time validation.

Every illegality is reported with the transaction and event position, and an
invalid original log never reaches the oracle command.
"""

from __future__ import annotations

import unittest
from pathlib import Path

from ddreduce.logmodel import LogContractError, load_changelog
from ddreduce.model import Database
from ddreduce.replay import validate_log_inputs
from ddreduce.schema import load_schema

from .helpers import TempDir, dele, ins, make_log_fixture, tx, upd, write_jsonl

T_SCHEMA = {
    "tables": [
        {
            "name": "T",
            "columns": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "v", "type": "int", "nullable": False},
                {"name": "tag", "type": "string", "nullable": True},
            ],
            "primary_key": ["id"],
            "unique_keys": [["tag"]],
        },
    ]
}

FK_SCHEMA = {
    "tables": [
        {
            "name": "P",
            "columns": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "v", "type": "int", "nullable": False},
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

CYCLE_SCHEMA = {
    "tables": [
        {
            "name": "A",
            "columns": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "b_id", "type": "string", "nullable": False},
            ],
            "primary_key": ["id"],
            "foreign_keys": [
                {"columns": ["b_id"], "ref_table": "B", "ref_columns": ["id"]}
            ],
        },
        {
            "name": "B",
            "columns": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "a_id", "type": "string", "nullable": False},
            ],
            "primary_key": ["id"],
            "foreign_keys": [
                {"columns": ["a_id"], "ref_table": "A", "ref_columns": ["id"]}
            ],
        },
    ]
}


class LogContractCase(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()

    def tearDown(self):
        self.tmp.cleanup()

    def validated(self, schema, snapshot, changes, name="case"):
        base = self.tmp.path / name
        schema_path, snapshot_dir, changes_path, _ = make_log_fixture(
            base, schema, snapshot, changes
        )
        schema_obj = load_schema(schema_path)
        schema_text = Path(schema_path).read_text(encoding="utf-8")
        db = Database.load(schema_obj, snapshot_dir)
        log = load_changelog(changes_path, schema_obj)
        return validate_log_inputs(schema_obj, schema_text, db, log)

    def expect_invalid(self, schema, snapshot, changes, *needles, name="bad"):
        with self.assertRaises(LogContractError) as ctx:
            self.validated(schema, snapshot, changes, name=name)
        message = str(ctx.exception)
        for needle in needles:
            self.assertIn(needle, message)
        return message


class StructuralValidationTest(LogContractCase):
    def test_valid_log_loads_and_chained_events_share_private_state(self):
        # One transaction updates the same key twice; the second event reads
        # the version privately produced by the first event of the same tx.
        v = self.validated(
            T_SCHEMA,
            {"T": [{"id": "x", "v": 1, "tag": None}]},
            [
                tx(
                    "t1",
                    upd("T", {"id": "x"}, {"id": "x", "v": 1, "tag": None},
                        {"id": "x", "v": 2, "tag": None}),
                    upd("T", {"id": "x"}, {"id": "x", "v": 2, "tag": None},
                        {"id": "x", "v": 3, "tag": None}),
                )
            ],
        )
        reads = v.witnesses[0].event_reads
        self.assertEqual(reads[0].read, 'B|T/str:"x"')
        self.assertEqual(reads[1].read, "T|0|0")
        final = v.normalize({0}).final_database()
        self.assertEqual(final.rows['T/str:"x"'].data["v"], 3)

    def test_duplicate_tx_id_rejected(self):
        self.expect_invalid(
            T_SCHEMA, {"T": []},
            [tx("t1", ins("T", {"id": "a"}, {"id": "a", "v": 1, "tag": None})),
             tx("t1", ins("T", {"id": "b"}, {"id": "b", "v": 2, "tag": None}))],
            "duplicate tx_id",
        )

    def test_empty_events_rejected(self):
        self.expect_invalid(
            T_SCHEMA, {"T": []}, [{"tx_id": "t1", "events": []}],
            "non-empty",
        )

    def test_unknown_op_and_unknown_table_rejected(self):
        self.expect_invalid(
            T_SCHEMA, {"T": []},
            [tx("t1", {"op": "merge", "table": "T", "key": {"id": "a"},
                       "after": {"id": "a", "v": 1, "tag": None}})],
            "op",
        )
        self.expect_invalid(
            T_SCHEMA, {"T": []},
            [tx("t1", ins("ZZ", {"id": "a"}, {"id": "a", "v": 1, "tag": None}))],
            "unknown table",
        )

    def test_key_must_match_row_image(self):
        self.expect_invalid(
            T_SCHEMA, {"T": []},
            [tx("t1", ins("T", {"id": "a"}, {"id": "b", "v": 1, "tag": None}))],
            "key does not match",
        )

    def test_update_must_not_change_primary_key(self):
        self.expect_invalid(
            T_SCHEMA,
            {"T": [{"id": "x", "v": 1, "tag": None}]},
            [tx("t1", upd("T", {"id": "x"}, {"id": "x", "v": 1, "tag": None},
                          {"id": "y", "v": 1, "tag": None}))],
            "must not change primary key",
        )

    def test_incomplete_row_image_rejected(self):
        self.expect_invalid(
            T_SCHEMA, {"T": []},
            [tx("t1", ins("T", {"id": "a"}, {"id": "a", "v": 1}))],
            "incomplete row image",
        )

    def test_unknown_column_rejected(self):
        self.expect_invalid(
            T_SCHEMA, {"T": []},
            [tx("t1", ins("T", {"id": "a"}, {"id": "a", "v": 1, "tag": None, "zz": 1}))],
            "unknown columns",
        )

    def test_null_in_not_null_and_bool_not_int_rejected(self):
        self.expect_invalid(
            T_SCHEMA, {"T": []},
            [tx("t1", ins("T", {"id": "a"}, {"id": "a", "v": None, "tag": None}))],
            "NOT NULL",
        )
        self.expect_invalid(
            T_SCHEMA, {"T": []},
            [tx("t1", ins("T", {"id": "a"}, {"id": "a", "v": True, "tag": None}))],
            "expected int",
        )


class ReplayValidationTest(LogContractCase):
    def test_insert_existing_reports_tx_and_event(self):
        message = self.expect_invalid(
            T_SCHEMA,
            {"T": [{"id": "x", "v": 1, "tag": None}]},
            [tx("tA", ins("T", {"id": "x"}, {"id": "x", "v": 9, "tag": None}))],
            "tx 'tA'", "event 0", "insert requires an absent key",
        )
        self.assertIn("baseline row", message)

    def test_update_before_mismatch_reports_position(self):
        self.expect_invalid(
            T_SCHEMA,
            {"T": [{"id": "x", "v": 1, "tag": None}]},
            [tx("tB", upd("T", {"id": "x"}, {"id": "x", "v": 2, "tag": None},
                          {"id": "x", "v": 3, "tag": None}))],
            "tx 'tB'", "event 0", "before-image does not match",
        )

    def test_delete_missing_reports_position(self):
        self.expect_invalid(
            T_SCHEMA,
            {"T": []},
            [tx("tC", dele("T", {"id": "x"}, {"id": "x", "v": 1, "tag": None}))],
            "tx 'tC'", "event 0", "delete requires an existing row",
        )

    def test_second_event_position_reported(self):
        self.expect_invalid(
            T_SCHEMA,
            {"T": [{"id": "x", "v": 1, "tag": None}]},
            [tx(
                "tD",
                upd("T", {"id": "x"}, {"id": "x", "v": 1, "tag": None},
                    {"id": "x", "v": 2, "tag": None}),
                dele("T", {"id": "x"}, {"id": "x", "v": 1, "tag": None}),
            )],
            "tx 'tD'", "event 1",
        )

    def test_commit_unique_violation_reports_tx(self):
        # Two rows ending with the same unique tag inside one transaction.
        self.expect_invalid(
            T_SCHEMA,
            {"T": [
                {"id": "u1", "v": 1, "tag": "a"},
                {"id": "u2", "v": 2, "tag": "b"},
            ]},
            [tx("tU", upd("T", {"id": "u2"}, {"id": "u2", "v": 2, "tag": "b"},
                          {"id": "u2", "v": 2, "tag": "a"}))],
            "tx 'tU'", "commit violates", "unique constraint",
        )

    def test_commit_fk_violation_reports_tx(self):
        self.expect_invalid(
            FK_SCHEMA,
            {"P": [], "C": []},
            [tx("tF", ins("C", {"id": "c1"}, {"id": "c1", "p_id": "missing"}))],
            "tx 'tF'", "commit violates", "foreign key",
        )


class CommitTimeSemanticsTest(LogContractCase):
    """Cross-row constraints are checked at commit, not per event."""

    def test_unique_value_swap_inside_one_transaction_commits(self):
        v = self.validated(
            T_SCHEMA,
            {"T": [
                {"id": "u1", "v": 1, "tag": "a"},
                {"id": "u2", "v": 2, "tag": "b"},
            ]},
            [tx(
                "swap",
                upd("T", {"id": "u1"}, {"id": "u1", "v": 1, "tag": "a"},
                    {"id": "u1", "v": 1, "tag": "b"}),
                upd("T", {"id": "u2"}, {"id": "u2", "v": 2, "tag": "b"},
                    {"id": "u2", "v": 2, "tag": "a"}),
            )],
        )
        final = v.normalize({0}).final_database()
        self.assertEqual(final.rows['T/str:"u1"'].data["tag"], "b")
        self.assertEqual(final.rows['T/str:"u2"'].data["tag"], "a")

    def test_unique_swap_split_across_transactions_is_invalid(self):
        # The same swap as two transactions: the first commit already
        # violates uniqueness, so the original log is rejected.
        self.expect_invalid(
            T_SCHEMA,
            {"T": [
                {"id": "u1", "v": 1, "tag": "a"},
                {"id": "u2", "v": 2, "tag": "b"},
            ]},
            [
                tx("s1", upd("T", {"id": "u1"}, {"id": "u1", "v": 1, "tag": "a"},
                             {"id": "u1", "v": 1, "tag": "b"})),
                tx("s2", upd("T", {"id": "u2"}, {"id": "u2", "v": 2, "tag": "b"},
                             {"id": "u2", "v": 2, "tag": "a"})),
            ],
            "tx 's1'", "commit violates", "unique constraint",
        )

    def test_child_before_parent_inside_one_transaction_commits(self):
        v = self.validated(
            FK_SCHEMA,
            {"P": [], "C": []},
            [tx(
                "mk",
                ins("C", {"id": "c1"}, {"id": "c1", "p_id": "p1"}),
                ins("P", {"id": "p1"}, {"id": "p1", "v": 7}),
            )],
        )
        final = v.normalize({0}).final_database()
        self.assertIn('P/str:"p1"', final.rows)
        self.assertIn('C/str:"c1"', final.rows)

    def test_cyclic_foreign_keys_inside_one_transaction_commit(self):
        v = self.validated(
            CYCLE_SCHEMA,
            {"A": [], "B": []},
            [tx(
                "cyc",
                ins("A", {"id": "a1"}, {"id": "a1", "b_id": "b1"}),
                ins("B", {"id": "b1"}, {"id": "b1", "a_id": "a1"}),
            )],
        )
        final = v.normalize({0}).final_database()
        self.assertEqual(len(final.rows), 2)

    def test_file_order_is_business_order_not_tx_id_order(self):
        # tx_ids sort in the opposite order of the file; the second line must
        # still see the first line's write.
        v = self.validated(
            T_SCHEMA,
            {"T": []},
            [
                tx("t9", ins("T", {"id": "x"}, {"id": "x", "v": 1, "tag": None})),
                tx("t1", upd("T", {"id": "x"}, {"id": "x", "v": 1, "tag": None},
                             {"id": "x", "v": 2, "tag": None})),
            ],
        )
        self.assertEqual(v.witnesses[1].event_reads[0].read, "T|0|0")
        final = v.normalize({0, 1}).final_database()
        self.assertEqual(final.rows['T/str:"x"'].data["v"], 2)


class AtomicCommitTest(LogContractCase):
    def test_failed_commit_skips_whole_transaction_without_leaking_writes(self):
        # txP inserts parent p2; txX inserts k1 AND a child of p2; txY updates
        # k1. Dropping txP makes txX's commit fail (dangling required FK), so
        # neither k1 nor the child may become visible to txY.
        changes = [
            tx("txP", ins("P", {"id": "p2"}, {"id": "p2", "v": 1})),
            tx(
                "txX",
                ins("T", {"id": "k1"}, {"id": "k1", "v": 5, "tag": None}),
                ins("C", {"id": "c1"}, {"id": "c1", "p_id": "p2"}),
            ),
            tx("txY", upd("T", {"id": "k1"}, {"id": "k1", "v": 5, "tag": None},
                          {"id": "k1", "v": 6, "tag": None})),
        ]
        schema = {
            "tables": [
                T_SCHEMA["tables"][0],
                FK_SCHEMA["tables"][0],
                FK_SCHEMA["tables"][1],
            ]
        }
        v = self.validated(
            schema, {"T": [], "P": [], "C": []}, changes, name="atomic"
        )
        # Full log is valid and commits everything.
        full = v.normalize({0, 1, 2})
        self.assertEqual(full.committed, [0, 1, 2])
        # Drop txP: txX fails its commit (missing parent), txY then cannot
        # read k1's version because txX never committed.
        result = v.normalize({1, 2})
        self.assertEqual(result.committed, [])
        skipped = {d.tx_id: d for d in result.dispositions if d.status == "skipped"}
        self.assertEqual(set(skipped), {"txX", "txY"})
        self.assertIn("foreign key", skipped["txX"].reason)
        self.assertIn("row version written by tx position 1", skipped["txY"].reason)
        # No partial write leaked: the final state is exactly the snapshot.
        self.assertEqual(len(result.final_database().rows), 0)


if __name__ == "__main__":
    unittest.main()
