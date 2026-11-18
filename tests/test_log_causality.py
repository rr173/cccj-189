"""Hand-computed causality scenarios for version/absence-aware replay.

Scenario A (overwrite-update chain): baseline x=1; a: 1->2; b: 2->1; c: 1->9.
Deleting a makes b and c lose their original versions; c must never rebind to
the baseline row merely because it holds the same value 1.

Scenario B (tombstone chain): empty baseline; i: insert x=1; d: delete x;
r: insert x=2.  Removing the prefix must not let the rebuild treat the
initial absence as the delete's tombstone.

Scenario C (parent versions): P goes v1->v2->v1 across a and b; c inserts a
child of P.  Dropping a and b must not let c rebind to the baseline parent
row even though it holds the same value.
"""

from __future__ import annotations

import unittest
from pathlib import Path

from ddreduce.logmodel import load_changelog
from ddreduce.model import Database
from ddreduce.replay import validate_log_inputs
from ddreduce.schema import load_schema

from .helpers import TempDir, dele, ins, make_log_fixture, tx, upd

X_SCHEMA = {
    "tables": [
        {
            "name": "X",
            "columns": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "v", "type": "int", "nullable": False},
            ],
            "primary_key": ["id"],
        },
    ]
}

PC_SCHEMA = {
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


def x_row(v):
    return {"id": "x", "v": v}


class CausalityCase(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()

    def tearDown(self):
        self.tmp.cleanup()

    def validated(self, schema, snapshot, changes, name):
        base = self.tmp.path / name
        schema_path, snapshot_dir, changes_path, _ = make_log_fixture(
            base, schema, snapshot, changes
        )
        schema_obj = load_schema(schema_path)
        schema_text = Path(schema_path).read_text(encoding="utf-8")
        db = Database.load(schema_obj, snapshot_dir)
        log = load_changelog(changes_path, schema_obj)
        return validate_log_inputs(schema_obj, schema_text, db, log)


class OverwriteChainTest(CausalityCase):
    """baseline x=1; a: x 1->2; b: x 2->1; c: x 1->9."""

    def setUp(self):
        super().setUp()
        self.v = self.validated(
            X_SCHEMA,
            {"X": [x_row(1)]},
            [
                tx("a", upd("X", {"id": "x"}, x_row(1), x_row(2))),
                tx("b", upd("X", {"id": "x"}, x_row(2), x_row(1))),
                tx("c", upd("X", {"id": "x"}, x_row(1), x_row(9))),
            ],
            "chain",
        )
        # tx positions: a=0, b=1, c=2
        self.tx_pos = {"a": 0, "b": 1, "c": 2}

    def final_v(self, result):
        rows = result.final_database().rows
        return rows['X/str:"x"'].data["v"] if rows else None

    def check(self, keep_ids, expected_committed, expected_v):
        keep = {self.tx_pos[t] for t in keep_ids}
        result = self.v.normalize(keep)
        self.assertEqual(
            result.committed,
            sorted(self.tx_pos[t] for t in expected_committed),
            f"keep={keep_ids}",
        )
        self.assertEqual(self.final_v(result), expected_v, f"keep={keep_ids}")
        return result

    def test_all_hand_computed_selections(self):
        self.check(set(), [], 1)
        self.check({"a"}, ["a"], 2)
        self.check({"b"}, [], 1)          # b reads a's version: gone
        self.check({"c"}, [], 1)          # c reads b's version: gone
        self.check({"a", "b"}, ["a", "b"], 1)
        self.check({"a", "c"}, ["a"], 2)  # c still misses b's version
        self.check({"a", "b", "c"}, ["a", "b", "c"], 9)

    def test_deleting_a_must_not_rebind_c_to_same_value_baseline(self):
        # The key acceptance case: b wrote back the same value the baseline
        # holds (1), yet c witnessed b's *version*, not the baseline row.
        result = self.check({"b", "c"}, [], 1)
        skipped = {d.tx_id: d for d in result.dispositions if d.status == "skipped"}
        self.assertEqual(set(skipped), {"b", "c"})
        self.assertIn("row version written by tx position 0", skipped["b"].reason)
        self.assertIn("row version written by tx position 1", skipped["c"].reason)
        # c's before-image (1) equals the baseline value, but the baseline
        # version is never a substitute: c stays skipped.
        self.assertNotIn("baseline", skipped["c"].reason.split("requires")[1].split(",")[0])

    def test_replay_is_deterministic(self):
        first = self.v.normalize({0, 2})
        second = self.v.normalize({0, 2})
        self.assertEqual(first.committed, second.committed)
        self.assertEqual(first.state_hash(), second.state_hash())


class TombstoneChainTest(CausalityCase):
    """empty baseline; i: insert x=1; d: delete x; r: insert x=2."""

    def setUp(self):
        super().setUp()
        self.v = self.validated(
            X_SCHEMA,
            {"X": []},
            [
                tx("i", ins("X", {"id": "x"}, x_row(1))),
                tx("d", dele("X", {"id": "x"}, x_row(1))),
                tx("r", ins("X", {"id": "x"}, x_row(2))),
            ],
            "tombstone",
        )
        self.tx_pos = {"i": 0, "d": 1, "r": 2}

    def check(self, keep_ids, expected_committed, expected_present, expected_v=None):
        keep = {self.tx_pos[t] for t in keep_ids}
        result = self.v.normalize(keep)
        self.assertEqual(
            result.committed,
            sorted(self.tx_pos[t] for t in expected_committed),
            f"keep={keep_ids}",
        )
        rows = result.final_database().rows
        self.assertEqual('X/str:"x"' in rows, expected_present, f"keep={keep_ids}")
        if expected_present:
            self.assertEqual(rows['X/str:"x"'].data["v"], expected_v)
        return result

    def test_all_hand_computed_selections(self):
        self.check(set(), [], False)
        self.check({"i"}, ["i"], True, 1)
        self.check({"d"}, [], False)       # d reads i's version: gone
        self.check({"r"}, [], False)       # r needs d's tombstone: not initial absence
        self.check({"i", "d"}, ["i", "d"], False)
        self.check({"i", "r"}, ["i"], True, 1)  # r needs the tombstone, key is present
        self.check({"d", "r"}, [], False)       # whole prefix gone: nothing commits
        self.check({"i", "d", "r"}, ["i", "d", "r"], True, 2)

    def test_tombstone_is_not_initial_absence(self):
        # Deleting the prefix leaves the *initial* absence of x; the rebuild
        # transaction r witnessed the delete's tombstone instead and must be
        # skipped rather than rebound.
        result = self.check({"r"}, [], False)
        skipped = [d for d in result.dispositions if d.status == "skipped"]
        self.assertEqual(len(skipped), 1)
        self.assertIn("absence from delete at tx position 1", skipped[0].reason)
        self.assertIn("initial absence", skipped[0].reason)


class ParentVersionTest(CausalityCase):
    """P: v1->v2 (a), v2->v1 (b); c inserts a child referencing P."""

    def setUp(self):
        super().setUp()
        self.v = self.validated(
            PC_SCHEMA,
            {"P": [{"id": "p", "v": 1}], "C": []},
            [
                tx("a", upd("P", {"id": "p"}, {"id": "p", "v": 1}, {"id": "p", "v": 2})),
                tx("b", upd("P", {"id": "p"}, {"id": "p", "v": 2}, {"id": "p", "v": 1})),
                tx("c", ins("C", {"id": "k"}, {"id": "k", "p_id": "p"})),
            ],
            "parent",
        )

    def test_child_witnesses_the_parent_version_at_commit(self):
        refs = self.v.witnesses[2].parent_refs
        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0].parent_version, "T|1|0")  # written by b

    def test_same_value_baseline_parent_is_not_a_substitute(self):
        # Drop a and b: the baseline parent row still exists with the same
        # value v=1, but c witnessed the version written by b.
        result = self.v.normalize({2})
        self.assertEqual(result.committed, [])
        skipped = [d for d in result.dispositions if d.status == "skipped"]
        self.assertEqual(len(skipped), 1)
        self.assertIn("commit requires parent", skipped[0].reason)
        self.assertIn("tx position 1", skipped[0].reason)
        self.assertIn("baseline row", skipped[0].reason)
        # The child row must not exist in the final state.
        self.assertEqual(len(result.final_database().rows), 1)  # only baseline P

    def test_keeping_b_without_a_also_fails(self):
        # b itself reads a's version, so b is skipped and c's parent premise
        # fails as well.
        result = self.v.normalize({1, 2})
        self.assertEqual(result.committed, [])
        skipped = {d.tx_id: d.status for d in result.dispositions}
        self.assertEqual(skipped, {"a": "deselected", "b": "skipped", "c": "skipped"})


class EmptyLogTest(CausalityCase):
    def test_deleting_everything_yields_the_empty_log(self):
        v = self.validated(
            X_SCHEMA,
            {"X": [x_row(1)]},
            [tx("a", upd("X", {"id": "x"}, x_row(1), x_row(2)))],
            "empty",
        )
        result = v.normalize(set())
        self.assertEqual(result.committed, [])
        candidate = v.candidate(result.committed)
        self.assertEqual(candidate.transactions, [])
        # The empty candidate materializes a valid, runnable package.
        out = self.tmp.path / "empty-pkg"
        candidate.materialize(out)
        self.assertEqual((out / "changes.jsonl").read_text(), "")
        self.assertTrue((out / "snapshot" / "X.jsonl").exists())
        self.assertTrue((out / "schema.json").exists())


if __name__ == "__main__":
    unittest.main()
