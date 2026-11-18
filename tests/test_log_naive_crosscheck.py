"""Cross-check the product replay engine against a naive independent replayer.

The naive replayer (tests/naive_replay.py) shares no code with the product.
For a small log we enumerate every transaction selection and compare kept
sequences and final data states; we also compare per-commit states and the
neighbors reported by a real certificate.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from ddreduce.logmodel import load_changelog
from ddreduce.model import Database
from ddreduce.replay import validate_log_inputs
from ddreduce.schema import load_schema
from ddreduce.values import canonical_json

from .helpers import TempDir, dele, ins, make_log_fixture, run_cli, tx, upd
from .naive_replay import NaiveReplayer

SCHEMA = {
    "tables": [
        {
            "name": "T",
            "columns": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "v", "type": "int", "nullable": False},
            ],
            "primary_key": ["id"],
        },
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

SNAPSHOT = {"T": [], "P": [{"id": "p", "v": 1}], "C": []}

# A causally linked chain t0->t1->t2->t3 plus one irrelevant transaction t4.
#   t0: insert T x=1, insert T y=2        (multi-event tx)
#   t1: update T x 1->2, update T x 2->3  (chained private reads)
#   t2: delete T x                        (tombstone)
#   t3: rebuild T x=9; insert C k->p      (tombstone premise + FK parent)
#   t4: insert T z=5                      (irrelevant)
CHANGES = [
    tx("t0", ins("T", {"id": "x"}, {"id": "x", "v": 1}),
       ins("T", {"id": "y"}, {"id": "y", "v": 2})),
    tx("t1", upd("T", {"id": "x"}, {"id": "x", "v": 1}, {"id": "x", "v": 2}),
       upd("T", {"id": "x"}, {"id": "x", "v": 2}, {"id": "x", "v": 3})),
    tx("t2", dele("T", {"id": "x"}, {"id": "x", "v": 3})),
    tx("t3", ins("T", {"id": "x"}, {"id": "x", "v": 9}),
       ins("C", {"id": "k"}, {"id": "k", "p_id": "p"})),
    tx("t4", ins("T", {"id": "z"}, {"id": "z", "v": 5})),
]

MOCK = {"fail_tx_sets": [["t2", "t3"]], "fail_signature": "SIG-PAIR"}


def state_rows(db: Database) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for row in db.rows.values():
        out.setdefault(row.table, []).append(canonical_json(row.data))
    return {t: sorted(rows) for t, rows in out.items()}


def naive_state_rows(state: dict[str, dict]) -> dict[str, list[str]]:
    return {
        t: sorted(canonical_json(r) for r in rows.values())
        for t, rows in state.items()
        if rows
    }


class NaiveCrossCheckTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        self.schema_path, self.snapshot_dir, self.changes_path, self.oracle_path = (
            make_log_fixture(self.tmp.path, SCHEMA, SNAPSHOT, CHANGES, MOCK)
        )
        schema = load_schema(self.schema_path)
        schema_text = Path(self.schema_path).read_text(encoding="utf-8")
        db = Database.load(schema, self.snapshot_dir)
        log = load_changelog(self.changes_path, schema)
        self.validated = validate_log_inputs(schema, schema_text, db, log)
        self.naive = NaiveReplayer(self.schema_path, self.snapshot_dir, self.changes_path)
        self.workdir = self.tmp.path / "work"

    def tearDown(self):
        self.tmp.cleanup()

    def test_every_selection_matches_the_naive_replayer(self):
        n = len(CHANGES)
        naive_all = self.naive.enumerate_all()
        self.assertEqual(len(naive_all), 1 << n)
        for keep, (naive_committed, naive_state) in naive_all.items():
            result = self.validated.normalize(keep)
            self.assertEqual(
                result.committed, naive_committed, f"keep={sorted(keep)}"
            )
            self.assertEqual(
                state_rows(result.final_database()),
                naive_state_rows(naive_state),
                f"keep={sorted(keep)}",
            )

    def test_per_commit_states_match(self):
        for i, naive_state in enumerate(self.naive.per_commit_states()):
            result = self.validated.normalize(set(range(i + 1)))
            self.assertEqual(
                state_rows(result.final_database()),
                naive_state_rows(naive_state),
                f"prefix 0..{i}",
            )

    def test_certificate_neighbors_match_naive_computation(self):
        result = run_cli(
            "log-reduce",
            "--snapshot", str(self.snapshot_dir),
            "--schema", str(self.schema_path),
            "--changes", str(self.changes_path),
            "--oracle-config", str(self.oracle_path),
            "--workdir", str(self.workdir),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        cert = json.loads((self.workdir / "certificate.json").read_text())
        # The whole causal chain must stay; only the irrelevant t4 is removed.
        self.assertEqual(cert["result"]["kept_tx_ids"], ["t0", "t1", "t2", "t3"])

        verify = run_cli(
            "log-verify",
            "--workdir", str(self.workdir),
            "--oracle-config", str(self.oracle_path),
        )
        self.assertEqual(verify.returncode, 0, verify.stderr)
        report = json.loads((self.workdir / "log-verify-report.json").read_text())
        self.assertEqual(report["verdict"], "VERIFIED_MINIMAL")

        tx_ids = ["t0", "t1", "t2", "t3", "t4"]
        kept_positions = [0, 1, 2, 3]
        self.assertEqual(len(report["neighbors"]), 4)
        for neighbor in report["neighbors"]:
            pos = neighbor["tx_pos"]
            expected_committed, _state = self.naive.replay(
                set(kept_positions) - {pos}
            )
            self.assertEqual(
                neighbor["committed_tx_ids"],
                [tx_ids[p] for p in expected_committed],
                f"neighbor of tx at position {pos}",
            )
            self.assertEqual(neighbor["verdict"], "PASS")


if __name__ == "__main__":
    unittest.main()
