"""Cross-check the schema-evolving importer against a naive reference.

Every small combination of schema versions, commit points, renames, added
backfilled columns, int -> float widening, inserts/updates/deletes and window
cuts is restored both by the product importer and by the independent
:mod:`tests.naive_evolve` converter (which shares no product imaging code).
The two full-image transaction sequences must be identical.

Also checks the commit boundary is a *transaction* boundary: neither early
arrival of a later version's events nor a v1 transaction after the switch can
slip through.
"""

from __future__ import annotations

import itertools
import json
import random
import unittest
from pathlib import Path

from ddreduce.capture.errors import CaptureSchemaHistoryError
from ddreduce.capture.importer import run_import
from ddreduce.capture.verifier import VERDICT_MATCH, run_verify

from .capture_helpers import (
    commit,
    field,
    two_version_history,
    v2_change,
    write_v2_archive,
)
from .helpers import TempDir
from .naive_evolve import NaiveEvolvingReference

SCHEMA = {"tables": [{"name": "accounts", "columns": [
    {"name": "key", "type": "string", "nullable": False},
    {"name": "amount", "type": "float", "nullable": False},
    {"name": "currency", "type": "string", "nullable": False},
], "primary_key": ["key"]}]}

THREE_VERSION_HISTORY = {
    "format": "capture-schema-history-v1",
    "versions": [
        {"version": 1, "effective_commit_seq": 0, "tables": {
            "accounts": {"target_table": "accounts", "primary_key": ["f-key"],
                         "columns": [
                             field("f-key", "key", "string", False),
                             field("f-amount", "gross", "int", False)],
                         "unique_keys": [], "foreign_keys": []}}},
        {"version": 2, "effective_commit_seq": 2, "tables": {
            "accounts": {"target_table": "accounts", "primary_key": ["f-key"],
                         "columns": [
                             field("f-key", "key", "string", False),
                             field("f-amount", "amount", "float", False),
                             field("f-currency", "currency", "string", False,
                                   backfill="USD")],
                         "unique_keys": [], "foreign_keys": []}}},
        {"version": 3, "effective_commit_seq": 4, "tables": {
            "accounts": {"target_table": "accounts", "primary_key": ["f-key"],
                         "columns": [
                             field("f-key", "key", "string", False),
                             field("f-amount", "amount", "float", False),
                             field("f-currency", "currency", "string", False,
                                   backfill="USD"),
                             field("f-region", "region", "string", True)],
                         "unique_keys": [], "foreign_keys": []}}},
    ],
}

SCHEMA_WITH_REGION = {"tables": [{"name": "accounts", "columns": [
    {"name": "key", "type": "string", "nullable": False},
    {"name": "amount", "type": "float", "nullable": False},
    {"name": "currency", "type": "string", "nullable": False},
    {"name": "region", "type": "string", "nullable": True},
], "primary_key": ["key"]}]}


def _history_for(switch_at: int) -> dict:
    return two_version_history(switch_at=switch_at)


def build_history(
    *,
    n_ops: int,
    switch_at: int,
    seed: int,
    with_three_versions: bool = False,
) -> tuple[list[dict], dict, dict, dict[str, list[dict]]]:
    """Generate a small but non-trivial multi-version history.

    Rows start with v1 columns; every transaction after the switch writes v2
    images; with ``with_three_versions`` a v3 switches at commit 4 and adds a
    nullable ``region`` column (no backfill needed).
    """
    rng = random.Random(seed)
    if with_three_versions:
        history = json.loads(json.dumps(THREE_VERSION_HISTORY))
        schema = json.loads(json.dumps(SCHEMA_WITH_REGION))
    else:
        history = _history_for(switch_at)
        schema = json.loads(json.dumps(SCHEMA))
    rows = [{"key": f"k{i}", "gross": (i + 1) * 10} for i in range(3)]
    snapshot = {"accounts": [dict(r) for r in rows]}
    live = {r["key"]: {"key": r["key"], "amount": float(r["gross"])}
            for r in rows}
    messages: list[dict] = []
    offset = 0
    version_at = (lambda seq: 1 if seq < switch_at else 2)

    for i in range(n_ops):
        tx_id = f"tx{i:02d}"
        commit_seq = i + 1
        version = version_at(commit_seq)
        if with_three_versions and commit_seq >= 4:
            version = 3
        key = rng.choice(sorted(live))
        if version == 1:
            before = {"gross": int(live[key]["amount"])}
            after = {"gross": int(live[key]["amount"]) + rng.randrange(1, 5)}
            live[key]["amount"] = float(after["gross"])
        else:
            before = {"amount": float(live[key]["amount"])}
            after = {"amount": float(live[key]["amount"]) + rng.randrange(1, 5)}
            if version == 3 and rng.random() < 0.5:
                after["region"] = rng.choice(["eu", "us"])
                live[key]["region"] = after["region"]
            live[key]["amount"] = after["amount"]
        messages.append(v2_change(
            "s1", offset, tx_id, 1, "accounts", "update", {"key": key},
            before=before, after=after, schema_version=version,
        ))
        offset += 1
        messages.append(commit("s1", offset, tx_id, commit_seq, 1, {"accounts": 1}))
        offset += 1
    return messages, history, schema, snapshot


class EvolvingCrosscheckTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()

    def tearDown(self):
        self.tmp.cleanup()

    def _one(self, *, n_ops, switch_at, seed, with_three_versions=False,
             through=None):
        messages, history, schema, snapshot = build_history(
            n_ops=n_ops, switch_at=switch_at, seed=seed,
            with_three_versions=with_three_versions,
        )
        tag = f"c{n_ops}-{switch_at}-{seed}" + (
            "-three" if with_three_versions else ""
        ) + f"-t{through}"
        base = self.tmp.path / tag
        archive, snap, spath, hpath = write_v2_archive(
            base, messages, history=history, schema=schema,
            snapshot_rows=snapshot,
        )
        sealed = max(m["commit_seq"] for m in messages if m["kind"] == "commit")
        cut = sealed if through is None else through
        out = base / "pkg"
        run_import(archive, snap, spath, out, cut, history_path=hpath)

        reference = NaiveEvolvingReference(archive, history, schema, snapshot)
        reference.scan_and_assemble()
        expected = reference.restore(cut)
        actual = [json.loads(line) for line in
                  (out / "changes.jsonl").read_text("utf-8").splitlines()]
        self.assertEqual(
            json.dumps(actual, sort_keys=True),
            json.dumps(expected, sort_keys=True),
            f"seed {seed} switch {switch_at}: product differs from reference",
        )
        verify = run_verify(archive, snap, spath, cut, out, history_path=hpath)
        self.assertEqual(verify.verdict, VERDICT_MATCH, verify.failures)
        return actual, out

    def test_enumerated_switch_points_and_windows(self):
        for switch_at, n_ops, through in itertools.product(
            (2, 3), (3, 4), (None, 1, 2, 3)
        ):
            if through is not None and through > n_ops:
                continue
            with self.subTest(switch_at=switch_at, n_ops=n_ops,
                              through=through):
                self._one(n_ops=n_ops, switch_at=switch_at, seed=11,
                          through=through)

    def test_three_versions_with_nullable_addition(self):
        actual, _out = self._one(
            n_ops=6, switch_at=2, seed=5, with_three_versions=True,
        )
        # Every row image carries the region column (null where absent).
        for tx in actual:
            for ev in tx["events"]:
                self.assertIn("region", ev["after"])
                self.assertIn("amount", ev["after"])
                self.assertIn("currency", ev["after"])
                self.assertNotIn("gross", ev["after"])

    def test_every_old_row_keeps_its_backfill(self):
        actual, _out = self._one(n_ops=5, switch_at=3, seed=3)
        for tx in actual:
            for ev in tx["events"]:
                self.assertEqual(ev["before"]["currency"], "USD")
                self.assertEqual(ev["after"]["currency"], "USD")

    def test_widening_preserves_values_exactly(self):
        actual, _out = self._one(n_ops=4, switch_at=2, seed=9)
        for tx in actual:
            for ev in tx["events"]:
                for image in ("before", "after"):
                    if image in ev:
                        value = ev[image]["amount"]
                        self.assertIsInstance(value, float)
                        self.assertEqual(value, float(int(value)))


class CommitBoundaryTest(unittest.TestCase):
    """Schema switches are transaction commit boundaries."""

    def setUp(self):
        self.tmp = TempDir()

    def tearDown(self):
        self.tmp.cleanup()

    def _archive(self, messages, switch_at=2):
        base = self.tmp.path / "boundary"
        archive, snap, spath, hpath = write_v2_archive(
            base, messages, history=_history_for(switch_at), schema=SCHEMA,
            snapshot_rows={"accounts": [{"key": "k", "gross": 10}]},
            sealed_commit_seq=max(
                m["commit_seq"] for m in messages if m["kind"] == "commit"
            ),
        )
        return archive, snap, spath, hpath, base

    def test_v2_events_arriving_early_are_not_applied_before_the_switch(self):
        # Transport order has T2 (v2, commit 2) before T1 (v1, commit 1); the
        # importer must still apply T1 first, and T1's v1 image must land in
        # the target column.  A version applied on arrival would reject T2's
        # before image or lose the backfill.
        messages = [
            v2_change("s1", 0, "T2", 1, "accounts", "update", {"key": "k"},
                      before={"amount": 20.0}, after={"amount": 30.0},
                      schema_version=2),
            commit("s1", 1, "T2", 2, 1, {"accounts": 1}),
            v2_change("s1", 2, "T1", 1, "accounts", "update", {"key": "k"},
                      before={"gross": 10}, after={"gross": 20},
                      schema_version=1),
            commit("s1", 3, "T1", 1, 1, {"accounts": 1}),
        ]
        archive, snap, spath, hpath, base = self._archive(messages)
        out = base / "pkg"
        run_import(archive, snap, spath, out, 2, history_path=hpath)
        rows = [json.loads(line) for line in
                (out / "changes.jsonl").read_text().splitlines()]
        self.assertEqual([r["tx_id"] for r in rows], ["T1", "T2"])
        self.assertEqual(rows[0]["events"][0]["before"],
                         {"key": "k", "amount": 10.0, "currency": "USD"})

    def test_v1_transaction_after_switch_is_refused(self):
        messages = [
            v2_change("s1", 0, "T1", 1, "accounts", "update", {"key": "k"},
                      before={"gross": 10}, after={"gross": 11},
                      schema_version=1),
            commit("s1", 1, "T1", 1, 1, {"accounts": 1}),
            v2_change("s1", 2, "T2", 1, "accounts", "update", {"key": "k"},
                      before={"gross": 11}, after={"gross": 12},
                      schema_version=1),
            commit("s1", 3, "T2", 2, 1, {"accounts": 1}),
        ]
        archive, snap, spath, hpath, base = self._archive(messages)
        with self.assertRaises(CaptureSchemaHistoryError):
            run_import(archive, snap, spath, base / "pkg", 2,
                       history_path=hpath)

    def test_transaction_with_events_of_two_versions_is_refused(self):
        messages = [
            v2_change("s1", 0, "T", 1, "accounts", "update", {"key": "k"},
                      before={"gross": 10}, after={"gross": 11},
                      schema_version=1),
            v2_change("s1", 1, "T", 2, "accounts", "update", {"key": "k"},
                      before={"amount": 11.0}, after={"amount": 12.0},
                      schema_version=2),
            commit("s1", 2, "T", 2, 2, {"accounts": 2}),
        ]
        archive, snap, spath, hpath, base = self._archive(messages)
        from ddreduce.capture.errors import CaptureArchiveError

        with self.assertRaises(CaptureArchiveError):
            run_import(archive, snap, spath, base / "pkg", 2,
                       history_path=hpath)


if __name__ == "__main__":
    unittest.main()
