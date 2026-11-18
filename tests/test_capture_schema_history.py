"""capture-v2: ordered schema history, versioned interpretation and verification.

The committed hand-computed scenario (see :mod:`demo.capture.gen_evolving`):

* version 1: ``gross`` is an ``int`` field with stable field ID ``f-amount``;
* version 2 (effective at commit_seq 2) renames the same field ID to
  ``amount``, widens it to ``float`` and adds ``currency`` with the explicit
  constant backfill ``"USD"``;
* one commit before the switch and one after each update the row.

Both events must land in the target column ``amount`` and keep ``currency``.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from ddreduce.capture.errors import (
    CaptureArchiveError,
    CaptureSchemaHistoryError,
)
from ddreduce.capture.history import load_schema_history
from ddreduce.capture.importer import PACKAGE_FILES, STAGING_PREFIX, run_import
from ddreduce.capture.verifier import (
    VERDICT_ARCHIVE,
    VERDICT_MATCH,
    VERDICT_PACKAGE,
    run_verify,
)
from ddreduce.capture.store import CaptureStore
from ddreduce.capture.manifest import load_manifest
from ddreduce.logmodel import load_changelog
from ddreduce.model import Database
from ddreduce.replay import validate_log_inputs
from ddreduce.schema import load_schema

from .capture_helpers import (
    change,
    commit,
    field,
    history_hash,
    target_accounts_schema,
    two_version_history,
    v2_change,
    write_v2_archive,
)
from .helpers import TempDir, write_json

SCHEMA = target_accounts_schema()
HISTORY = two_version_history()

SNAPSHOT = {"accounts": [{"key": "k", "gross": 10}]}


def hand_messages() -> list[dict]:
    """T1 (commit 1, v1) amount 10 -> 20; T2 (commit 2, v2) amount 20 -> 30."""
    return [
        v2_change("s1", 0, "T1", 1, "accounts", "update", {"key": "k"},
                  before={"gross": 10}, after={"gross": 20}, schema_version=1),
        commit("s1", 1, "T1", 1, 1, {"accounts": 1}),
        v2_change("s1", 2, "T2", 1, "accounts", "update", {"key": "k"},
                  before={"amount": 20.0}, after={"amount": 30.0},
                  schema_version=2),
        commit("s1", 3, "T2", 2, 1, {"accounts": 1}),
    ]


class HandComputedEvolutionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        self.base = self.tmp.path / "hand"

    def tearDown(self):
        self.tmp.cleanup()

    def _build(self, messages=None, **kwargs):
        return write_v2_archive(
            self.base,
            messages if messages is not None else hand_messages(),
            history=kwargs.pop("history", HISTORY),
            schema=kwargs.pop("schema", SCHEMA),
            snapshot_rows=kwargs.pop("snapshot", SNAPSHOT),
            **kwargs,
        )

    def test_both_events_share_the_renamed_target_column(self):
        archive, snap, spath, hpath = self._build()
        result = run_import(archive, snap, spath, self.base / "pkg", 2,
                            history_path=hpath)
        lines = [json.loads(line) for line in
                 (self.base / "pkg" / "changes.jsonl").read_text().splitlines()]
        self.assertEqual([tx["tx_id"] for tx in lines], ["T1", "T2"])

        # The pre-rename event is re-expressed in the unified schema: the old
        # name "gross" never leaks into the package and the old int stays exact.
        first = lines[0]["events"][0]
        self.assertEqual(first["key"], {"key": "k"})
        self.assertEqual(first["before"],
                         {"key": "k", "amount": 10.0, "currency": "USD"})
        self.assertEqual(first["after"],
                         {"key": "k", "amount": 20.0, "currency": "USD"})
        # A widening must not change the representable value.
        self.assertIsInstance(first["after"]["amount"], float)
        self.assertEqual(first["before"]["amount"], 10.0)

        second = lines[1]["events"][0]
        self.assertEqual(second["before"],
                         {"key": "k", "amount": 20.0, "currency": "USD"})
        self.assertEqual(second["after"],
                         {"key": "k", "amount": 30.0, "currency": "USD"})

        # The converted snapshot also carries the backfill.
        snapshot_rows = [
            json.loads(line) for line in
            (self.base / "pkg" / "snapshot" / "accounts.jsonl")
            .read_text().splitlines()
        ]
        self.assertEqual(snapshot_rows,
                         [{"key": "k", "amount": 10.0, "currency": "USD"}])

        # Provenance names the declared version of every source event.
        map_rows = [json.loads(line) for line in
                    (self.base / "pkg" / "capture-map.jsonl")
                    .read_text().splitlines()]
        self.assertEqual([r["source"]["schema_version"] for r in map_rows],
                         [1, 2])

        report = result.report
        self.assertEqual(report["schema_history"]["history_hash"],
                         history_hash(HISTORY))
        self.assertEqual(report["schema_history"]["switch_points"],
                         [{"version": 2, "effective_commit_seq": 2}])
        self.assertEqual(report["counts"]["commits_version_checked"], 2)
        self.assertEqual(report["target_schema"]["schema_hash"],
                         report["inputs"]["schema_hash"])

    def test_package_replays_through_existing_log_tools(self):
        archive, snap, spath, hpath = self._build()
        out = self.base / "pkg"
        run_import(archive, snap, spath, out, 2, history_path=hpath)
        schema = load_schema(out / "schema.json")
        db = Database.load(schema, out / "snapshot")
        log = load_changelog(out / "changes.jsonl", schema)
        validated = validate_log_inputs(
            schema, (out / "schema.json").read_text("utf-8"), db, log
        )
        final = validated.normalize({0, 1}).final_database()
        self.assertEqual(final.rows['accounts/str:"k"'].data,
                         {"key": "k", "amount": 30.0, "currency": "USD"})

    def test_independent_verification_matches_hand_package(self):
        archive, snap, spath, hpath = self._build()
        out = self.base / "pkg"
        run_import(archive, snap, spath, out, 2, history_path=hpath)
        verify = run_verify(archive, snap, spath, 2, out, history_path=hpath)
        self.assertEqual(verify.verdict, VERDICT_MATCH, verify.failures)
        self.assertEqual(verify.failures, [])

    def test_window_truncation_keeps_archive_checks(self):
        archive, snap, spath, hpath = self._build()
        out = self.base / "pkg1"
        result = run_import(archive, snap, spath, out, 1, history_path=hpath)
        lines = [json.loads(line) for line in
                 (out / "changes.jsonl").read_text().splitlines()]
        self.assertEqual([tx["tx_id"] for tx in lines], ["T1"])
        self.assertEqual(result.report["counts"]["out_of_window_commits"], 1)

    def test_new_output_dir_is_never_overwritten(self):
        archive, snap, spath, hpath = self._build()
        out = self.base / "pkg"
        run_import(archive, snap, spath, out, 2, history_path=hpath)
        with self.assertRaises(CaptureError):
            run_import(archive, snap, spath, out, 2, history_path=hpath)


from ddreduce.capture.errors import CaptureError  # noqa: E402


class SameNameDifferentFieldTest(unittest.TestCase):
    """Reusing a column *name* for a different field ID never merges them.

    Version 1 has one numeric field ``amount`` (ID ``f-legacy``).  Version 2
    renames that field to ``amount_legacy`` and introduces a brand-new field
    ``amount`` (ID ``f-amount``, backfilled with ``0``) that happens to reuse
    the old name.  A converter that matched by name would pour the old int
    values into the new field's target column.  Field IDs keep them apart:
    the old values go to ``amount_legacy`` and the new field is backfilled.
    """

    def setUp(self):
        self.tmp = TempDir()
        self.base = self.tmp.path / "name-reuse"

    def tearDown(self):
        self.tmp.cleanup()

    def _history(self) -> dict:
        return {
            "format": "capture-schema-history-v1",
            "versions": [
                {"version": 1, "effective_commit_seq": 0, "tables": {
                    "t": {"target_table": "t", "primary_key": ["f-k"],
                          "columns": [
                              field("f-k", "k", "string", False),
                              field("f-legacy", "amount", "int", False)],
                          "unique_keys": [], "foreign_keys": []}}},
                {"version": 2, "effective_commit_seq": 2, "tables": {
                    "t": {"target_table": "t", "primary_key": ["f-k"],
                          "columns": [
                              field("f-k", "k", "string", False),
                              field("f-legacy", "amount_legacy", "int", False),
                              field("f-amount", "amount", "float", False,
                                    backfill=0)],
                          "unique_keys": [], "foreign_keys": []}}},
            ],
        }

    def _schema(self) -> dict:
        return {"tables": [{"name": "t", "columns": [
            {"name": "k", "type": "string", "nullable": False},
            {"name": "amount_legacy", "type": "int", "nullable": False},
            {"name": "amount", "type": "float", "nullable": False},
        ], "primary_key": ["k"]}]}

    def test_same_name_different_id_is_not_merged(self):
        msgs = [
            v2_change("s1", 0, "A", 1, "t", "update", {"k": "x"},
                      before={"amount": 10}, after={"amount": 20},
                      schema_version=1),
            commit("s1", 1, "A", 1, 1, {"t": 1}),
        ]
        archive, snap, spath, hpath = write_v2_archive(
            self.base, msgs, history=self._history(), schema=self._schema(),
            snapshot_rows={"t": [{"k": "x", "amount": 10}]},
            sealed_commit_seq=2,
        )
        out = self.base / "pkg"
        run_import(archive, snap, spath, out, 2, history_path=hpath)
        snapshot_rows = [json.loads(line) for line in
                         (out / "snapshot" / "t.jsonl").read_text().splitlines()]
        self.assertEqual(snapshot_rows, [
            {"k": "x", "amount_legacy": 10, "amount": 0.0},
        ])
        events = json.loads(
            (out / "changes.jsonl").read_text().splitlines()[0]
        )["events"][0]
        self.assertEqual(events["before"],
                         {"k": "x", "amount_legacy": 10, "amount": 0.0})
        self.assertEqual(events["after"],
                         {"k": "x", "amount_legacy": 20, "amount": 0.0})
        # The new field was never written by the old field's values.
        self.assertNotIn(10.0, [events["after"]["amount"]])

        verify = run_verify(archive, snap, spath, 2, out, history_path=hpath)
        self.assertEqual(verify.verdict, VERDICT_MATCH, verify.failures)

    def test_replacing_a_field_id_is_refused(self):
        # Dropping f-legacy entirely and letting f-amount take over the old
        # name (no rename) is a field-ID replacement, not a rename: rejected.
        history = self._history()
        history["versions"][1]["tables"]["t"]["columns"] = [
            field("f-k", "k", "string", False),
            field("f-amount", "amount", "float", False, backfill=0),
        ]
        path = write_json(self.tmp.path / "bad-history.json", history)
        with self.assertRaises(CaptureSchemaHistoryError) as ctx:
            load_schema_history(path)
        self.assertIn("deletes field IDs", str(ctx.exception))

    def test_target_schema_with_swapped_types_is_refused(self):
        archive, snap, spath, hpath = write_v2_archive(
            self.base / "ok", [
                v2_change("s1", 0, "A", 1, "t", "insert", {"k": "x"},
                          after={"k": "x", "amount": 5}, schema_version=1),
                commit("s1", 1, "A", 1, 1, {"t": 1}),
            ],
            history=self._history(), schema=self._schema(),
            snapshot_rows={"t": []}, sealed_commit_seq=2,
        )
        schema = self._schema()
        schema["tables"][0]["columns"][1]["type"] = "float"
        bad = write_json(self.base / "bad-schema.json", schema)
        with self.assertRaises(CaptureSchemaHistoryError) as ctx:
            run_import(archive, snap, bad, self.base / "pkg", 2,
                       history_path=hpath)
        self.assertIn("does not match", str(ctx.exception))


class EvolutionRuleTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()

    def tearDown(self):
        self.tmp.cleanup()

    def _history(self, mutate):
        history = two_version_history()
        mutate(history)
        path = write_json(self.tmp.path / "h.json", history)
        return path

    def _load(self, mutate):
        path = self._history(mutate)
        return load_schema_history(path)

    def test_narrowing_type_rejected(self):
        def mutate(h):
            h["versions"][1]["tables"]["accounts"]["columns"][1]["type"] = "int"
            h["versions"][0]["tables"]["accounts"]["columns"][1]["type"] = "float"
        with self.assertRaises(CaptureSchemaHistoryError) as ctx:
            self._load(mutate)
        self.assertIn("only int -> float widening", str(ctx.exception))

    def test_field_id_replacement_rejected(self):
        def mutate(h):
            h["versions"][1]["tables"]["accounts"]["columns"][1]["field_id"] = "f-other"
        with self.assertRaises(CaptureSchemaHistoryError) as ctx:
            self._load(mutate)
        self.assertIn("deletes field IDs", str(ctx.exception))

    def test_nullability_change_rejected(self):
        def mutate(h):
            h["versions"][1]["tables"]["accounts"]["columns"][1]["nullable"] = True
        with self.assertRaises(CaptureSchemaHistoryError) as ctx:
            self._load(mutate)
        self.assertIn("nullability", str(ctx.exception))

    def test_new_non_null_column_without_backfill_rejected(self):
        def mutate(h):
            del h["versions"][1]["tables"]["accounts"]["columns"][2]["backfill"]
        with self.assertRaises(CaptureSchemaHistoryError) as ctx:
            self._load(mutate)
        self.assertIn("has no explicit backfill", str(ctx.exception))

    def test_new_nullable_column_without_backfill_is_allowed(self):
        def mutate(h):
            col = h["versions"][1]["tables"]["accounts"]["columns"][2]
            del col["backfill"]
            col["nullable"] = True
        self._load(mutate)

    def test_duplicate_version_number_rejected(self):
        def mutate(h):
            h["versions"][1]["version"] = 1
        with self.assertRaises(CaptureSchemaHistoryError) as ctx:
            self._load(mutate)
        self.assertIn("duplicate schema version number", str(ctx.exception))

    def test_commit_points_must_strictly_increase(self):
        def mutate(h):
            h["versions"][1]["effective_commit_seq"] = 0
        with self.assertRaises(CaptureSchemaHistoryError) as ctx:
            self._load(mutate)
        self.assertIn("commit point conflict", str(ctx.exception))

    def test_first_version_must_start_at_zero(self):
        def mutate(h):
            h["versions"][0]["effective_commit_seq"] = 3
            h["versions"][1]["effective_commit_seq"] = 4
        with self.assertRaises(CaptureSchemaHistoryError) as ctx:
            self._load(mutate)
        self.assertIn("must be effective at commit_seq 0", str(ctx.exception))

    def test_primary_key_change_rejected(self):
        def mutate(h):
            h["versions"][1]["tables"]["accounts"]["primary_key"] = ["f-amount"]
        with self.assertRaises(CaptureSchemaHistoryError) as ctx:
            self._load(mutate)
        self.assertIn("primary key", str(ctx.exception))

    def test_table_removal_rejected(self):
        def mutate(h):
            h["versions"][1]["tables"] = {}
        with self.assertRaises(CaptureSchemaHistoryError):
            self._load(mutate)

    def test_unknown_field_in_constraint_rejected(self):
        def mutate(h):
            h["versions"][1]["tables"]["accounts"]["unique_keys"] = [["f-nope"]]
        with self.assertRaises(CaptureSchemaHistoryError):
            self._load(mutate)


class ArchiveBindingTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        self.base = self.tmp.path / "bind"

    def tearDown(self):
        self.tmp.cleanup()

    def _build(self, **kwargs):
        return write_v2_archive(
            self.base, kwargs.pop("messages", None) or hand_messages(),
            history=HISTORY, schema=SCHEMA, snapshot_rows=SNAPSHOT, **kwargs,
        )

    def test_missing_schema_version_rejected(self):
        msgs = hand_messages()
        del msgs[0]["schema_version"]
        archive, snap, spath, hpath = self._build(messages=msgs)
        with self.assertRaises(CaptureArchiveError) as ctx:
            run_import(archive, snap, spath, self.base / "pkg", 2,
                       history_path=hpath)
        self.assertIn("missing schema_version", str(ctx.exception))

    def test_unknown_schema_version_rejected(self):
        msgs = hand_messages()
        msgs[0]["schema_version"] = 7
        archive, snap, spath, hpath = self._build(messages=msgs)
        with self.assertRaises(CaptureArchiveError) as ctx:
            run_import(archive, snap, spath, self.base / "pkg", 2,
                       history_path=hpath)
        self.assertIn("schema_version 7 is not declared", str(ctx.exception))
        self.assertIn("s1@0", str(ctx.exception))

    def test_transaction_straddling_switch_rejected(self):
        # One transaction with one v1 and one v2 event.
        msgs = [
            v2_change("s1", 0, "T", 1, "accounts", "update", {"key": "k"},
                      before={"gross": 10}, after={"gross": 20},
                      schema_version=1),
            v2_change("s1", 1, "T", 2, "accounts", "update", {"key": "k"},
                      before={"amount": 20.0}, after={"amount": 25.0},
                      schema_version=2),
            commit("s1", 2, "T", 2, 2, {"accounts": 2}),
        ]
        archive, snap, spath, hpath = self._build(messages=msgs)
        with self.assertRaises(CaptureArchiveError) as ctx:
            run_import(archive, snap, spath, self.base / "pkg", 2,
                       history_path=hpath)
        self.assertIn("cannot straddle a schema switch", str(ctx.exception))

    def test_event_version_disagreeing_with_commit_point_rejected(self):
        # Unique commit points, but T2 is written as v1 while committing after
        # the v2 switch: the message version must match the commit interval.
        msgs = [
            v2_change("s1", 0, "T1", 1, "accounts", "update", {"key": "k"},
                      before={"gross": 10}, after={"gross": 20},
                      schema_version=1),
            commit("s1", 1, "T1", 1, 1, {"accounts": 1}),
            v2_change("s1", 2, "T2", 1, "accounts", "update", {"key": "k"},
                      before={"gross": 20}, after={"gross": 30},
                      schema_version=1),
            commit("s1", 3, "T2", 2, 1, {"accounts": 1}),
        ]
        archive, snap, spath, hpath = self._build(
            messages=msgs, sealed_commit_seq=2,
        )
        with self.assertRaises(CaptureSchemaHistoryError) as ctx:
            run_import(archive, snap, spath, self.base / "pkg", 2,
                       history_path=hpath)
        self.assertIn("version in force", str(ctx.exception))
        self.assertIn("T2", str(ctx.exception))

    def test_early_switch_cannot_apply_before_commit(self):
        # T1 (written at v1) commits *after* the v2 switch point: refused even
        # though its images are structurally readable, because the version in
        # force at its commit is 2.  The switch is a commit boundary, never a
        # hint applied to events that arrive out of transport order.
        msgs = [
            v2_change("s1", 0, "T1", 1, "accounts", "update", {"key": "k"},
                      before={"gross": 10}, after={"gross": 11},
                      schema_version=1),
            commit("s1", 1, "T1", 3, 1, {"accounts": 1}),
            v2_change("s1", 2, "T2", 1, "accounts", "update", {"key": "k"},
                      before={"amount": 11.0}, after={"amount": 12.0},
                      schema_version=2),
            commit("s1", 3, "T2", 4, 1, {"accounts": 1}),
        ]
        archive, snap, spath, hpath = self._build(
            messages=msgs, sealed_commit_seq=4,
        )
        with self.assertRaises(CaptureSchemaHistoryError) as ctx:
            run_import(archive, snap, spath, self.base / "pkg", 4,
                       history_path=hpath)
        self.assertIn("version in force", str(ctx.exception))
        self.assertIn("commit boundary", str(ctx.exception))

    def test_image_naming_a_column_of_another_version_is_rejected(self):
        # T1 is declared v1 (only key/gross exist) but its after image names
        # the v2 column "currency": the archive and its history disagree.
        msgs = hand_messages()
        msgs[0]["after"] = {"gross": 20, "currency": "USD"}
        archive, snap, spath, hpath = self._build(messages=msgs)
        with self.assertRaises(CaptureArchiveError) as ctx:
            run_import(archive, snap, spath, self.base / "pkg", 2,
                       history_path=hpath)
        self.assertIn("does not define", str(ctx.exception))
        self.assertEqual(ctx.exception.locator.get("stream"), "s1")
        self.assertEqual(ctx.exception.locator.get("offset"), 0)

    def test_history_hash_mismatch_rejected(self):
        archive, snap, spath, hpath = self._build(
            history_hash_override="0" * 64,
        )
        with self.assertRaises(CaptureArchiveError) as ctx:
            run_import(archive, snap, spath, self.base / "pkg", 2,
                       history_path=hpath)
        self.assertIn("history_hash", str(ctx.exception))

    def test_different_supplied_history_rejected(self):
        archive, snap, spath, hpath = self._build()
        other = two_version_history(switch_at=3)
        other_path = write_json(self.tmp.path / "other-history.json", other)
        with self.assertRaises(CaptureSchemaHistoryError) as ctx:
            run_import(archive, snap, spath, self.base / "pkg", 2,
                       history_path=other_path)
        self.assertIn("does not match", str(ctx.exception))

    def test_missing_history_argument_rejected(self):
        archive, snap, spath, _hpath = self._build()
        with self.assertRaises(CaptureSchemaHistoryError) as ctx:
            run_import(archive, snap, spath, self.base / "pkg", 2)
        self.assertIn("--schema-history", str(ctx.exception))

    def test_history_on_capture_v1_rejected(self):
        base = self.tmp.path / "v1"
        from .capture_helpers import write_archive

        archive, snap, spath = write_archive(
            base,
            [change("s1", 0, "a", 1, "T", "insert", {"id": "q"},
                    after={"id": "q", "v": 1, "tag": None}),
             commit("s1", 1, "a", 1, 1, {"T": 1})],
            schema={"tables": [{"name": "T", "columns": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "v", "type": "int", "nullable": False},
                {"name": "tag", "type": "string", "nullable": True},
            ], "primary_key": ["id"]}]},
            snapshot={"T": []},
        )
        with self.assertRaises(CaptureSchemaHistoryError):
            run_import(archive, snap, spath, base / "pkg", 1,
                       history_path=write_json(base / "h.json", HISTORY))

    def test_switching_definition_not_applied_early(self):
        # v1 event after the switch point is refused even if its before image
        # would still match; the switch is a commit boundary, not a hint.
        msgs = [
            v2_change("s1", 0, "A", 1, "accounts", "update", {"key": "k"},
                      before={"gross": 10}, after={"gross": 11},
                      schema_version=1),
            commit("s1", 1, "A", 1, 1, {"accounts": 1}),
            v2_change("s1", 2, "B", 1, "accounts", "update", {"key": "k"},
                      before={"amount": 11.0}, after={"amount": 12.0},
                      schema_version=2),
            commit("s1", 3, "B", 2, 1, {"accounts": 1}),
            v2_change("s1", 4, "C", 1, "accounts", "update", {"key": "k"},
                      before={"gross": 12}, after={"gross": 13},
                      schema_version=1),
            commit("s1", 5, "C", 3, 1, {"accounts": 1}),
        ]
        archive, snap, spath, hpath = self._build(
            messages=msgs, sealed_commit_seq=3,
        )
        with self.assertRaises(CaptureSchemaHistoryError):
            run_import(archive, snap, spath, self.base / "pkg", 3,
                       history_path=hpath)

    def test_failure_leaves_no_staging_or_output(self):
        msgs = hand_messages()
        del msgs[0]["schema_version"]
        archive, snap, spath, hpath = self._build(messages=msgs)
        with self.assertRaises(CaptureArchiveError):
            run_import(archive, snap, spath, self.base / "pkg", 2,
                       history_path=hpath)
        self.assertFalse((self.base / "pkg").exists())
        leftovers = [p for p in self.base.iterdir()
                     if p.name.startswith(STAGING_PREFIX)]
        self.assertEqual(leftovers, [])


class VerifierTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        self.base = self.tmp.path / "verify"
        self.archive, self.snap, self.spath, self.hpath = write_v2_archive(
            self.base, hand_messages(), history=HISTORY, schema=SCHEMA,
            snapshot_rows=SNAPSHOT,
        )
        self.out = self.base / "pkg"
        run_import(self.archive, self.snap, self.spath, self.out, 2,
                   history_path=self.hpath)

    def tearDown(self):
        self.tmp.cleanup()

    def _verify(self, package=None):
        return run_verify(self.archive, self.snap, self.spath, 2,
                          package or self.out, history_path=self.hpath)

    def _rewrite_changes(self, mutate):
        path = self.out / "changes.jsonl"
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        mutate(rows)
        path.write_text("\n".join(json.dumps(r, sort_keys=True) for r in rows)
                        + "\n", encoding="utf-8")

    def test_clean_package_matches(self):
        result = self._verify()
        self.assertEqual(result.verdict, VERDICT_MATCH, result.failures)

    def test_old_column_written_to_same_named_target_column_is_caught(self):
        # A name-based converter would put the *old* field into a same-named
        # column with a different field ID.  Simulate a package that does so:
        # "gross" survives in the converted row.
        def mutate(rows):
            rows[0]["events"][0]["before"]["gross"] = 10
            rows[0]["events"][0]["after"]["gross"] = 20
        self._rewrite_changes(mutate)
        result = self._verify()
        self.assertEqual(result.verdict, VERDICT_PACKAGE)
        self.assertTrue(any("changes.jsonl" in f["file"] for f in result.failures))

    def test_old_field_poured_into_same_named_target_column_is_caught(self):
        # A name-based converter sees the v1 column "gross" and writes it into
        # the *target* column called "gross" - which belongs to a different
        # field ID (a brand-new field) - instead of into "amount".  Build
        # exactly that package and require the verifier to reject it.
        history = {
            "format": "capture-schema-history-v1",
            "versions": [
                {"version": 1, "effective_commit_seq": 0, "tables": {
                    "accounts": {"target_table": "accounts",
                                 "primary_key": ["f-key"],
                                 "columns": [
                                     field("f-key", "key", "string", False),
                                     field("f-amount", "gross", "int", False)],
                                 "unique_keys": [], "foreign_keys": []}}},
                {"version": 2, "effective_commit_seq": 2, "tables": {
                    "accounts": {"target_table": "accounts",
                                 "primary_key": ["f-key"],
                                 "columns": [
                                     field("f-key", "key", "string", False),
                                     field("f-amount", "amount", "float", False),
                                     field("f-gross", "gross", "int", False,
                                           backfill=0)],
                                 "unique_keys": [], "foreign_keys": []}}},
            ],
        }
        schema = {"tables": [{"name": "accounts", "columns": [
            {"name": "key", "type": "string", "nullable": False},
            {"name": "amount", "type": "float", "nullable": False},
            {"name": "gross", "type": "int", "nullable": False},
        ], "primary_key": ["key"]}]}
        msgs = [
            v2_change("s1", 0, "T1", 1, "accounts", "update", {"key": "k"},
                      before={"gross": 10}, after={"gross": 20},
                      schema_version=1),
            commit("s1", 1, "T1", 1, 1, {"accounts": 1}),
            v2_change("s1", 2, "T2", 1, "accounts", "update", {"key": "k"},
                      before={"amount": 20.0}, after={"amount": 30.0},
                      schema_version=2),
            commit("s1", 3, "T2", 2, 1, {"accounts": 1}),
        ]
        base = self.tmp.path / "same-name-target"
        archive, snap, spath, hpath = write_v2_archive(
            base, msgs, history=history, schema=schema,
            snapshot_rows={"accounts": [{"key": "k", "gross": 10}]},
        )
        out = base / "pkg"
        run_import(archive, snap, spath, out, 2, history_path=hpath)
        self.assertEqual(
            run_verify(archive, snap, spath, 2, out, history_path=hpath).verdict,
            VERDICT_MATCH,
        )
        # Wrong package: the old field's value lands in the target "gross"
        # column (name match) and "amount" is missing/zero.
        changes = out / "changes.jsonl"
        rows = [json.loads(line) for line in changes.read_text().splitlines()]
        ev = rows[0]["events"][0]
        ev["before"].update({"gross": 10, "amount": 10.0})
        ev["after"].update({"gross": 20, "amount": 20.0})
        changes.write_text(
            "\n".join(json.dumps(r, sort_keys=True) for r in rows) + "\n",
            encoding="utf-8",
        )
        result = run_verify(archive, snap, spath, 2, out, history_path=hpath)
        self.assertEqual(result.verdict, VERDICT_PACKAGE)

    def test_backfill_replaced_by_null_is_caught(self):
        def mutate(rows):
            for tx in rows:
                for ev in tx["events"]:
                    for image in ("before", "after"):
                        if image in ev:
                            ev[image]["currency"] = None
        self._rewrite_changes(mutate)
        result = self._verify()
        self.assertEqual(result.verdict, VERDICT_PACKAGE)

    def test_events_converted_with_the_wrong_version_are_caught(self):
        def mutate(rows):
            # T2's after image mistakenly widened from the v1 image: 30.0
            # becomes the *old* gross column semantics, i.e. the before state
            # of the switch is reused.
            rows[1]["events"][0]["before"]["amount"] = 10.0
        self._rewrite_changes(mutate)
        result = self._verify()
        self.assertEqual(result.verdict, VERDICT_PACKAGE)

    def test_map_claiming_wrong_schema_version_is_caught(self):
        path = self.out / "capture-map.jsonl"
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        rows[0]["source"]["schema_version"] = 2
        path.write_text("\n".join(json.dumps(r, sort_keys=True) for r in rows)
                        + "\n", encoding="utf-8")
        result = self._verify()
        self.assertEqual(result.verdict, VERDICT_PACKAGE)
        self.assertTrue(any("schema version" in f["detail"]
                            for f in result.failures))

    def test_report_history_hash_tamper_is_caught(self):
        path = self.out / "capture-report.json"
        report = json.loads(path.read_text())
        report["schema_history"]["history_hash"] = "0" * 64
        path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        result = self._verify()
        self.assertEqual(result.verdict, VERDICT_PACKAGE)
        self.assertTrue(any(f.get("section") == "schema_history"
                            for f in result.failures))

    def test_report_switch_points_tamper_is_caught(self):
        path = self.out / "capture-report.json"
        report = json.loads(path.read_text())
        report["schema_history"]["switch_points"][0]["effective_commit_seq"] = 1
        path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        result = self._verify()
        self.assertEqual(result.verdict, VERDICT_PACKAGE)
        self.assertTrue(any(f.get("section") == "schema_history"
                            for f in result.failures))

    def test_package_snapshot_without_backfill_is_caught(self):
        path = self.out / "snapshot" / "accounts.jsonl"
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        rows[0]["currency"] = None
        path.write_text("\n".join(json.dumps(r, sort_keys=True) for r in rows)
                        + "\n", encoding="utf-8")
        result = self._verify()
        self.assertEqual(result.verdict, VERDICT_PACKAGE)
        self.assertTrue(any("snapshot" in f["file"] for f in result.failures))

    def test_different_history_supplied_to_verifier_is_archive_invalid(self):
        other = write_json(self.tmp.path / "other.json",
                           two_version_history(switch_at=3))
        result = run_verify(self.archive, self.snap, self.spath, 2,
                            self.out, history_path=other)
        self.assertEqual(result.verdict, VERDICT_ARCHIVE)

    def test_illegal_archive_under_a_different_history_is_archive_invalid(self):
        # Corrupt the archive itself: event version past the switch.
        archive2 = self.base / "arch2"
        import shutil

        shutil.copytree(self.archive, archive2)
        shard = archive2 / "shards" / "s1.jsonl"
        rows = [json.loads(line) for line in shard.read_text().splitlines()]
        for row in rows:
            if row.get("kind") == "change" and row.get("tx_id") == "T1":
                row["schema_version"] = 2
        shard.write_text("\n".join(json.dumps(r, sort_keys=True) for r in rows)
                         + "\n", encoding="utf-8")
        result = run_verify(archive2, self.snap, self.spath, 2, self.out,
                            history_path=self.hpath)
        self.assertEqual(result.verdict, VERDICT_ARCHIVE)
        self.assertTrue(any("schema version" in f["detail"]
                            for f in result.failures))

    def test_extra_package_file_is_caught(self):
        (self.out / "rogue.txt").write_text("x\n", encoding="utf-8")
        result = self._verify()
        self.assertEqual(result.verdict, VERDICT_PACKAGE)


if __name__ == "__main__":
    unittest.main()


class MissingVersusNullTest(unittest.TestCase):
    """A missing column preserves; an explicit null writes null - in v2 too."""

    def setUp(self):
        self.tmp = TempDir()
        self.base = self.tmp.path / "nulls"

    def tearDown(self):
        self.tmp.cleanup()

    def test_omitted_later_column_preserves_the_backfill(self):
        # A v2 update that never mentions "currency" must keep the backfilled
        # "USD", not rewrite it to null and not drop it.
        msgs = hand_messages() + [
            v2_change("s1", 4, "T3", 1, "accounts", "update", {"key": "k"},
                      before={}, after={"amount": 40.0}, schema_version=2),
            commit("s1", 5, "T3", 3, 1, {"accounts": 1}),
        ]
        archive, snap, spath, hpath = write_v2_archive(
            self.base, msgs, history=HISTORY, schema=SCHEMA,
            snapshot_rows=SNAPSHOT, sealed_commit_seq=3,
        )
        out = self.base / "pkg"
        run_import(archive, snap, spath, out, 3, history_path=hpath)
        rows = [json.loads(line) for line in
                (out / "changes.jsonl").read_text().splitlines()]
        ev = rows[-1]["events"][0]
        self.assertEqual(ev["after"],
                         {"key": "k", "amount": 40.0, "currency": "USD"})
        self.assertEqual(run_verify(archive, snap, spath, 3, out,
                                    history_path=hpath).verdict, VERDICT_MATCH)

    def test_explicit_null_on_a_later_nullable_field_writes_null(self):
        history = two_version_history(include_currency=True)
        # Make the added currency column nullable so an explicit null is legal.
        history["versions"][1]["tables"]["accounts"]["columns"][2][
            "nullable"] = True
        schema = target_accounts_schema()
        schema["tables"][0]["columns"][2]["nullable"] = True
        msgs = hand_messages() + [
            v2_change("s1", 4, "T3", 1, "accounts", "update", {"key": "k"},
                      before={"currency": "USD"}, after={"currency": None},
                      schema_version=2),
            commit("s1", 5, "T3", 3, 1, {"accounts": 1}),
        ]
        archive, snap, spath, hpath = write_v2_archive(
            self.base, msgs, history=history, schema=schema,
            snapshot_rows=SNAPSHOT, sealed_commit_seq=3,
        )
        out = self.base / "nullpkg"
        run_import(archive, snap, spath, out, 3, history_path=hpath)
        rows = [json.loads(line) for line in
                (out / "changes.jsonl").read_text().splitlines()]
        ev = rows[-1]["events"][0]
        self.assertIsNone(ev["after"]["currency"])
        self.assertEqual(ev["before"]["currency"], "USD")
        self.assertEqual(run_verify(archive, snap, spath, 3, out,
                                    history_path=hpath).verdict, VERDICT_MATCH)

    def test_wrong_but_present_backfill_is_caught(self):
        archive, snap, spath, hpath = write_v2_archive(
            self.base / "wrong", hand_messages(), history=HISTORY,
            schema=SCHEMA, snapshot_rows=SNAPSHOT,
        )
        out = self.base / "wrong" / "pkg"
        run_import(archive, snap, spath, out, 2, history_path=hpath)
        path = out / "changes.jsonl"
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        rows[0]["events"][0]["before"]["currency"] = "EUR"
        path.write_text("\n".join(json.dumps(r, sort_keys=True) for r in rows)
                        + "\n", encoding="utf-8")
        result = run_verify(archive, snap, spath, 2, out, history_path=hpath)
        self.assertEqual(result.verdict, VERDICT_PACKAGE)


class ReportHashTamperTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        self.base = self.tmp.path / "hashes"
        self.archive, self.snap, self.spath, self.hpath = write_v2_archive(
            self.base, hand_messages(), history=HISTORY, schema=SCHEMA,
            snapshot_rows=SNAPSHOT,
        )
        self.out = self.base / "pkg"
        run_import(self.archive, self.snap, self.spath, self.out, 2,
                   history_path=self.hpath)

    def tearDown(self):
        self.tmp.cleanup()

    def _tamper_report(self, mutate):
        path = self.out / "capture-report.json"
        report = json.loads(path.read_text())
        mutate(report)
        path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    def test_output_changes_hash_tamper_is_caught(self):
        self._tamper_report(
            lambda r: r["output"].update(changes_hash="f" * 64)
        )
        result = run_verify(self.archive, self.snap, self.spath, 2, self.out,
                            history_path=self.hpath)
        self.assertEqual(result.verdict, VERDICT_PACKAGE)
        self.assertTrue(any(f.get("section") == "output"
                            for f in result.failures))

    def test_target_schema_hash_tamper_is_caught(self):
        self._tamper_report(
            lambda r: r["target_schema"].update(schema_hash="0" * 64)
        )
        result = run_verify(self.archive, self.snap, self.spath, 2, self.out,
                            history_path=self.hpath)
        self.assertEqual(result.verdict, VERDICT_PACKAGE, result.failures)
        self.assertTrue(any(f.get("section") == "target_schema"
                            for f in result.failures))

    def test_dropping_the_history_section_is_caught(self):
        self._tamper_report(lambda r: r.pop("schema_history"))
        result = run_verify(self.archive, self.snap, self.spath, 2, self.out,
                            history_path=self.hpath)
        self.assertEqual(result.verdict, VERDICT_PACKAGE)
        self.assertTrue(any(f.get("section") == "schema_history"
                            for f in result.failures))


class ForeignKeyEvolutionTest(unittest.TestCase):
    """Foreign keys are defined by field ID and survive a rename."""

    def setUp(self):
        self.tmp = TempDir()
        self.base = self.tmp.path / "fk"

    def tearDown(self):
        self.tmp.cleanup()

    def _history(self):
        return {
            "format": "capture-schema-history-v1",
            "versions": [
                {"version": 1, "effective_commit_seq": 0, "tables": {
                    "P": {"target_table": "P", "primary_key": ["f-pid"],
                          "columns": [
                              field("f-pid", "id", "string", False),
                              field("f-tot", "amt", "int", False)],
                          "unique_keys": [], "foreign_keys": []},
                    "C": {"target_table": "C", "primary_key": ["f-cid"],
                          "columns": [
                              field("f-cid", "id", "string", False),
                              field("f-p", "pid", "string", False)],
                          "unique_keys": [],
                          "foreign_keys": [{
                              "columns": ["f-p"], "ref_table": "P",
                              "ref_columns": ["f-pid"], "nullable": False,
                          }]}}},
                {"version": 2, "effective_commit_seq": 2, "tables": {
                    "P": {"target_table": "P", "primary_key": ["f-pid"],
                          "columns": [
                              field("f-pid", "id", "string", False),
                              field("f-tot", "total", "float", False),
                              field("f-ccy", "ccy", "string", False,
                                    backfill="USD")],
                          "unique_keys": [], "foreign_keys": []},
                    "C": {"target_table": "C", "primary_key": ["f-cid"],
                          "columns": [
                              field("f-cid", "id", "string", False),
                              field("f-p", "pid", "string", False)],
                          "unique_keys": [],
                          "foreign_keys": [{
                              "columns": ["f-p"], "ref_table": "P",
                              "ref_columns": ["f-pid"], "nullable": False,
                          }]}}},
            ],
        }

    def _schema(self):
        return {"tables": [
            {"name": "P", "columns": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "total", "type": "float", "nullable": False},
                {"name": "ccy", "type": "string", "nullable": False}],
             "primary_key": ["id"]},
            {"name": "C", "columns": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "pid", "type": "string", "nullable": False}],
             "primary_key": ["id"],
             "foreign_keys": [{"columns": ["pid"], "ref_table": "P",
                               "ref_columns": ["id"]}]},
        ]}

    def test_parent_and_child_convert_and_verify(self):
        msgs = [
            {"source_id": "src-test-1", "stream": "s1", "offset": 0,
             "kind": "change", "tx_id": "t", "tx_order": 1, "table": "C",
             "op": "update", "key": {"id": "c1"}, "before": {"pid": "p1"},
             "after": {"pid": "p1"}, "schema_version": 1},
            commit("s1", 1, "t", 1, 1, {"C": 1}),
        ]
        archive, snap, spath, hpath = write_v2_archive(
            self.base, msgs, history=self._history(), schema=self._schema(),
            snapshot_rows={
                "P": [{"id": "p1", "amt": 5}],
                "C": [{"id": "c1", "pid": "p1"}],
            },
            sealed_commit_seq=2,
        )
        out = self.base / "pkg"
        run_import(archive, snap, spath, out, 1, history_path=hpath)
        parent = [json.loads(line) for line in
                  (out / "snapshot" / "P.jsonl").read_text().splitlines()]
        self.assertEqual(parent, [{"id": "p1", "total": 5.0, "ccy": "USD"}])
        result = run_verify(archive, snap, spath, 1, out, history_path=hpath)
        self.assertEqual(result.verdict, VERDICT_MATCH, result.failures)

    def test_retyped_foreign_key_is_refused(self):
        history = self._history()
        history["versions"][1]["tables"]["P"]["columns"][1]["type"] = "string"
        path = write_json(self.tmp.path / "bad-fk.json", history)
        with self.assertRaises(CaptureSchemaHistoryError) as ctx:
            load_schema_history(path)
        self.assertIn("only int -> float widening", str(ctx.exception))

    def test_missing_parent_row_fails_restoration(self):
        msgs = [
            {"source_id": "src-test-1", "stream": "s1", "offset": 0,
             "kind": "change", "tx_id": "t", "tx_order": 1, "table": "C",
             "op": "insert", "key": {"id": "c9"},
             "after": {"id": "c9", "pid": "pX"}, "schema_version": 1},
            commit("s1", 1, "t", 1, 1, {"C": 1}),
        ]
        archive, snap, spath, hpath = write_v2_archive(
            self.base, msgs, history=self._history(), schema=self._schema(),
            snapshot_rows={
                "P": [{"id": "p1", "amt": 5}],
                "C": [],
            },
            sealed_commit_seq=2,
        )
        from ddreduce.capture.errors import CaptureRestoreError

        with self.assertRaises(CaptureRestoreError) as ctx:
            run_import(archive, snap, spath, self.base / "pkg", 1,
                       history_path=hpath)
        self.assertIn("foreign key", str(ctx.exception))
        self.assertFalse((self.base / "pkg").exists())
