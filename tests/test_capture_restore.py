"""capture-v1 row restoration semantics and provenance tests."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from ddreduce.capture.errors import CaptureArchiveError, CaptureRestoreError
from ddreduce.capture.importer import run_import
from ddreduce.capture.verifier import (
    VERDICT_MATCH,
    VERDICT_PACKAGE,
    VERDICT_RESTORE,
    run_verify,
)
from ddreduce.logmodel import load_changelog
from ddreduce.model import Database
from ddreduce.replay import validate_log_inputs
from ddreduce.schema import load_schema

from .capture_helpers import (
    abort,
    change,
    commit,
    heartbeat,
    write_archive,
)
from .helpers import TempDir

SCHEMA = {
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
        }
    ]
}

FK_SCHEMA = {
    "tables": [
        {"name": "P",
         "columns": [{"name": "id", "type": "string", "nullable": False},
                     {"name": "v", "type": "int", "nullable": False}],
         "primary_key": ["id"]},
        {"name": "C",
         "columns": [{"name": "id", "type": "string", "nullable": False},
                     {"name": "p_id", "type": "string", "nullable": False}],
         "primary_key": ["id"],
         "foreign_keys": [
             {"columns": ["p_id"], "ref_table": "P", "ref_columns": ["id"]}
         ]},
    ]
}


class CaptureCase(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        self.counter = 0

    def tearDown(self):
        self.tmp.cleanup()

    def build(self, messages, schema, snapshot, *, through=None,
              snapshot_commit_seq=0, sealed=None, bindings=None):
        self.counter += 1
        base = self.tmp.path / f"case{self.counter}"
        archive, snap_dir, schema_path = write_archive(
            base, messages, schema=schema, snapshot=snapshot,
            snapshot_commit_seq=snapshot_commit_seq,
            sealed_commit_seq=sealed, bindings=bindings,
        )
        if through is None:
            sealed_value = sealed if sealed is not None else max(
                (m["commit_seq"] for m in messages if m["kind"] == "commit"),
                default=snapshot_commit_seq,
            )
            through = sealed_value
        out = base / "package"
        result = run_import(archive, snap_dir, schema_path, out, through)
        return result, archive, snap_dir, schema_path, out, through

    def load_output(self, out, schema_path, snap_dir):
        schema = load_schema(schema_path)
        db = Database.load(schema, snap_dir)
        log = load_changelog(out / "changes.jsonl", schema)
        return validate_log_inputs(schema, Path(schema_path).read_text("utf-8"),
                                   db, log)


class PartialImageRestoreTest(CaptureCase):
    def test_partial_update_preserves_unspecified_and_writes_explicit_null(self):
        msgs = [
            change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={"v": 2}),
            change("s1", 1, "a", 2, "T", "update", {"id": "x"},
                   before={"tag": "old"}, after={"tag": None}),
            commit("s1", 2, "a", 1, 2, {"T": 2}),
        ]
        result, archive, snap, spath, out, through = self.build(
            msgs, SCHEMA, {"T": [{"id": "x", "v": 1, "tag": "old"}]}
        )
        validated = self.load_output(out, spath, snap)
        tx = validated.log.transactions[0]
        self.assertEqual(tx.events[0].after, {"id": "x", "v": 2, "tag": "old"})
        self.assertEqual(tx.events[1].before, {"id": "x", "v": 2, "tag": "old"})
        self.assertEqual(tx.events[1].after, {"id": "x", "v": 2, "tag": None})
        final = validated.normalize({0}).final_database()
        self.assertEqual(final.rows['T/str:"x"'].data,
                         {"id": "x", "v": 2, "tag": None})

    def test_empty_after_is_still_an_update(self):
        msgs = [
            change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={}),
            commit("s1", 1, "a", 1, 1, {"T": 1}),
        ]
        result, *_rest = self.build(
            msgs, SCHEMA, {"T": [{"id": "x", "v": 1, "tag": "t"}]}
        )
        out = _rest[3]
        spath = _rest[2]
        snap = _rest[1]
        validated = self.load_output(out, spath, snap)
        self.assertEqual(len(validated.log), 1)
        tx = validated.log.transactions[0]
        self.assertEqual(len(tx.events), 1)
        self.assertEqual(tx.events[0].after, {"id": "x", "v": 1, "tag": "t"})

    def test_stale_before_image_is_restore_failure(self):
        msgs = [
            change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                   before={"v": 99}, after={"v": 2}),
            commit("s1", 1, "a", 1, 1, {"T": 1}),
        ]
        with self.assertRaises(CaptureRestoreError) as ctx:
            self.build(msgs, SCHEMA, {"T": [{"id": "x", "v": 1, "tag": "t"}]})
        self.assertIn("stale before-image", str(ctx.exception))
        self.assertIn("tx 'a'", str(ctx.exception))
        self.assertIn("tx_order 1", str(ctx.exception))

    def test_aborted_events_never_apply(self):
        msgs = [
            change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={"v": 999}),
            abort("s1", 1, "a", 1, {"T": 1}),
            heartbeat("s1", 2),
        ]
        result, *_ = self.build(msgs, SCHEMA,
                                {"T": [{"id": "x", "v": 1, "tag": "t"}]})
        self.assertEqual(result.report["counts"]["aborted_total"], 1)
        self.assertEqual(result.report["counts"]["exported_transactions"], 0)
        # No staging directory left behind, package still has all five parts.
        leftovers = [p for p in result.output_dir.parent.iterdir()
                     if p.name.startswith(".capture-import-staging-")]
        self.assertEqual(leftovers, [])

    def test_insert_on_existing_key_and_delete_on_absent_key_rejected(self):
        with self.assertRaises(CaptureRestoreError):
            self.build(
                [change("s1", 0, "a", 1, "T", "insert", {"id": "x"},
                        after={"id": "x", "v": 1, "tag": None}),
                 commit("s1", 1, "a", 1, 1, {"T": 1})],
                SCHEMA, {"T": [{"id": "x", "v": 1, "tag": "t"}]},
            )
        with self.assertRaises(CaptureRestoreError):
            self.build(
                [change("s1", 0, "a", 1, "T", "delete", {"id": "q"}, before={}),
                 commit("s1", 1, "a", 1, 1, {"T": 1})],
                SCHEMA, {"T": []},
            )


class KeyMoveTest(CaptureCase):
    def test_key_move_expands_to_delete_insert_with_full_images(self):
        msgs = [
            change("s1", 0, "b", 1, "T", "update", {"id": "x"},
                   before={}, after={"v": 5}, new_key={"id": "y"}),
            commit("s1", 1, "b", 1, 1, {"T": 1}),
        ]
        result, archive, snap, spath, out, through = self.build(
            msgs, SCHEMA, {"T": [{"id": "x", "v": 1, "tag": "t"}]}
        )
        validated = self.load_output(out, spath, snap)
        tx = validated.log.transactions[0]
        self.assertEqual([e.op for e in tx.events], ["delete", "insert"])
        self.assertEqual(tx.events[0].before, {"id": "x", "v": 1, "tag": "t"})
        self.assertEqual(tx.events[1].after, {"id": "y", "v": 5, "tag": "t"})
        # Both expanded events trace back to the one logical event.
        map_rows = [json.loads(line)
                    for line in (out / "capture-map.jsonl").read_text(
                        "utf-8").splitlines()]
        sources = {(r["source"]["tx_id"], r["source"]["tx_order"]) for r in map_rows}
        self.assertEqual(sources, {("b", 1)})
        self.assertTrue(all(r["source"]["expanded_from_key_move"] for r in map_rows))

    def test_key_move_onto_existing_target_rejected(self):
        msgs = [
            change("s1", 0, "b", 1, "T", "update", {"id": "x"},
                   before={}, after={}, new_key={"id": "y"}),
            commit("s1", 1, "b", 1, 1, {"T": 1}),
        ]
        with self.assertRaises(CaptureRestoreError) as ctx:
            self.build(msgs, SCHEMA,
                       {"T": [{"id": "x", "v": 1, "tag": None},
                              {"id": "y", "v": 2, "tag": None}]})
        self.assertIn("key move target", str(ctx.exception))

    def test_new_key_equal_to_old_key_is_one_ordinary_update(self):
        msgs = [
            change("s1", 0, "b", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={"v": 7}, new_key={"id": "x"}),
            commit("s1", 1, "b", 1, 1, {"T": 1}),
        ]
        result, *_rest = self.build(
            msgs, SCHEMA, {"T": [{"id": "x", "v": 1, "tag": None}]}
        )
        out, spath, snap = _rest[3], _rest[2], _rest[1]
        validated = self.load_output(out, spath, snap)
        tx = validated.log.transactions[0]
        self.assertEqual(len(tx.events), 1)
        self.assertEqual(tx.events[0].op, "update")
    def test_plain_update_changing_pk_without_new_key_rejected(self):
        msgs = [
            change("s1", 0, "b", 1, "T", "update", {"id": "x"},
                   before={}, after={"id": "y"}),
            commit("s1", 1, "b", 1, 1, {"T": 1}),
        ]
        with self.assertRaises(CaptureRestoreError):
            self.build(msgs, SCHEMA,
                       {"T": [{"id": "x", "v": 1, "tag": None}]})

    def test_after_key_value_must_agree_with_new_key(self):
        msgs = [
            change("s1", 0, "b", 1, "T", "update", {"id": "x"},
                   before={}, after={"id": "z"}, new_key={"id": "y"}),
            commit("s1", 1, "b", 1, 1, {"T": 1}),
        ]
        with self.assertRaises(CaptureRestoreError):
            self.build(msgs, SCHEMA,
                       {"T": [{"id": "x", "v": 1, "tag": None}]})


class CommitConstraintTest(CaptureCase):
    def test_unique_value_swap_inside_one_transaction_commits(self):
        msgs = [
            change("s1", 0, "sw", 1, "T", "update", {"id": "u1"},
                   before={"tag": "a"}, after={"tag": "b"}),
            change("s1", 1, "sw", 2, "T", "update", {"id": "u2"},
                   before={"tag": "b"}, after={"tag": "a"}),
            commit("s1", 2, "sw", 1, 2, {"T": 2}),
        ]
        result, *_rest = self.build(
            msgs, SCHEMA,
            {"T": [{"id": "u1", "v": 1, "tag": "a"},
                   {"id": "u2", "v": 2, "tag": "b"}]},
        )
        out, spath, snap = _rest[3], _rest[2], _rest[1]
        validated = self.load_output(out, spath, snap)
        normalized = validated.normalize({0})
        final = normalized.final_database()
        self.assertEqual(final.rows['T/str:"u1"'].data["tag"], "b")
        self.assertEqual(final.rows['T/str:"u2"'].data["tag"], "a")

    def test_commit_time_unique_violation_rejects_import(self):
        msgs = [
            change("s1", 0, "sw", 1, "T", "update", {"id": "u2"},
                   before={"tag": "b"}, after={"tag": "a"}),
            commit("s1", 1, "sw", 1, 1, {"T": 1}),
        ]
        with self.assertRaises(CaptureRestoreError) as ctx:
            self.build(
                msgs, SCHEMA,
                {"T": [{"id": "u1", "v": 1, "tag": "a"},
                       {"id": "u2", "v": 2, "tag": "b"}]},
            )
        self.assertIn("unique constraint", str(ctx.exception))

    def test_child_before_parent_inside_one_transaction_commits(self):
        msgs = [
            change("s1", 0, "mk", 1, "C", "insert", {"id": "c1"},
                   after={"id": "c1", "p_id": "p1"}),
            change("s1", 1, "mk", 2, "P", "insert", {"id": "p1"},
                   after={"id": "p1", "v": 7}),
            commit("s1", 2, "mk", 1, 2, {"C": 1, "P": 1}),
        ]
        result, *_rest = self.build(msgs, FK_SCHEMA, {"P": [], "C": []})
        out, spath, snap = _rest[3], _rest[2], _rest[1]
        validated = self.load_output(out, spath, snap)
        normalized = validated.normalize({0})
        self.assertEqual(len(normalized.final_database().rows), 2)

    def test_commit_time_fk_violation_rejects_import(self):
        msgs = [
            change("s1", 0, "mk", 1, "C", "insert", {"id": "c1"},
                   after={"id": "c1", "p_id": "missing"}),
            commit("s1", 1, "mk", 1, 1, {"C": 1}),
        ]
        with self.assertRaises(CaptureRestoreError):
            self.build(msgs, FK_SCHEMA, {"P": [], "C": []})


class WindowAndOrderingTest(CaptureCase):
    def _two_tx_messages(self):
        return [
            # B's commit physically first: transport order is meaningless.
            commit("s1", 0, "B", 2, 1, {"T": 1}),
            change("s1", 1, "A", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={"v": 2}),
            commit("s1", 2, "A", 1, 1, {"T": 1}),
            change("s1", 3, "B", 1, "T", "update", {"id": "x"},
                   before={"v": 2}, after={"v": 3}),
        ]

    def test_export_order_is_commit_seq_not_transport_order(self):
        result, *_rest = self.build(
            self._two_tx_messages(), SCHEMA,
            {"T": [{"id": "x", "v": 1, "tag": None}]},
            snapshot_commit_seq=0, sealed=2,
        )
        out, spath, snap = _rest[3], _rest[2], _rest[1]
        validated = self.load_output(out, spath, snap)
        self.assertEqual([tx.tx_id for tx in validated.log], ["A", "B"])

    def test_through_truncation_keeps_later_tx_out_of_row_state(self):
        result, *_rest = self.build(
            self._two_tx_messages(), SCHEMA,
            {"T": [{"id": "x", "v": 1, "tag": None}]},
            through=1, snapshot_commit_seq=0, sealed=2,
        )
        out, spath, snap = _rest[3], _rest[2], _rest[1]
        validated = self.load_output(out, spath, snap)
        self.assertEqual([tx.tx_id for tx in validated.log], ["A"])
        counts = result.report["counts"]
        self.assertEqual(counts["out_of_window_commits"], 1)
        self.assertEqual(counts["exported_transactions"], 1)

    def test_through_outside_window_rejected(self):
        with self.assertRaises(Exception):
            self.build(
                self._two_tx_messages(), SCHEMA,
                {"T": [{"id": "x", "v": 1, "tag": None}]},
                through=9, snapshot_commit_seq=0, sealed=2,
            )

    def test_zero_event_commit_kept_in_map_and_report_not_in_changes(self):
        msgs = [
            change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={"v": 2}),
            commit("s1", 1, "a", 1, 1, {"T": 1}),
            commit("s1", 2, "z", 2, 0, {}),
        ]
        result, *_rest = self.build(msgs, SCHEMA,
                                    {"T": [{"id": "x", "v": 1, "tag": None}]})
        out = _rest[3]
        change_lines = (out / "changes.jsonl").read_text("utf-8").splitlines()
        map_lines = [json.loads(line)
                     for line in (out / "capture-map.jsonl").read_text(
                         "utf-8").splitlines()]
        self.assertEqual(len(change_lines), 1)
        zero = [r for r in map_lines if r.get("zero_event_commit")]
        self.assertEqual(len(zero), 1)
        self.assertEqual(zero[0]["tx_id"], "z")
        self.assertTrue(zero[0]["end_coordinates"])
        self.assertEqual(
            result.report["counts"]["in_window_zero_event_commits"], 1
        )

    def test_existing_output_dir_is_never_overwritten(self):
        result, archive, snap, spath, out, through = self.build(
            [change("s1", 0, "a", 1, "T", "insert", {"id": "q"},
                    after={"id": "q", "v": 1, "tag": None}),
             commit("s1", 1, "a", 1, 1, {"T": 1})],
            SCHEMA, {"T": []},
        )
        with self.assertRaises(Exception):
            run_import(archive, snap, spath, out, through)


class PackageVerifyTest(CaptureCase):
    def setUp(self):
        super().setUp()
        self._inputs: dict | None = None

    def _build_and_verify(self, messages, schema, snapshot, **kwargs):
        result, archive, snap, spath, out, through = self.build(
            messages, schema, snapshot, **kwargs
        )
        self._inputs = {
            "archive": archive, "snap": snap, "spath": spath, "through": through,
        }
        verify = run_verify(archive, snap, spath, through, out)
        return result, verify, out

    def _reverify(self, out):
        i = self._inputs
        return run_verify(i["archive"], i["snap"], i["spath"], i["through"], out)

    def test_clean_package_matches(self):
        msgs = [
            change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={"v": 2}),
            commit("s1", 1, "a", 1, 1, {"T": 1}),
        ]
        _result, verify, _out = self._build_and_verify(
            msgs, SCHEMA, {"T": [{"id": "x", "v": 1, "tag": None}]}
        )
        self.assertEqual(verify.verdict, VERDICT_MATCH)
        self.assertEqual(verify.failures, [])

    def test_tampered_change_is_package_mismatch_not_archive_failure(self):
        # Two updates on the same key; dropping the first leaves the same final
        # row but erases an overwritten update the verifier must still catch.
        msgs = [
            change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={"v": 2}),
            change("s1", 1, "b", 1, "T", "update", {"id": "x"},
                   before={"v": 2}, after={"v": 3}),
            commit("s1", 2, "a", 1, 1, {"T": 1}),
            commit("s1", 3, "b", 2, 1, {"T": 1}),
        ]
        _result, _verify, out = self._build_and_verify(
            msgs, SCHEMA, {"T": [{"id": "x", "v": 1, "tag": None}]}
        )
        lines = (out / "changes.jsonl").read_text("utf-8").splitlines()
        (out / "changes.jsonl").write_text(lines[1] + "\n", encoding="utf-8")
        verify = self._reverify(out)
        self.assertEqual(verify.verdict, VERDICT_PACKAGE)
        self.assertTrue(any("changes.jsonl" in f["file"] for f in verify.failures))

    def test_merged_rebuild_is_detected(self):
        # delete then same-key rebuild must remain two events; folding them
        # away into one update must be detected.
        msgs = [
            change("s1", 0, "d", 1, "T", "delete", {"id": "x"}, before={}),
            commit("s1", 1, "d", 1, 1, {"T": 1}),
            change("s1", 2, "i", 1, "T", "insert", {"id": "x"},
                   after={"id": "x", "v": 9, "tag": None}),
            commit("s1", 3, "i", 2, 1, {"T": 1}),
        ]
        _result, _verify, out = self._build_and_verify(
            msgs, SCHEMA, {"T": [{"id": "x", "v": 1, "tag": None}]}
        )
        folded = json.dumps({
            "tx_id": "folded",
            "events": [{
                "op": "update", "table": "T", "key": {"id": "x"},
                "before": {"id": "x", "v": 1, "tag": None},
                "after": {"id": "x", "v": 9, "tag": None},
            }],
        }, sort_keys=True)
        (out / "changes.jsonl").write_text(folded + "\n", encoding="utf-8")
        verify = self._reverify(out)
        self.assertEqual(verify.verdict, VERDICT_PACKAGE)

    def test_rewritten_provenance_is_detected(self):
        msgs = [
            change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={"v": 2}),
            commit("s1", 1, "a", 1, 1, {"T": 1}),
        ]
        _result, _verify, out = self._build_and_verify(
            msgs, SCHEMA, {"T": [{"id": "x", "v": 1, "tag": None}]}
        )
        map_path = out / "capture-map.jsonl"
        rows = [json.loads(line) for line in map_path.read_text("utf-8").splitlines()]
        rows[0]["source"]["coordinates"] = [{"stream": "s1", "offset": 999}]
        map_path.write_text(
            "\n".join(json.dumps(r, sort_keys=True) for r in rows) + "\n",
            encoding="utf-8",
        )
        verify = self._reverify(out)
        self.assertEqual(verify.verdict, VERDICT_PACKAGE)
        self.assertTrue(any("capture-map.jsonl" in f["file"]
                            for f in verify.failures))

    def test_count_tampering_in_report_is_detected(self):
        msgs = [
            change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={"v": 2}),
            commit("s1", 1, "a", 1, 1, {"T": 1}),
        ]
        _result, _verify, out = self._build_and_verify(
            msgs, SCHEMA, {"T": [{"id": "x", "v": 1, "tag": None}]}
        )
        report_path = out / "capture-report.json"
        report = json.loads(report_path.read_text("utf-8"))
        report["counts"]["physical_messages_read"] = 12345
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        verify = self._reverify(out)
        self.assertEqual(verify.verdict, VERDICT_PACKAGE)
        self.assertTrue(any(f.get("section") == "counts" for f in verify.failures))

    def test_extra_package_file_is_detected(self):
        msgs = [
            change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={"v": 2}),
            commit("s1", 1, "a", 1, 1, {"T": 1}),
        ]
        _result, _verify, out = self._build_and_verify(
            msgs, SCHEMA, {"T": [{"id": "x", "v": 1, "tag": None}]}
        )
        (out / "rogue.txt").write_text("not part of the package\n", encoding="utf-8")
        verify = self._reverify(out)
        self.assertEqual(verify.verdict, VERDICT_PACKAGE)
        self.assertTrue(any("unexpected package files" in f["detail"]
                            for f in verify.failures))


if __name__ == "__main__":
    unittest.main()
