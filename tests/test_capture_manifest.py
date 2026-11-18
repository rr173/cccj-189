"""capture-v1 manifest and message-level validation tests."""

from __future__ import annotations

import json
import os
import unittest
from pathlib import Path

from ddreduce.capture.errors import CaptureArchiveError
from ddreduce.capture.manifest import load_manifest
from ddreduce.capture.messages import validate_message
from ddreduce.schema import load_schema

from .capture_helpers import (
    DEFAULT_SOURCE_ID,
    abort,
    change,
    commit,
    heartbeat,
    identity_bindings,
    write_archive,
)
from .helpers import TempDir, write_json

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

SNAPSHOT = {"T": [{"id": "x", "v": 1, "tag": "old"}]}


class ManifestValidationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        self.base = self.tmp.path / "case"
        msgs = [
            change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={"v": 2}),
            commit("s1", 1, "a", 1, 1, {"T": 1}),
        ]
        self.archive, self.snapshot, self.schema_path = write_archive(
            self.base, msgs, schema=SCHEMA, snapshot=SNAPSHOT
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _manifest(self):
        return load_manifest(self.archive, load_schema(self.schema_path))

    def _rewrite_manifest(self, mutate):
        path = self.archive / "capture.json"
        doc = json.loads(path.read_text(encoding="utf-8"))
        mutate(doc)
        write_json(path, doc)

    def test_valid_manifest_loads(self):
        m = self._manifest()
        self.assertEqual(m.version, 1)
        self.assertEqual(m.source_id, DEFAULT_SOURCE_ID)
        self.assertEqual(m.bindings.target_table("T"), "T")

    def test_unsupported_version_rejected(self):
        self._rewrite_manifest(lambda d: d.update(version=2))
        with self.assertRaises(CaptureArchiveError) as ctx:
            self._manifest()
        self.assertIn("unsupported capture version", str(ctx.exception))

    def test_missing_manifest_field_rejected(self):
        self._rewrite_manifest(lambda d: d.pop("streams"))
        with self.assertRaises(CaptureArchiveError) as ctx:
            self._manifest()
        self.assertIn("missing fields", str(ctx.exception))

    def test_unknown_manifest_field_rejected(self):
        self._rewrite_manifest(lambda d: d.update(extra=1))
        with self.assertRaises(CaptureArchiveError) as ctx:
            self._manifest()
        self.assertIn("unknown manifest fields", str(ctx.exception))

    def test_bool_impersonating_watermark_rejected(self):
        self._rewrite_manifest(lambda d: d.update(sealed_commit_seq=True))
        with self.assertRaises(CaptureArchiveError):
            self._manifest()

    def test_sealed_before_snapshot_rejected(self):
        self._rewrite_manifest(
            lambda d: d.update(snapshot_commit_seq=5, sealed_commit_seq=4)
        )
        with self.assertRaises(CaptureArchiveError):
            self._manifest()

    def test_duplicate_stream_name_rejected(self):
        self._rewrite_manifest(
            lambda d: d.update(streams=[
                {"name": "s1", "first_offset": 0, "last_offset": 1},
                {"name": "s1", "first_offset": 0, "last_offset": 0},
            ])
        )
        with self.assertRaises(CaptureArchiveError):
            self._manifest()

    def test_escaping_and_absolute_paths_rejected(self):
        for bad in ("../escape.jsonl", "/abs/path.jsonl", "shards/../../x"):
            self._rewrite_manifest(lambda d, b=bad: d.update(files=[b]))
            with self.assertRaises(CaptureArchiveError):
                self._manifest()

    def test_duplicate_file_entry_rejected(self):
        self._rewrite_manifest(
            lambda d: d.update(files=["shards/s1.jsonl", "shards/s1.jsonl"])
        )
        with self.assertRaises(CaptureArchiveError):
            self._manifest()

    def test_missing_declared_file_rejected(self):
        self._rewrite_manifest(lambda d: d.update(files=["shards/nope.jsonl"]))
        with self.assertRaises(CaptureArchiveError):
            self._manifest()

    def test_undeclared_file_rejected(self):
        rogue = self.archive / "shards" / "rogue.jsonl"
        rogue.parent.mkdir(exist_ok=True)
        rogue.write_text('{"x": 1}\n', encoding="utf-8")
        with self.assertRaises(CaptureArchiveError) as ctx:
            self._manifest()
        self.assertIn("undeclared file", str(ctx.exception))

    def test_symlink_rejected(self):
        link = self.archive / "shards" / "link.jsonl"
        os.symlink(self.archive / "shards" / "s1.jsonl", link)
        with self.assertRaises(CaptureArchiveError):
            self._manifest()
        link.unlink()

    def test_binding_must_cover_every_target_column(self):
        self._rewrite_manifest(lambda d: d["bindings"]["tables"]["T"]["columns"].pop("tag"))
        with self.assertRaises(CaptureArchiveError) as ctx:
            self._manifest()
        self.assertIn("does not cover target columns", str(ctx.exception))

    def test_binding_duplicate_target_column_rejected(self):
        def mutate(d):
            d["bindings"]["tables"]["T"]["columns"]["v"] = "id"
        self._rewrite_manifest(mutate)
        with self.assertRaises(CaptureArchiveError):
            self._manifest()

    def test_binding_unknown_target_table_rejected(self):
        self._rewrite_manifest(
            lambda d: d["bindings"]["tables"]["T"].update(target="nope")
        )
        with self.assertRaises(CaptureArchiveError):
            self._manifest()

    def test_binding_must_cover_every_target_table(self):
        two = {
            "tables": [
                SCHEMA["tables"][0],
                {"name": "P",
                 "columns": [{"name": "id", "type": "string", "nullable": False}],
                 "primary_key": ["id"]},
            ]
        }
        base2 = self.tmp.path / "two"
        partial_bindings = {
            "tables": {
                "T": {"target": "T",
                      "columns": {"id": "id", "v": "v", "tag": "tag"}},
            }
        }
        archive2, _snap, schema2 = write_archive(
            base2,
            [change("s1", 0, "a", 1, "T", "insert", {"id": "q"},
                    after={"id": "q", "v": 1, "tag": None}),
             commit("s1", 1, "a", 1, 1, {"T": 1})],
            schema=two, snapshot={"T": [], "P": []},
            bindings=partial_bindings,
        )
        with self.assertRaises(CaptureArchiveError) as ctx:
            load_manifest(archive2, load_schema(schema2))
        self.assertIn("do not cover target tables", str(ctx.exception))


class MessageValidationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        base = self.tmp.path / "case"
        self.archive, _snap, self.schema_path = write_archive(
            base,
            [change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                    before={"v": 1}, after={"v": 2}),
             commit("s1", 1, "a", 1, 1, {"T": 1})],
            schema=SCHEMA, snapshot=SNAPSHOT,
        )
        self.schema = load_schema(self.schema_path)
        self.manifest = load_manifest(self.archive, self.schema)

    def tearDown(self):
        self.tmp.cleanup()

    def _validate(self, msg):
        return validate_message(msg, self.manifest, self.schema, "f.jsonl", 1)

    def test_envelope_requires_nonneg_integer_offset(self):
        with self.assertRaises(CaptureArchiveError):
            self._validate(change("s1", -1, "a", 1, "T", "update", {"id": "x"},
                                  before={"v": 1}, after={"v": 2}))
        with self.assertRaises(CaptureArchiveError):
            self._validate(change("s1", True, "a", 1, "T", "update", {"id": "x"},
                                  before={"v": 1}, after={"v": 2}))

    def test_undeclared_stream_and_out_of_range_offset_rejected(self):
        with self.assertRaises(CaptureArchiveError):
            self._validate(change("nope", 0, "a", 1, "T", "update", {"id": "x"},
                                  before={"v": 1}, after={"v": 2}))
        with self.assertRaises(CaptureArchiveError):
            self._validate(change("s1", 99, "a", 1, "T", "update", {"id": "x"},
                                  before={"v": 1}, after={"v": 2}))

    def test_source_id_must_match_manifest(self):
        msg = change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                     before={"v": 1}, after={"v": 2}, source_id="other")
        with self.assertRaises(CaptureArchiveError) as ctx:
            self._validate(msg)
        self.assertIn("does not match manifest source_id", str(ctx.exception))

    def test_unknown_kind_rejected(self):
        msg = {"source_id": DEFAULT_SOURCE_ID, "stream": "s1", "offset": 0,
               "kind": "watermark"}
        with self.assertRaises(CaptureArchiveError):
            self._validate(msg)

    def test_unknown_message_field_rejected(self):
        msg = heartbeat("s1", 0)
        msg["extra"] = 1
        with self.assertRaises(CaptureArchiveError) as ctx:
            self._validate(msg)
        self.assertIn("unknown fields", str(ctx.exception))

    def test_change_requires_fields_and_valid_tx_order(self):
        base = change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                      before={"v": 1}, after={"v": 2})
        for bad in (dict(base, tx_order=0), dict(base, tx_order=True),
                    dict(base, tx_id="")):
            with self.assertRaises(CaptureArchiveError):
                self._validate(bad)

    def test_insert_requires_complete_after_and_no_before(self):
        ok = change("s1", 0, "a", 1, "T", "insert", {"id": "q"},
                    after={"id": "q", "v": 1, "tag": None})
        self._validate(ok)
        with self.assertRaises(CaptureArchiveError):
            self._validate(dict(ok, before={}))
        bad = dict(ok)
        bad["after"] = {"id": "q", "v": 1}
        with self.assertRaises(CaptureArchiveError) as ctx:
            self._validate(bad)
        self.assertIn("insert after must be complete", str(ctx.exception))

    def test_delete_allows_empty_before_but_no_after(self):
        self._validate(change("s1", 0, "a", 1, "T", "delete", {"id": "x"}, before={}))
        with self.assertRaises(CaptureArchiveError):
            self._validate(change("s1", 0, "a", 1, "T", "delete", {"id": "x"},
                                  before={}, after={"v": 1}))

    def test_update_requires_both_images_empty_after_allowed(self):
        self._validate(change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                              before={"v": 1}, after={}))
        no_after = change("s1", 0, "a", 1, "T", "update", {"id": "x"})
        no_after["before"] = {}
        with self.assertRaises(CaptureArchiveError):
            self._validate(no_after)

    def test_new_key_only_on_update_with_full_pk(self):
        with self.assertRaises(CaptureArchiveError):
            self._validate(change("s1", 0, "a", 1, "T", "insert", {"id": "q"},
                                  after={"id": "q", "v": 1, "tag": None},
                                  new_key={"id": "z"}))
        with self.assertRaises(CaptureArchiveError):
            self._validate(change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                                  before={}, after={}, new_key={"v": 2}))

    def test_commit_count_mismatch_and_bool_seq_rejected(self):
        with self.assertRaises(CaptureArchiveError):
            self._validate(commit("s1", 0, "a", 1, 2, {"T": 1}))
        with self.assertRaises(CaptureArchiveError):
            self._validate(commit("s1", 0, "a", True, 1, {"T": 1}))
        with self.assertRaises(CaptureArchiveError):
            self._validate(commit("s1", 0, "a", 1, 0, {"T": 0}))

    def test_abort_must_not_carry_commit_seq(self):
        good = abort("s1", 0, "a", 1, {"T": 1})
        self._validate(good)
        bad = dict(good)
        bad["commit_seq"] = 9
        with self.assertRaises(CaptureArchiveError) as ctx:
            self._validate(bad)
        self.assertIn("abort must not carry commit_seq", str(ctx.exception))

    def test_zero_event_tx_uses_empty_table_counts(self):
        self._validate(commit("s1", 0, "z", 1, 0, {}))
        with self.assertRaises(CaptureArchiveError):
            self._validate(commit("s1", 0, "z", 1, 0, {"T": 0}))

    def test_unknown_table_and_bad_key_columns_rejected(self):
        with self.assertRaises(CaptureArchiveError):
            self._validate(change("s1", 0, "a", 1, "nope", "insert", {"id": "q"},
                                  after={"id": "q"}))
        with self.assertRaises(CaptureArchiveError):
            self._validate(change("s1", 0, "a", 1, "T", "insert", {"v": 1},
                                  after={"id": "q", "v": 1, "tag": None}))


if __name__ == "__main__":
    unittest.main()
