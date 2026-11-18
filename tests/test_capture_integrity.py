"""capture-v1 archive integrity: coverage, de-duplication and closure tests.

Every test feeds a deliberately broken archive and asserts the importer
refuses it with an :class:`CaptureArchiveError` that names the offending
stream/offset, transaction or logical order.  A broken transaction can never
be skipped to produce a seemingly complete package.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from ddreduce.capture.errors import CaptureArchiveError
from ddreduce.capture.importer import run_import
from ddreduce.capture.verifier import VERDICT_ARCHIVE, run_verify

from .capture_helpers import (
    abort,
    change,
    commit,
    heartbeat,
    write_archive,
)
from .helpers import TempDir, write_jsonl

SCHEMA = {
    "tables": [
        {"name": "T",
         "columns": [
             {"name": "id", "type": "string", "nullable": False},
             {"name": "v", "type": "int", "nullable": False},
         ],
         "primary_key": ["id"]}
    ]
}


def good_messages() -> list[dict]:
    return [
        change("s1", 0, "a", 1, "T", "update", {"id": "x"},
               before={"v": 1}, after={"v": 2}),
        commit("s1", 1, "a", 1, 1, {"T": 1}),
    ]


class ArchiveIntegrityTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        self.n = 0

    def tearDown(self):
        self.tmp.cleanup()

    def _run(self, messages, *, expect_verify_archive=False, manifest_kwargs=None):
        self.n += 1
        base = self.tmp.path / f"broken{self.n}"
        kwargs = {"schema": SCHEMA, "snapshot": {"T": [{"id": "x", "v": 1}]}}
        if manifest_kwargs:
            kwargs.update(manifest_kwargs)
        archive, snap, spath = write_archive(base, messages, **kwargs)
        out = base / "pkg"
        with self.assertRaises(CaptureArchiveError) as ctx:
            run_import(archive, snap, spath, out, sealed_of(messages))
        if expect_verify_archive:
            result = run_verify(archive, snap, spath, sealed_of(messages), out)
            self.assertEqual(result.verdict, VERDICT_ARCHIVE)
        return str(ctx.exception)

    def test_missing_offset_is_detected_with_first_missing_position(self):
        msgs = good_messages()
        # Declare a wider range than the messages occupy.
        msg = self._run(
            msgs,
            manifest_kwargs={"streams": [{"name": "s1", "first_offset": 0,
                                          "last_offset": 5}]},
            expect_verify_archive=True,
        )
        self.assertIn("coverage gap", msg)
        self.assertIn("first missing offset 2", msg)

    def test_interior_coverage_gap_names_first_missing_offset(self):
        # Offsets 0,1,2,3 present but manifest claims 0..4: min/max match, the
        # interior hole at offset 4 must be located precisely.
        msgs = [
            change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={"v": 2}),
            commit("s1", 1, "a", 1, 1, {"T": 1}),
            heartbeat("s1", 2),
            heartbeat("s1", 3),
        ]
        msg = self._run(
            msgs,
            manifest_kwargs={"streams": [{"name": "s1", "first_offset": 0,
                                          "last_offset": 4}]},
        )
        self.assertIn("coverage gap", msg)
        self.assertIn("first missing offset 4", msg)

    def test_physical_same_coordinate_conflict_rejected(self):
        msgs = good_messages()
        conflict = change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                          before={"v": 1}, after={"v": 77})  # different payload
        msgs.append(conflict)
        msg = self._run(msgs, expect_verify_archive=True)
        self.assertIn("physical coordinate conflict", msg)
        self.assertIn("s1@0", msg)

    def test_identical_physical_retransmission_is_accepted(self):
        self.n += 1
        base = self.tmp.path / f"ok{self.n}"
        msgs = good_messages()
        msgs.append(json.loads(json.dumps(msgs[0])))  # identical at same coord
        archive, snap, spath = write_archive(
            base, msgs, schema=SCHEMA, snapshot={"T": [{"id": "x", "v": 1}]}
        )
        result = run_import(archive, snap, spath, base / "pkg", 1)
        self.assertEqual(result.report["counts"]["physical_duplicates"], 1)

    def test_logical_event_conflict_rejected(self):
        msgs = good_messages()
        # Same (tx_id, tx_order) at a new coordinate with different content.
        msgs.append(change("s1", 2, "a", 1, "T", "update", {"id": "x"},
                           before={"v": 1}, after={"v": 88}))
        msg = self._run(msgs)
        self.assertIn("logical event conflict", msg)
        self.assertIn("tx_order 1", msg)

    def test_commit_abort_conflict_rejected(self):
        msgs = good_messages()
        msgs.append(abort("s1", 2, "a", 1, {"T": 1}))
        msg = self._run(msgs)
        self.assertIn("conflicting end decisions", msg)

    def test_retransmitted_commit_with_changed_content_rejected(self):
        msgs = good_messages()
        msgs.append(commit("s1", 2, "a", 1, 1, {"T": 1}))  # same content: fine
        self.n += 1
        base = self.tmp.path / f"changed{self.n}"
        archive, snap, spath = write_archive(
            base, msgs, schema=SCHEMA, snapshot={"T": [{"id": "x", "v": 1}]}
        )
        ok = run_import(archive, snap, spath, base / "pkg", 1)
        self.assertEqual(ok.report["counts"]["end_duplicates"], 1)

        bad_msgs = good_messages()
        bad_msgs.append(commit("s1", 2, "a", 9, 1, {"T": 1}))  # changed seq
        base2 = self.tmp.path / f"changed{self.n}b"
        archive2, snap2, spath2 = write_archive(
            base2, bad_msgs, schema=SCHEMA, snapshot={"T": [{"id": "x", "v": 1}]}
        )
        with self.assertRaises(CaptureArchiveError) as ctx:
            run_import(archive2, snap2, spath2, base2 / "pkg", 9)
        self.assertIn("end decisions are immutable", str(ctx.exception))

    def test_end_event_count_mismatch_rejected(self):
        msgs = [
            change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={"v": 2}),
            commit("s1", 1, "a", 1, 2, {"T": 2}),  # claims 2 events, has 1
        ]
        msg = self._run(msgs)
        self.assertIn("tx_order must be exactly 1..event_count", msg)

    def test_table_counts_mismatch_rejected(self):
        msgs = [
            change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={"v": 2}),
            commit("s1", 1, "a", 1, 1, {}),  # zero counts for one event
        ]
        msg = self._run(msgs)
        self.assertIn("table_counts", msg)

    def test_missing_end_message_rejected(self):
        msgs = [
            change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={"v": 2}),
            heartbeat("s1", 1),
        ]
        msg = self._run(msgs, expect_verify_archive=True)
        self.assertIn("no commit/abort decision", msg)

    def test_duplicate_commit_seq_rejected(self):
        msgs = [
            change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={"v": 2}),
            commit("s1", 1, "a", 5, 1, {"T": 1}),
            change("s1", 2, "b", 1, "T", "update", {"id": "x"},
                   before={"v": 2}, after={"v": 3}),
            commit("s1", 3, "b", 5, 1, {"T": 1}),  # same commit_seq
        ]
        msg = self._run(msgs)
        self.assertIn("commit_seq 5 used by both", msg)

    def test_gap_in_commit_seq_is_allowed(self):
        self.n += 1
        base = self.tmp.path / f"gap{self.n}"
        msgs = [
            change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={"v": 2}),
            commit("s1", 1, "a", 7, 1, {"T": 1}),  # gaps around 7 are fine
        ]
        archive, snap, spath = write_archive(
            base, msgs, schema=SCHEMA, snapshot={"T": [{"id": "x", "v": 1}]},
            sealed_commit_seq=10,
        )
        result = run_import(archive, snap, spath, base / "pkg", 10)
        self.assertEqual(result.report["counts"]["exported_transactions"], 1)

    def test_coordinate_conflict_in_otherwise_unexported_tx_still_rejected(self):
        # Even a transaction outside the export window cannot hide conflicts.
        # Two physically distinct messages share one (stream, offset) with
        # different business content; export through 1 would not even select
        # tx 'new', but the archive must still be rejected outright.
        self.n += 1
        base = self.tmp.path / f"hidden{self.n}"
        first = change("s1", 0, "new", 1, "T", "update", {"id": "x"},
                       before={"v": 2}, after={"v": 3})
        colliding = change("s1", 0, "new", 1, "T", "update", {"id": "x"},
                           before={"v": 2}, after={"v": 4})
        msgs = [
            change("s1", 1, "old", 1, "T", "update", {"id": "x"},
                   before={"v": 1}, after={"v": 2}),
            commit("s1", 2, "old", 1, 1, {"T": 1}),
            first,
            colliding,
            commit("s1", 3, "new", 2, 1, {"T": 1}),
        ]
        archive, snap, spath = write_archive(
            base, msgs, schema=SCHEMA, snapshot={"T": [{"id": "x", "v": 1}]}
        )
        with self.assertRaises(CaptureArchiveError) as ctx:
            run_import(archive, snap, spath, base / "pkg", 1)
        self.assertIn("physical coordinate conflict", str(ctx.exception))

    def test_failed_import_leaves_no_output_directory(self):
        self.n += 1
        base = self.tmp.path / f"cleanup{self.n}"
        msgs = [
            change("s1", 0, "a", 1, "T", "update", {"id": "x"},
                   before={"v": 99}, after={"v": 2}),  # stale before
            commit("s1", 1, "a", 1, 1, {"T": 1}),
        ]
        archive, snap, spath = write_archive(
            base, msgs, schema=SCHEMA, snapshot={"T": [{"id": "x", "v": 1}]}
        )
        out = base / "pkg"
        with self.assertRaises(Exception):
            run_import(archive, snap, spath, out, 1)
        self.assertFalse(out.exists())
        staging = [p for p in base.iterdir()
                   if p.name.startswith(".capture-import-staging-")]
        self.assertEqual(staging, [])


def sealed_of(messages: list[dict]) -> int:
    return max(
        (m["commit_seq"] for m in messages if m["kind"] == "commit"),
        default=0,
    )


if __name__ == "__main__":
    unittest.main()
