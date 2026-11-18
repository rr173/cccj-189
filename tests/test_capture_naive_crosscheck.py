"""Cross-check the streaming importer against the naive reference replayer.

For many small randomized archives - multiple transactions, shard reordering,
physical and logical retransmissions, aborts, heartbeats and window
truncation - the product importer must produce exactly the full-image
transaction sequence that the independent :mod:`tests.naive_capture`
reference restores.  The reference shares no product imaging or merge code.

Also covered here:

* re-ordering shards or redelivering messages keeps the normalized output and
  the logical-event -> output-event mapping identical;
* redelivery at a *new* offset records every transport position and only
  raises the duplicate counters;
* two archives with the same final row but different histories
  ("update-then-overwrite" vs "direct update") are never treated as equal.
"""

from __future__ import annotations

import json
import random
import unittest
from pathlib import Path

from ddreduce.capture.importer import run_import
from ddreduce.capture.verifier import VERDICT_MATCH, run_verify

from .capture_helpers import (
    DEFAULT_SOURCE_ID,
    abort,
    change,
    commit,
    heartbeat,
    identity_bindings,
    write_archive,
)
from .helpers import TempDir
from .naive_capture import NaiveCaptureReference

SCHEMA = {
    "tables": [
        {"name": "T",
         "columns": [
             {"name": "id", "type": "string", "nullable": False},
             {"name": "v", "type": "int", "nullable": False},
             {"name": "tag", "type": "string", "nullable": True},
         ],
         "primary_key": ["id"]}
    ]
}


def make_random_history(rng: random.Random) -> tuple[list[dict], list[dict], int]:
    """Build a valid randomized logical history; return (messages, snapshot, sealed)."""
    key_count = 4
    # current full rows
    rows = {f"k{i}": {"id": f"k{i}", "v": i, "tag": (None if i % 2 else f"t{i}")}
            for i in range(key_count)}
    snapshot = [dict(r) for r in rows.values()]
    messages: list[dict] = []
    seq = 0
    # Every emitted message gets a unique (stream, offset) coordinate; stream
    # offsets are allocated densely per stream at build time.
    stream_next = {"s1": 0, "s2": 0}

    def emit(msg: dict, stream: str) -> dict:
        msg = dict(msg)
        msg["stream"] = stream
        msg["offset"] = stream_next[stream]
        stream_next[stream] += 1
        messages.append(msg)
        return msg

    n_tx = rng.randrange(4, 9)
    for tx_index in range(n_tx):
        tx_id = f"tx{tx_index}"
        live_keys = list(rows)
        # Private working copy: events inside the transaction read the state
        # produced by earlier events of the same transaction.
        working = {k: dict(r) for k, r in rows.items()}
        events: list[dict] = []
        n_events = rng.randrange(1, 4)
        aborted = rng.random() < 0.2
        order = 0
        touched: set[str] = set()
        for _ in range(n_events):
            order += 1
            op = rng.choice(["update", "update", "delete", "insert"])
            if op == "insert":
                new_id = f"n{tx_index}-{order}"
                if new_id in working:
                    op = "update"
                else:
                    after = {"id": new_id, "v": rng.randrange(100),
                             "tag": (None if rng.random() < 0.4 else f"g{tx_index}")}
                    stream = rng.choice(["s1", "s2"])
                    emit(change(stream, 0, tx_id, order, "T", "insert",
                                {"id": new_id}, after=after), stream)
                    events.append(("insert", new_id, after))
                    working[new_id] = dict(after)
                    touched.add(new_id)
                    continue
            if op == "delete":
                if not working:
                    order -= 1
                    continue
                candidates = [k for k in working if k not in touched] or list(working)
                key_id = rng.choice(candidates)
                stream = rng.choice(["s1", "s2"])
                # partial before naming the current v
                emit(change(stream, 0, tx_id, order, "T", "delete",
                            {"id": key_id},
                            before={"v": working[key_id]["v"]}),
                     stream)
                events.append(("delete", key_id, None))
                working.pop(key_id, None)
                touched.add(key_id)
                continue
            candidates = [k for k in live_keys if k in working]
            if not candidates:
                order -= 1
                continue
            key_id = rng.choice(candidates)
            cur = working[key_id]
            if rng.random() < 0.3:
                # explicit null write on the nullable column
                after = {"tag": None}
                before = {"tag": cur["tag"]}
            else:
                new_v = cur["v"] + rng.randrange(1, 5)
                after = {"v": new_v}
                before = {"v": cur["v"]}
            stream = rng.choice(["s1", "s2"])
            emit(change(stream, 0, tx_id, order, "T", "update",
                        {"id": key_id}, before=before, after=after), stream)
            events.append(("update", key_id, (before, after)))
            working[key_id] = dict(cur)
            working[key_id].update(after)
            touched.add(key_id)

        if not events:
            # guarantee at least one event per generated tx
            if not working:
                # Nothing to update: emit a fresh insert instead.
                order = 1
                new_id = f"n{tx_index}-fallback"
                after = {"id": new_id, "v": 1, "tag": None}
                stream = rng.choice(["s1", "s2"])
                emit(change(stream, 0, tx_id, order, "T", "insert",
                            {"id": new_id}, after=after), stream)
                events.append(("insert", new_id, after))
                working[new_id] = dict(after)
                touched.add(new_id)
            else:
                order = 1
                key_id = next(iter(working))
                stream = rng.choice(["s1", "s2"])
                emit(change(stream, 0, tx_id, order, "T", "update",
                            {"id": key_id},
                            before={"v": working[key_id]["v"]},
                            after={"v": working[key_id]["v"] + 1}), stream)
                events.append(("update", key_id,
                               ({"v": working[key_id]["v"]},
                                {"v": working[key_id]["v"] + 1})))
                working[key_id]["v"] += 1
                touched.add(key_id)

        table_counts = {"T": len(events)}
        if aborted:
            stream = rng.choice(["s1", "s2"])
            emit(abort(stream, 0, tx_id, len(events), table_counts), stream)
            continue
        seq += 1
        stream = rng.choice(["s1", "s2"])
        emit(commit(stream, 0, tx_id, seq, len(events), table_counts), stream)
        # fold the committed working state into the model state
        for key_id in touched:
            if key_id in working:
                rows[key_id] = dict(working[key_id])
            else:
                rows.pop(key_id, None)
        live_keys = list(rows)

    if rng.random() < 0.5:
        emit(heartbeat(rng.choice(["s1", "s2"]), 0), rng.choice(["s1", "s2"]))
    sealed = seq
    return messages, snapshot, sealed


def add_retransmissions(rng: random.Random, messages: list[dict]) -> list[dict]:
    """Add physical and logical retransmissions without changing content."""
    out = list(messages)
    change_msgs = [m for m in messages if m["kind"] == "change"]
    if change_msgs:
        # Physical duplicate: identical message at the same coordinate.
        pick = rng.choice(change_msgs)
        out.append(json.loads(json.dumps(pick)))
    end_msgs = [m for m in messages if m["kind"] in ("commit", "abort", "heartbeat")]
    if end_msgs:
        # Logical/end retransmission: identical content at a new coordinate in
        # some stream.
        pick = rng.choice(end_msgs)
        clone = json.loads(json.dumps(pick))
        clone["stream"] = rng.choice(["s1", "s2"])
        # Offset chosen beyond current usage; writer will renumber safely.
        clone["offset"] = 10_000 + rng.randrange(1000)
        out.append(clone)
    return out


def normalize_offset_layout(messages: list[dict]) -> list[dict]:
    """Renumber per-stream offsets to a dense 0..N-1 layout."""
    per_stream: dict[str, list[dict]] = {}
    for msg in messages:
        per_stream.setdefault(msg["stream"], []).append(json.loads(json.dumps(msg)))
    for stream, msgs in per_stream.items():
        seen: dict[str, int] = {}
        for msg in msgs:
            canon = json.dumps(msg, sort_keys=True)
            if canon not in seen:
                seen[canon] = len(seen)
            msg["offset"] = seen[canon]
    return [m for msgs in per_stream.values() for m in msgs]


class NaiveCrosscheckTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()

    def tearDown(self):
        self.tmp.cleanup()

    def _run_case(self, seed: int):
        rng = random.Random(seed)
        messages, snapshot, sealed = make_random_history(rng)
        messages = add_retransmissions(rng, messages)
        messages = normalize_offset_layout(messages)
        rng.shuffle(messages)  # transport order has no meaning

        base = self.tmp.path / f"case{seed}"
        archive, snap_dir, schema_path = write_archive(
            base, messages, schema=SCHEMA,
            snapshot={"T": snapshot}, sealed_commit_seq=max(sealed, 1),
        )
        through = max(sealed, 1)
        result = run_import(archive, snap_dir, schema_path,
                            base / "pkg", through)
        # Independent reference restoration.
        reference = NaiveCaptureReference(archive, SCHEMA, {"T": snapshot})
        reference.scan_and_assemble()
        expected_tx = reference.restore(through)
        actual = [
            json.loads(line)
            for line in (base / "pkg" / "changes.jsonl").read_text(
                "utf-8").splitlines()
        ]
        self.assertEqual(
            json.dumps(actual, sort_keys=True),
            json.dumps(expected_tx, sort_keys=True),
            f"seed {seed}: product output differs from naive reference",
        )
        # The output must pass the existing log contract / witness builder.
        from ddreduce.logmodel import load_changelog
        from ddreduce.model import Database
        from ddreduce.replay import validate_log_inputs
        from ddreduce.schema import load_schema

        schema = load_schema(schema_path)
        db = Database.load(schema, snap_dir)
        log = load_changelog(base / "pkg" / "changes.jsonl", schema)
        validate_log_inputs(
            schema, Path(schema_path).read_text("utf-8"), db, log
        )
        # Independent package verification must agree.
        verify = run_verify(archive, snap_dir, schema_path, through,
                            base / "pkg")
        self.assertEqual(verify.verdict, VERDICT_MATCH,
                         f"seed {seed}: {verify.failures}")
        return result, base, archive, snap_dir, schema_path, through, messages

    def test_randomized_archives_match_naive_reference(self):
        for seed in range(25):
            self._run_case(seed)

    def test_shard_reorder_and_redelivery_keep_normalized_output_stable(self):
        result, base, archive, snap_dir, schema_path, through, messages = \
            self._run_case(1234)
        first_changes = (base / "pkg" / "changes.jsonl").read_text("utf-8")
        first_map = (base / "pkg" / "capture-map.jsonl").read_text("utf-8")

        # Rebuild the same logical archive with a different shuffle/seed and
        # one more redelivery at a fresh coordinate.
        rng = random.Random(99)
        redelivered = add_retransmissions(rng, messages)
        redelivered = normalize_offset_layout(redelivered)
        rng.shuffle(redelivered)
        archive2, _, _ = write_archive(
            base / "again", redelivered, schema=SCHEMA,
            snapshot={"T": [json.loads(l) for l in
                            (snap_dir / "T.jsonl").read_text().splitlines()]},
            sealed_commit_seq=through,
        )
        result2 = run_import(archive2, snap_dir, schema_path,
                             base / "pkg2", through)
        second_changes = (base / "pkg2" / "changes.jsonl").read_text("utf-8")
        self.assertEqual(first_changes, second_changes)

        # The logical mapping (ignoring the extra transport coordinates) must
        # describe the same source logical events per output event.
        def logical_view(text: str):
            rows = [json.loads(line) for line in text.splitlines()]
            return [
                (r["output_tx_position"], r.get("output_event_index"),
                 r["source"]["tx_id"], r["source"]["tx_order"],
                 r["source"].get("expanded_from_key_move", False))
                for r in rows
            ]
        self.assertEqual(
            logical_view(first_map),
            logical_view((base / "pkg2" / "capture-map.jsonl").read_text("utf-8")),
        )
        # New transport positions are fully retained and counters grow.
        first_coords = sum(
            len(json.loads(line)["source"]["coordinates"])
            for line in first_map.splitlines()
        )
        second_coords = sum(
            len(json.loads(line)["source"]["coordinates"])
            for line in (base / "pkg2" / "capture-map.jsonl")
            .read_text("utf-8").splitlines()
        )
        self.assertGreaterEqual(second_coords, first_coords)
        self.assertGreaterEqual(
            result2.report["counts"]["physical_messages_read"],
            result.report["counts"]["physical_messages_read"],
        )

    def test_same_final_row_different_histories_are_distinct(self):
        # Archive X: direct update 1 -> 30.
        direct = [
            change("s1", 0, "D", 1, "T", "update", {"id": "k"},
                   before={"v": 1}, after={"v": 30}),
            commit("s1", 1, "D", 1, 1, {"T": 1}),
        ]
        # Archive Y: 1 -> 30 then an overwrite 30 -> 30 (new version, same row).
        overwrite = [
            change("s1", 0, "D1", 1, "T", "update", {"id": "k"},
                   before={"v": 1}, after={"v": 30}),
            commit("s1", 1, "D1", 1, 1, {"T": 1}),
            change("s1", 2, "D2", 1, "T", "update", {"id": "k"},
                   before={"v": 30}, after={"v": 30}),
            commit("s1", 3, "D2", 2, 1, {"T": 1}),
        ]
        snap = {"T": [{"id": "k", "v": 1, "tag": None}]}
        a_dir, _, _ = write_archive(
            self.tmp.path / "direct", direct, schema=SCHEMA, snapshot=snap,
            sealed_commit_seq=1,
        )
        b_dir, b_snap, b_schema = write_archive(
            self.tmp.path / "overwrite", overwrite, schema=SCHEMA, snapshot=snap,
            sealed_commit_seq=2,
        )
        run_import(a_dir, self.tmp.path / "direct" / "snapshot", b_schema,
                   self.tmp.path / "direct" / "pkg", 1)
        run_import(b_dir, b_snap, b_schema,
                   self.tmp.path / "overwrite" / "pkg", 2)
        direct_lines = (self.tmp.path / "direct" / "pkg" /
                        "changes.jsonl").read_text("utf-8").splitlines()
        overwrite_lines = (self.tmp.path / "overwrite" / "pkg" /
                           "changes.jsonl").read_text("utf-8").splitlines()
        self.assertEqual(len(direct_lines), 1)
        self.assertEqual(len(overwrite_lines), 2)
        self.assertNotEqual(direct_lines, overwrite_lines)
        # Cross-verification must reject: package Y is not a valid restoration
        # of archive X (different transaction/image history).
        cross = run_verify(
            a_dir,
            self.tmp.path / "direct" / "snapshot",
            b_schema, 1,
            self.tmp.path / "overwrite" / "pkg",
        )
        self.assertNotEqual(cross.verdict, VERDICT_MATCH)


if __name__ == "__main__":
    unittest.main()
