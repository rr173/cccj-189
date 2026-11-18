#!/usr/bin/env python3
"""Reproducible capture-v1 archive generator (bounded hand sample + tests).

The generator builds capture-v1 archives on top of a tiny ``accounts`` table::

    accounts(key string PK, amount int NOT NULL, note string NULL)

from a fixed hand-computed scenario:

* snapshot: key ``k`` has amount=10, note="old"; snapshot_commit_seq=12;
* transaction A (commit_seq 13): two updates on ``k`` - amount 10 -> 20, then
  note explicitly set to null;
* transaction B (commit_seq 14): move key ``k`` to ``z`` (a key move);
* transaction C: aborted, amount would become 999 - must never be applied;
* a heartbeat;
* the messages are split across several JSONL shards, shuffled in transport
  order, A's messages are retransmitted and B's commit arrives before B's
  changes.

Two additional scenario flavors are supported for verification tests:

* ``--flavor overwrite-vs-direct`` emits two archives that end at the same
  final row but have different histories (update-then-overwrite vs one
  direct update); the verifier must treat them as distinct histories;
* ``--flavor random`` randomizes shard assignment, retransmission and
  reordering from ``--seed`` while keeping the logical content fixed.

The output is deterministic for a given set of arguments.  Committed sample
outputs under demo/capture/sample are bounded; larger archives are generated
at runtime into temp directories by the test suite.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path

SCHEMA = {
    "tables": [
        {
            "name": "accounts",
            "columns": [
                {"name": "key", "type": "string", "nullable": False},
                {"name": "amount", "type": "int", "nullable": False},
                {"name": "note", "type": "string", "nullable": True},
            ],
            "primary_key": ["key"],
        }
    ]
}

SNAPSHOT_ROWS = [{"key": "k", "amount": 10, "note": "old"}]

# Identity bindings: source names equal target names.
BINDINGS = {
    "tables": {
        "accounts": {
            "target": "accounts",
            "columns": {"key": "key", "amount": "amount", "note": "note"},
        }
    }
}

SOURCE_ID = "src-hand-1"
SNAPSHOT_COMMIT_SEQ = 12
SEALED_COMMIT_SEQ = 14


def _msg(stream, offset, kind, **rest) -> dict:
    return {"source_id": SOURCE_ID, "stream": stream, "offset": offset,
            "kind": kind, **rest}


def change(stream, offset, tx_id, tx_order, table, op, key, *,
           before=None, after=None, new_key=None) -> dict:
    msg = _msg(stream, offset, "change", tx_id=tx_id, tx_order=tx_order,
               table=table, op=op, key=key)
    if before is not None:
        msg["before"] = before
    if after is not None:
        msg["after"] = after
    if new_key is not None:
        msg["new_key"] = new_key
    return msg


def commit(stream, offset, tx_id, commit_seq, event_count, table_counts) -> dict:
    return _msg(stream, offset, "commit", tx_id=tx_id, commit_seq=commit_seq,
                event_count=event_count, table_counts=table_counts)


def abort(stream, offset, tx_id, event_count, table_counts) -> dict:
    return _msg(stream, offset, "abort", tx_id=tx_id, event_count=event_count,
                table_counts=table_counts)


def heartbeat(stream, offset) -> dict:
    return _msg(stream, offset, "heartbeat")


def hand_messages() -> list[dict]:
    """The canonical hand-computed message set (single stream, offsets 0..N)."""
    msgs: list[dict] = []
    off = 0

    def add(msg):
        nonlocal off
        msg = dict(msg)
        msg["offset"] = off
        off += 1
        msgs.append(msg)
        return msg

    # Transaction A: amount -> 20 (partial update), then note := null.
    add(change("s1", 0, "A", 1, "accounts", "update", {"key": "k"},
               before={"amount": 10}, after={"amount": 20}))
    add(change("s1", 0, "A", 2, "accounts", "update", {"key": "k"},
               before={"note": "old"}, after={"note": None}))
    add(commit("s1", 0, "A", 13, 2, {"accounts": 2}))

    # Transaction B: move key k -> z.
    add(change("s1", 0, "B", 1, "accounts", "update", {"key": "k"},
               before={}, after={}, new_key={"key": "z"}))
    add(commit("s1", 0, "B", 14, 1, {"accounts": 1}))

    # Aborted transaction C: would set amount 999, never applied.
    add(change("s1", 0, "C", 1, "accounts", "update", {"key": "k"},
               before={"amount": 20}, after={"amount": 999}))
    add(abort("s1", 0, "C", 1, {"accounts": 1}))

    add(heartbeat("s1", 0))
    return msgs


def hand_messages_with_retransmit() -> list[dict]:
    """Hand scenario with A retransmitted, B's commit first, extra transport.

    The multiset mixes both retransmission shapes required by capture-v1:

    * one *physical* retransmission: A's first change is delivered twice at the
      same stream/offset (identical content, different JSON formatting is
      tolerated by the importer);
    * *logical* retransmissions: A's second change, A's commit and a heartbeat
      are redelivered at new coordinates on a second stream;
    * B's commit is moved before B's change (transport order has no meaning).
    """
    base = hand_messages()
    extra: list[dict] = []
    a1 = next(m for m in base if m["kind"] == "change" and m.get("tx_id") == "A"
              and m["tx_order"] == 1)
    # Physical duplicate: same stream, same offset, identical content.
    extra.append(json.loads(json.dumps(a1)))
    for msg in base:
        if msg["kind"] == "change" and msg.get("tx_id") == "A" and msg["tx_order"] == 2:
            clone = json.loads(json.dumps(msg))
            clone["stream"] = "s2"  # logical redelivery at a new coordinate
            extra.append(clone)
        if msg["kind"] == "commit" and msg.get("tx_id") == "A":
            clone = json.loads(json.dumps(msg))
            clone["stream"] = "s2"
            extra.append(clone)
        if msg["kind"] == "heartbeat":
            clone = json.loads(json.dumps(msg))
            clone["stream"] = "s2"
            extra.append(clone)
    # B's commit is moved before B's change (transport order has no meaning).
    b_commit = next(m for m in base if m["kind"] == "commit" and m.get("tx_id") == "B")
    base.remove(b_commit)
    base.insert(0, b_commit)
    return base + extra


def direct_update_messages() -> list[dict]:
    """One direct update taking amount 10 -> 30 on key k (same final state)."""
    return [
        change("s1", 0, "D", 1, "accounts", "update", {"key": "k"},
               before={"amount": 10}, after={"amount": 30}),
        commit("s1", 0, "D", 13, 1, {"accounts": 1}),
    ]


def overwrite_chain_messages() -> list[dict]:
    """Update 10 -> 30 then overwrite 30 -> 30: same final row, more history."""
    return [
        change("s1", 0, "D1", 1, "accounts", "update", {"key": "k"},
               before={"amount": 10}, after={"amount": 30}),
        commit("s1", 0, "D1", 13, 1, {"accounts": 1}),
        change("s1", 0, "D2", 1, "accounts", "update", {"key": "k"},
               before={"amount": 30}, after={"amount": 30}),
        commit("s1", 0, "D2", 14, 1, {"accounts": 1}),
    ]


def emit_archive(
    out: Path,
    messages: list[dict],
    *,
    shards: int = 3,
    seed: int = 7,
    streams: tuple[str, ...] = ("s1", "s2"),
    sealed_commit_seq: int = SEALED_COMMIT_SEQ,
    snapshot_commit_seq: int = SNAPSHOT_COMMIT_SEQ,
) -> dict:
    """Shuffle transport order and split each stream into JSONL shards.

    Each message already carries its assigned ``stream`` and a dense
    per-stream ``offset``; this function only shuffles the transport order
    within/across streams and chunks the per-stream sequences into shard
    files.  A transaction is never split across streams.
    """
    rng = random.Random(seed)
    ordered = [json.loads(json.dumps(m)) for m in messages]
    rng.shuffle(ordered)

    # Group by the stream already assigned to each message.
    per_stream: dict[str, list[dict]] = {name: [] for name in streams}
    for msg in ordered:
        if msg["stream"] not in per_stream:
            per_stream[msg["stream"]] = []
        per_stream[msg["stream"]].append(msg)

    files: list[str] = []
    stream_specs = []
    for stream, stream_msgs in per_stream.items():
        if not stream_msgs:
            continue
        # Keep offsets contiguous/dense: renumber canonically.
        canon_to_offset: dict[str, int] = {}
        for msg in sorted(stream_msgs, key=lambda m: (m["offset"], m["kind"])):
            canon = json.dumps(msg, sort_keys=True)
            if canon not in canon_to_offset:
                canon_to_offset[canon] = len(canon_to_offset)
        for msg in stream_msgs:
            msg["offset"] = canon_to_offset[json.dumps(msg, sort_keys=True)]
        # Shuffle transport order inside the stream shards.
        rng.shuffle(stream_msgs)
        chunks = _split(stream_msgs, shards)
        shard_index = 0
        for chunk in chunks:
            if not chunk:
                continue
            rel = f"shards/{stream}-{shard_index:02d}.jsonl"
            shard_index += 1
            files.append(rel)
            path = out / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                for msg in chunk:
                    fh.write(json.dumps(msg, ensure_ascii=False, sort_keys=True))
                    fh.write("\n")
        stream_specs.append({
            "name": stream,
            "first_offset": 0,
            "last_offset": len(canon_to_offset) - 1,
        })

    manifest = {
        "version": 1,
        "source_id": SOURCE_ID,
        "snapshot_commit_seq": snapshot_commit_seq,
        "sealed_commit_seq": sealed_commit_seq,
        "streams": stream_specs,
        "files": sorted(files),
        "bindings": BINDINGS,
    }
    with open(out / "capture.json", "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.write("\n")
    return manifest


def _split(items: list, n: int) -> list[list]:
    n = max(1, min(n, len(items) or 1))
    size = (len(items) + n - 1) // n
    return [items[i:i + size] for i in range(0, len(items), size)] or [[]]


def write_snapshot_and_schema(out: Path) -> None:
    with open(out / "schema.json", "w", encoding="utf-8") as fh:
        json.dump(SCHEMA, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.write("\n")
    snap = out / "snapshot"
    snap.mkdir(parents=True, exist_ok=True)
    with open(snap / "accounts.jsonl", "w", encoding="utf-8") as fh:
        for row in SNAPSHOT_ROWS:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def generate(out: Path, flavor: str, seed: int, shards: int) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    write_snapshot_and_schema(out)
    archive = out / "archive"
    archive.mkdir(exist_ok=True)
    if flavor == "hand":
        messages = hand_messages_with_retransmit()
        sealed = SEALED_COMMIT_SEQ
    elif flavor == "direct":
        messages = direct_update_messages()
        sealed = 13
    elif flavor == "overwrite":
        messages = overwrite_chain_messages()
        sealed = 14
    else:
        raise ValueError(f"unknown flavor {flavor!r}")
    manifest = emit_archive(archive, messages, shards=shards, seed=seed,
                            sealed_commit_seq=sealed)
    return {"archive": str(archive), "manifest": manifest, "flavor": flavor}


def generate_large(
    out: Path, n_tx: int, n_keys: int, seed: int, shards: int = 4
) -> dict:
    """Generate a large runtime archive for resource/scale tests.

    Transactions are plain partial-image updates, inserts and deletes over
    ``n_keys`` accounts.  Every shard also carries retransmissions.  The
    archive content is fully determined by (n_tx, n_keys, seed).
    """
    rng = random.Random(seed)
    out.mkdir(parents=True, exist_ok=True)
    write_snapshot_and_schema(out)
    live = {f"k{i:06d}": {"key": f"k{i:06d}", "amount": i,
                          "note": (None if i % 3 else f"n{i}")}
            for i in range(n_keys)}
    (out / "snapshot" / "accounts.jsonl").write_text(
        "".join(json.dumps(r, sort_keys=True) + "\n" for r in live.values()),
        encoding="utf-8",
    )
    messages: list[dict] = []
    stream_cursor = {"s1": 0, "s2": 0}
    stream_seen: dict[str, dict[str, int]] = {"s1": {}, "s2": {}}

    def emit(msg, stream):
        msg["stream"] = stream
        canon = json.dumps(msg, sort_keys=True)
        if canon in stream_seen[stream]:
            # Physical retransmission: reuse the same offset.
            msg["offset"] = stream_seen[stream][canon]
        else:
            msg["offset"] = stream_cursor[stream]
            stream_seen[stream][canon] = msg["offset"]
            stream_cursor[stream] += 1
        messages.append(msg)

    sealed = 0
    for tx_index in range(1, n_tx + 1):
        tx_id = f"tx{tx_index:07d}"
        stream = "s1" if tx_index % 2 else "s2"
        kind = rng.choices(
            ["update", "insert", "delete", "zero"],
            weights=[70, 15, 10, 5], k=1,
        )[0]
        # Only keys that exist in the starting snapshot are updated/deleted:
        # inserted keys stay live for the rest of the run but are never the
        # target of a later mutation in this bounded generator.
        original_keys = [k for k in live if not k.startswith("new")]
        if kind in ("update", "delete") and not original_keys:
            kind = "zero"
        if kind == "zero":
            emit(commit(stream, 0, tx_id, tx_index, 0, {}), stream)
            sealed = tx_index
            continue
        if kind == "insert":
            new_id = f"new{tx_index:07d}"
            after = {"key": new_id, "amount": rng.randrange(1000), "note": None}
            emit(change(stream, 0, tx_id, 1, "accounts", "insert",
                        {"key": new_id}, after=after), stream)
            emit(commit(stream, 0, tx_id, tx_index, 1, {"accounts": 1}), stream)
            live[new_id] = after
        elif kind == "delete" and original_keys:
            key_id = rng.choice(original_keys)
            emit(change(stream, 0, tx_id, 1, "accounts", "delete",
                        {"key": key_id},
                        before={"amount": live[key_id]["amount"]}), stream)
            emit(commit(stream, 0, tx_id, tx_index, 1, {"accounts": 1}), stream)
            live.pop(key_id)
            original_keys.remove(key_id)
        elif kind == "update" and original_keys:
            key_id = rng.choice(original_keys)
            cur = live[key_id]
            new_amount = cur["amount"] + rng.randrange(1, 9)
            event = change(stream, 0, tx_id, 1, "accounts", "update",
                           {"key": key_id},
                           before={"amount": cur["amount"]},
                           after={"amount": new_amount})
            # Retransmit the identical event at the SAME coordinate: a
            # physical duplicate that must merge without changing coverage.
            emit(event, stream)
            emit(json.loads(json.dumps(event)), stream)
            emit(commit(stream, 0, tx_id, tx_index, 1, {"accounts": 1}), stream)
            live[key_id] = dict(cur, amount=new_amount)
        else:
            # No live rows to mutate: fall back to a zero-event commit.
            emit(commit(stream, 0, tx_id, tx_index, 0, {}), stream)
        sealed = tx_index

    archive = out / "archive"
    archive.mkdir(exist_ok=True)
    manifest = emit_archive(
        archive, messages, shards=shards, seed=seed,
        sealed_commit_seq=sealed, snapshot_commit_seq=0,
    )
    return {"archive": str(archive), "manifest": manifest,
            "n_tx": n_tx, "n_keys": n_keys, "sealed": sealed}


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="output directory")
    parser.add_argument(
        "--flavor", default="hand",
        choices=("hand", "direct", "overwrite", "large"),
        help="hand = full cross-partition scenario; direct/overwrite = same "
             "final row, different histories; large = runtime scale archive",
    )
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--shards", type=int, default=3)
    parser.add_argument("--tx", type=int, default=2000)
    parser.add_argument("--keys", type=int, default=200)
    args = parser.parse_args(argv)
    if args.flavor == "large":
        info = generate_large(Path(args.out), args.tx, args.keys,
                              args.seed, args.shards)
        print(json.dumps({"flavor": "large", "archive": info["archive"],
                          "n_tx": info["n_tx"], "sealed": info["sealed"]},
                         sort_keys=True, indent=2))
    else:
        info = generate(Path(args.out), args.flavor, args.seed, args.shards)
        print(json.dumps({"flavor": info["flavor"],
                          "archive": info["archive"],
                          "files": info["manifest"]["files"]},
                         ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
