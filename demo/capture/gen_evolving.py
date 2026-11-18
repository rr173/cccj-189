#!/usr/bin/env python3
"""Reproducible capture-v2 (schema-evolving) archive generator.

Builds the bounded hand-computed sample committed under
``demo/capture/evolving-sample`` plus larger archives that the resource tests
generate at runtime (never committed).

Hand scenario (``accounts``):

* version 1: field ``f-amount`` is named ``gross`` and typed ``int``;
* version 2 (effective at commit_seq 2): the same field ID is renamed to
  ``amount`` and widened to ``float``; a new field ``f-currency`` named
  ``currency`` is added with the explicit constant backfill ``"USD"``;
* commit 1 (version 1): ``gross`` 10 -> 20, one record updated before the
  switch;
* commit 2 (version 2): ``amount`` 20.0 -> 30.0, one record updated after it.

Expected import: both events are written to the target column ``amount``, the
old row and the updated row both carry ``currency="USD"``, and the numeric
value is unchanged (10/20/30, as floats).

Additional flavors:

* ``nameclash`` - a two-version archive where version 2 reuses the *name* of
  a dropped-looking column for a brand-new field ID.  A name-based converter
  would merge them; the field-ID based one keeps them apart (the old field is
  renamed, never merged).
* ``large`` - runtime scale archive with many transactions written across
  several schema versions, used by the resource test only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
from pathlib import Path

FORMAT = "capture-schema-history-v1"
SOURCE_ID = "src-evolving-1"
SNAPSHOT_COMMIT_SEQ = 0
SEALED_COMMIT_SEQ = 2


def canonical(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def history_hash(history: dict) -> str:
    return hashlib.sha256(canonical(history).encode("utf-8")).hexdigest()


def history_hand() -> dict:
    return {
        "format": FORMAT,
        "versions": [
            {
                "version": 1,
                "effective_commit_seq": 0,
                "tables": {
                    "accounts": {
                        "target_table": "accounts",
                        "primary_key": ["f-key"],
                        "columns": [
                            {"field_id": "f-key", "name": "key",
                             "type": "string", "nullable": False},
                            {"field_id": "f-amount", "name": "gross",
                             "type": "int", "nullable": False},
                        ],
                        "unique_keys": [],
                        "foreign_keys": [],
                    }
                },
            },
            {
                "version": 2,
                "effective_commit_seq": 2,
                "tables": {
                    "accounts": {
                        "target_table": "accounts",
                        "primary_key": ["f-key"],
                        "columns": [
                            {"field_id": "f-key", "name": "key",
                             "type": "string", "nullable": False},
                            {"field_id": "f-amount", "name": "amount",
                             "type": "float", "nullable": False},
                            {"field_id": "f-currency", "name": "currency",
                             "type": "string", "nullable": False,
                             "backfill": "USD"},
                        ],
                        "unique_keys": [],
                        "foreign_keys": [],
                    }
                },
            },
        ],
    }


def target_schema_hand() -> dict:
    return {
        "tables": [{
            "name": "accounts",
            "columns": [
                {"name": "key", "type": "string", "nullable": False},
                {"name": "amount", "type": "float", "nullable": False},
                {"name": "currency", "type": "string", "nullable": False},
            ],
            "primary_key": ["key"],
        }]
    }


def snapshot_hand() -> dict[str, list[dict]]:
    return {"accounts": [{"key": "k", "gross": 10}]}


def hand_messages() -> list[dict]:
    def change(offset, tx_id, order, key, *, before, after, version):
        return {
            "source_id": SOURCE_ID, "stream": "s1", "offset": offset,
            "kind": "change", "tx_id": tx_id, "tx_order": order,
            "table": "accounts", "op": "update", "key": key,
            "before": before, "after": after, "schema_version": version,
        }

    def commit(offset, tx_id, seq):
        return {
            "source_id": SOURCE_ID, "stream": "s1", "offset": offset,
            "kind": "commit", "tx_id": tx_id, "commit_seq": seq,
            "event_count": 1, "table_counts": {"accounts": 1},
        }

    return [
        change(0, "A", 1, {"key": "k"}, before={"gross": 10},
               after={"gross": 20}, version=1),
        commit(1, "A", 1),
        change(2, "B", 1, {"key": "k"}, before={"amount": 20.0},
               after={"amount": 30.0}, version=2),
        commit(3, "B", 2),
    ]


def nameclash_history() -> dict:
    """v2 renames ``amount`` to ``amount_legacy``; a *new* field takes ``amount``."""
    return {
        "format": FORMAT,
        "versions": [
            {"version": 1, "effective_commit_seq": 0, "tables": {
                "accounts": {"target_table": "accounts",
                             "primary_key": ["f-key"],
                             "columns": [
                                 {"field_id": "f-key", "name": "key",
                                  "type": "string", "nullable": False},
                                 {"field_id": "f-legacy", "name": "amount",
                                  "type": "int", "nullable": False}],
                             "unique_keys": [], "foreign_keys": []}}},
            {"version": 2, "effective_commit_seq": 2, "tables": {
                "accounts": {"target_table": "accounts",
                             "primary_key": ["f-key"],
                             "columns": [
                                 {"field_id": "f-key", "name": "key",
                                  "type": "string", "nullable": False},
                                 {"field_id": "f-legacy",
                                  "name": "amount_legacy", "type": "int",
                                  "nullable": False},
                                 {"field_id": "f-amount", "name": "amount",
                                  "type": "float", "nullable": False,
                                  "backfill": 0}],
                             "unique_keys": [], "foreign_keys": []}}},
        ],
    }


def nameclash_schema() -> dict:
    return {"tables": [{"name": "accounts", "columns": [
        {"name": "key", "type": "string", "nullable": False},
        {"name": "amount_legacy", "type": "int", "nullable": False},
        {"name": "amount", "type": "float", "nullable": False},
    ], "primary_key": ["key"]}]}


def nameclash_messages() -> list[dict]:
    return [{
        "source_id": SOURCE_ID, "stream": "s1", "offset": 0,
        "kind": "change", "tx_id": "N", "tx_order": 1,
        "table": "accounts", "op": "update", "key": {"key": "k"},
        "before": {"amount": 7}, "after": {"amount": 9},
        "schema_version": 1,
    }, {
        "source_id": SOURCE_ID, "stream": "s1", "offset": 1,
        "kind": "commit", "tx_id": "N", "commit_seq": 1,
        "event_count": 1, "table_counts": {"accounts": 1},
    }]


def emit(out: Path, messages: list[dict], *, history: dict, schema: dict,
         snapshot: dict[str, list[dict]], sealed: int, shards: int = 2,
         seed: int = 7, snapshot_commit_seq: int = 0) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    (out / "schema.json").write_text(
        json.dumps(schema, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    snap_dir = out / "snapshot"
    snap_dir.mkdir(exist_ok=True)
    for table, rows in snapshot.items():
        (snap_dir / f"{table}.jsonl").write_text(
            "".join(json.dumps(r, sort_keys=True) + "\n" for r in rows),
            encoding="utf-8",
        )
    archive = out / "archive"
    archive.mkdir(exist_ok=True)
    (archive / "shards").mkdir(exist_ok=True)
    (archive / "schema-history.json").write_text(
        json.dumps(history, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    rng = random.Random(seed)
    per_stream: dict[str, list[dict]] = {}
    for msg in messages:
        per_stream.setdefault(msg["stream"], []).append(msg)
    shards = max(1, shards)
    files = []
    for stream in sorted(per_stream):
        owned = [json.loads(json.dumps(m)) for m in per_stream[stream]]
        rng.shuffle(owned)
        for i in range(shards):
            chunk = owned[i::shards]
            if not chunk:
                continue
            rel = f"shards/{stream}-{i:02d}.jsonl"
            files.append(rel)
            (archive / rel).write_text(
                "".join(json.dumps(m, ensure_ascii=False, sort_keys=True) + "\n"
                        for m in chunk),
                encoding="utf-8",
            )
    manifest = {
        "version": 2,
        "source_id": SOURCE_ID,
        "snapshot_commit_seq": snapshot_commit_seq,
        "sealed_commit_seq": sealed,
        "streams": [
            {"name": name, "first_offset": 0,
             "last_offset": max(m["offset"] for m in messages
                                if m["stream"] == name)}
            for name in sorted({m["stream"] for m in messages})
        ],
        "files": sorted(files),
        "schema_history": "schema-history.json",
        "history_hash": history_hash(history),
    }
    (archive / "capture.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def generate_hand(out: Path, seed: int, shards: int) -> dict:
    return emit(out, hand_messages(), history=history_hand(),
                schema=target_schema_hand(), snapshot=snapshot_hand(),
                sealed=SEALED_COMMIT_SEQ, shards=shards, seed=seed)


def generate_nameclash(out: Path, seed: int, shards: int) -> dict:
    return emit(out, nameclash_messages(), history=nameclash_history(),
                schema=nameclash_schema(),
                snapshot={"accounts": [{"key": "k", "amount": 7}]},
                sealed=2, shards=shards, seed=seed)


def large_history(switch_points: tuple[int, ...]) -> dict:
    """A v1->v2->v3 history over ``accounts(key, amount, currency[, region])``."""
    versions = [
        {"version": 1, "effective_commit_seq": 0, "tables": {
            "accounts": {"target_table": "accounts", "primary_key": ["f-key"],
                         "columns": [
                             {"field_id": "f-key", "name": "key",
                              "type": "string", "nullable": False},
                             {"field_id": "f-amount", "name": "gross",
                              "type": "int", "nullable": False}],
                         "unique_keys": [], "foreign_keys": []}}},
        {"version": 2, "effective_commit_seq": switch_points[0], "tables": {
            "accounts": {"target_table": "accounts", "primary_key": ["f-key"],
                         "columns": [
                             {"field_id": "f-key", "name": "key",
                              "type": "string", "nullable": False},
                             {"field_id": "f-amount", "name": "amount",
                              "type": "float", "nullable": False},
                             {"field_id": "f-currency", "name": "currency",
                              "type": "string", "nullable": False,
                              "backfill": "USD"}],
                         "unique_keys": [], "foreign_keys": []}}},
        {"version": 3, "effective_commit_seq": switch_points[1], "tables": {
            "accounts": {"target_table": "accounts", "primary_key": ["f-key"],
                         "columns": [
                             {"field_id": "f-key", "name": "key",
                              "type": "string", "nullable": False},
                             {"field_id": "f-amount", "name": "amount",
                              "type": "float", "nullable": False},
                             {"field_id": "f-currency", "name": "currency",
                              "type": "string", "nullable": False,
                              "backfill": "USD"},
                             {"field_id": "f-region", "name": "region",
                              "type": "string", "nullable": True}],
                         "unique_keys": [], "foreign_keys": []}}},
    ]
    return {"format": FORMAT, "versions": versions}


def large_schema() -> dict:
    return {"tables": [{"name": "accounts", "columns": [
        {"name": "key", "type": "string", "nullable": False},
        {"name": "amount", "type": "float", "nullable": False},
        {"name": "currency", "type": "string", "nullable": False},
        {"name": "region", "type": "string", "nullable": True},
    ], "primary_key": ["key"]}]}


def generate_large(out: Path, n_tx: int, n_keys: int, seed: int,
                   shards: int = 4) -> dict:
    """Runtime scale archive spanning three schema versions."""
    rng = random.Random(seed)
    switch1 = max(1, n_tx // 3)
    switch2 = max(switch1 + 1, (2 * n_tx) // 3)
    history = large_history((switch1, switch2))
    live = {f"k{i:06d}": {"key": f"k{i:06d}", "gross": i + 1}
            for i in range(n_keys)}
    messages: list[dict] = []
    cursor = {"s1": 0, "s2": 0}
    for i in range(1, n_tx + 1):
        stream = "s1" if i % 2 else "s2"
        key = f"k{rng.randrange(n_keys):06d}"
        version = 1 if i < switch1 else (2 if i < switch2 else 3)
        if version == 1:
            before = {"gross": live[key]["gross"]}
            after = {"gross": live[key]["gross"] + rng.randrange(1, 5)}
            live[key]["gross"] = after["gross"]
        else:
            before = {"amount": float(live[key]["gross"])}
            after = {"amount": float(live[key]["gross"]) + rng.randrange(1, 5)}
            live[key]["gross"] = after["amount"]
            if version == 3 and rng.random() < 0.5:
                after["region"] = rng.choice(["eu", "us"])
        messages.append({
            "source_id": SOURCE_ID, "stream": stream,
            "offset": cursor[stream], "kind": "change",
            "tx_id": f"tx{i:07d}", "tx_order": 1, "table": "accounts",
            "op": "update", "key": {"key": key}, "before": before,
            "after": after, "schema_version": version,
        })
        cursor[stream] += 1
        messages.append({
            "source_id": SOURCE_ID, "stream": stream,
            "offset": cursor[stream], "kind": "commit",
            "tx_id": f"tx{i:07d}", "commit_seq": i, "event_count": 1,
            "table_counts": {"accounts": 1},
        })
        cursor[stream] += 1
    snapshot = {"accounts": [{"key": k, "gross": v["gross"]}
                             for k, v in live.items()]}
    # The runtime snapshot must be the *initial* state; reset amounts to the
    # starting values the version-1 events will read.
    start = {f"k{i:06d}": i + 1 for i in range(n_keys)}
    snapshot = {"accounts": [{"key": k, "gross": start[k]}
                             for k in sorted(start)]}
    manifest = emit(out, messages, history=history, schema=large_schema(),
                    snapshot=snapshot, sealed=n_tx, shards=shards, seed=seed)
    return {"archive": str(out / "archive"), "manifest": manifest,
            "switch_points": [switch1, switch2], "sealed": n_tx}


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    parser.add_argument("--flavor", default="hand",
                        choices=("hand", "nameclash", "large"))
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--shards", type=int, default=2)
    parser.add_argument("--tx", type=int, default=600)
    parser.add_argument("--keys", type=int, default=60)
    args = parser.parse_args(argv)
    out = Path(args.out)
    if args.flavor == "large":
        info = generate_large(out, args.tx, args.keys, args.seed, args.shards)
        print(json.dumps(info, sort_keys=True, indent=2))
    elif args.flavor == "nameclash":
        manifest = generate_nameclash(out, args.seed, args.shards)
        print(json.dumps({"archive": str(out / "archive"),
                          "files": manifest["files"]}, sort_keys=True, indent=2))
    else:
        manifest = generate_hand(out, args.seed, args.shards)
        print(json.dumps({"archive": str(out / "archive"),
                          "files": manifest["files"]}, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
