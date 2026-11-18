"""Helpers for building capture-v1 archives in tests."""

from __future__ import annotations

import json
from pathlib import Path

from tests.helpers import write_json, write_jsonl

DEFAULT_SOURCE_ID = "src-test-1"


def identity_bindings(schema: dict) -> dict:
    return {
        "tables": {
            t["name"]: {
                "target": t["name"],
                "columns": {c["name"]: c["name"] for c in t["columns"]},
            }
            for t in schema["tables"]
        }
    }


def envelope(stream: str, offset: int, kind: str, source_id: str = DEFAULT_SOURCE_ID) -> dict:
    return {"source_id": source_id, "stream": stream, "offset": offset, "kind": kind}


def change(stream, offset, tx_id, tx_order, table, op, key, *,
           before=None, after=None, new_key=None,
           source_id=DEFAULT_SOURCE_ID) -> dict:
    msg = envelope(stream, offset, "change", source_id)
    msg.update({"tx_id": tx_id, "tx_order": tx_order, "table": table,
                "op": op, "key": key})
    if before is not None:
        msg["before"] = before
    if after is not None:
        msg["after"] = after
    if new_key is not None:
        msg["new_key"] = new_key
    return msg


def commit(stream, offset, tx_id, commit_seq, event_count, table_counts,
           source_id=DEFAULT_SOURCE_ID) -> dict:
    msg = envelope(stream, offset, "commit", source_id)
    msg.update({"tx_id": tx_id, "commit_seq": commit_seq,
                "event_count": event_count, "table_counts": table_counts})
    return msg


def abort(stream, offset, tx_id, event_count, table_counts,
          source_id=DEFAULT_SOURCE_ID) -> dict:
    msg = envelope(stream, offset, "abort", source_id)
    msg.update({"tx_id": tx_id, "event_count": event_count,
                "table_counts": table_counts})
    return msg


def heartbeat(stream, offset, source_id=DEFAULT_SOURCE_ID) -> dict:
    return envelope(stream, offset, "heartbeat", source_id)


def write_archive(
    base: Path,
    messages: list[dict],
    *,
    schema: dict,
    snapshot: dict[str, list[dict]],
    files: list[str] | None = None,
    streams: list[dict] | None = None,
    snapshot_commit_seq: int = 0,
    sealed_commit_seq: int | None = None,
    bindings: dict | None = None,
    source_id: str = DEFAULT_SOURCE_ID,
    manifest_overrides: dict | None = None,
) -> tuple[Path, Path, Path]:
    """Write archive + snapshot + schema; return (archive, snapshot, schema) paths.

    ``messages`` are split into per-stream shard files using each message's
    own stream/offset coordinates.
    """
    schema_path = write_json(base / "schema.json", schema)
    snapshot_dir = base / "snapshot"
    for table, rows in snapshot.items():
        write_jsonl(snapshot_dir / f"{table}.jsonl", rows)
    archive = base / "archive"
    archive.mkdir(parents=True, exist_ok=True)

    per_stream: dict[str, list[dict]] = {}
    for msg in messages:
        per_stream.setdefault(msg["stream"], []).append(msg)
    declared_files: list[str] = []
    computed_streams: list[dict] = []
    for stream_name in sorted(per_stream):
        msgs = sorted(per_stream[stream_name], key=lambda m: m["offset"])
        offsets = sorted({m["offset"] for m in msgs})
        rel = f"shards/{stream_name}.jsonl"
        declared_files.append(rel)
        write_jsonl(archive / rel, msgs)
        computed_streams.append({
            "name": stream_name,
            "first_offset": offsets[0],
            "last_offset": offsets[-1],
        })
    if files is None:
        files = declared_files
    if streams is None:
        streams = computed_streams
    max_seq = sealed_commit_seq
    if max_seq is None:
        max_seq = max(
            (m["commit_seq"] for m in messages if m["kind"] == "commit"),
            default=snapshot_commit_seq,
        )
    manifest = {
        "version": 1,
        "source_id": source_id,
        "snapshot_commit_seq": snapshot_commit_seq,
        "sealed_commit_seq": max_seq,
        "streams": streams,
        "files": files,
        "bindings": bindings or identity_bindings(schema),
    }
    if manifest_overrides:
        manifest.update(manifest_overrides)
    write_json(archive / "capture.json", manifest)
    return archive, snapshot_dir, schema_path


# --------------------------------------------------------------- capture-v2
HISTORY_FORMAT = "capture-schema-history-v1"


def field(field_id: str, name: str, type_: str, nullable: bool, **extra) -> dict:
    out = {"field_id": field_id, "name": name, "type": type_, "nullable": nullable}
    out.update(extra)
    return out


def v2_change(stream, offset, tx_id, tx_order, table, op, key, *, schema_version,
              before=None, after=None, new_key=None,
              source_id=DEFAULT_SOURCE_ID) -> dict:
    msg = change(stream, offset, tx_id, tx_order, table, op, key,
                 before=before, after=after, new_key=new_key,
                 source_id=source_id)
    msg["schema_version"] = schema_version
    return msg


def history_hash(history: dict) -> str:
    import hashlib

    from ddreduce.values import canonical_json

    return hashlib.sha256(canonical_json(history).encode("utf-8")).hexdigest()


def write_v2_archive(
    base: Path,
    messages: list[dict],
    *,
    history: dict,
    schema: dict,
    snapshot_rows: dict[str, list[dict]],
    snapshot_commit_seq: int = 0,
    sealed_commit_seq: int | None = None,
    source_id: str = DEFAULT_SOURCE_ID,
    manifest_overrides: dict | None = None,
    history_override: dict | None = None,
    history_hash_override: str | None = None,
) -> tuple[Path, Path, Path, Path]:
    """Write a capture-v2 archive (with its history) + snapshot + target schema.

    Returns ``(archive, snapshot, schema_path, history_path)``.  The snapshot
    rows are written under the *initial* version's historical column names.
    """
    schema_path = write_json(base / "schema.json", schema)
    snapshot_dir = base / "snapshot"
    for table, rows in snapshot_rows.items():
        write_jsonl(snapshot_dir / f"{table}.jsonl", rows)
    archive = base / "archive"
    archive.mkdir(parents=True, exist_ok=True)
    history_path = write_json(archive / "schema-history.json", history)

    per_stream: dict[str, list[dict]] = {}
    for msg in messages:
        per_stream.setdefault(msg["stream"], []).append(msg)
    declared_files: list[str] = []
    streams: list[dict] = []
    for stream_name in sorted(per_stream):
        msgs = sorted(per_stream[stream_name], key=lambda m: m["offset"])
        offsets = sorted({m["offset"] for m in msgs})
        rel = f"shards/{stream_name}.jsonl"
        declared_files.append(rel)
        write_jsonl(archive / rel, msgs)
        streams.append({
            "name": stream_name,
            "first_offset": offsets[0],
            "last_offset": offsets[-1],
        })
    max_seq = sealed_commit_seq
    if max_seq is None:
        max_seq = max(
            (m["commit_seq"] for m in messages if m["kind"] == "commit"),
            default=snapshot_commit_seq,
        )
    manifest = {
        "version": 2,
        "source_id": source_id,
        "snapshot_commit_seq": snapshot_commit_seq,
        "sealed_commit_seq": max_seq,
        "streams": streams,
        "files": declared_files,
        "schema_history": "schema-history.json",
        "history_hash": history_hash_override or history_hash(
            history_override or history
        ),
    }
    if manifest_overrides:
        manifest.update(manifest_overrides)
    write_json(archive / "capture.json", manifest)
    return archive, snapshot_dir, schema_path, history_path


def two_version_history(
    *,
    target_table: str = "accounts",
    key_field: str = "f-key",
    key_name: str = "key",
    amount_field: str = "f-amount",
    old_name: str = "gross",
    old_type: str = "int",
    new_name: str = "amount",
    new_type: str = "float",
    currency_field: str = "f-currency",
    currency_name: str = "currency",
    currency_backfill: object = "USD",
    switch_at: int = 2,
    include_currency: bool = True,
    v2_override: dict | None = None,
) -> dict:
    """The canonical two-version history used by the hand-computed tests."""
    base_columns = [
        field(key_field, key_name, "string", False),
        field(amount_field, old_name, old_type, False),
    ]
    new_columns = [
        field(key_field, key_name, "string", False),
        field(amount_field, new_name, new_type, False),
    ]
    if include_currency:
        new_columns.append(
            field(currency_field, currency_name, "string", False,
                  backfill=currency_backfill)
        )
    v2_table = {
        "target_table": target_table,
        "primary_key": [key_field],
        "columns": new_columns,
        "unique_keys": [],
        "foreign_keys": [],
    }
    if v2_override:
        v2_table.update(v2_override)
    return {
        "format": HISTORY_FORMAT,
        "versions": [
            {
                "version": 1,
                "effective_commit_seq": 0,
                "tables": {
                    target_table: {
                        "target_table": target_table,
                        "primary_key": [key_field],
                        "columns": base_columns,
                        "unique_keys": [],
                        "foreign_keys": [],
                    }
                },
            },
            {
                "version": 2,
                "effective_commit_seq": switch_at,
                "tables": {target_table: v2_table},
            },
        ],
    }


def target_accounts_schema(
    *,
    table: str = "accounts",
    key: str = "key",
    amount: str = "amount",
    amount_type: str = "float",
    currency: str = "currency",
    include_currency: bool = True,
) -> dict:
    columns = [
        {"name": key, "type": "string", "nullable": False},
        {"name": amount, "type": amount_type, "nullable": False},
    ]
    if include_currency:
        columns.append({"name": currency, "type": "string", "nullable": False})
    return {
        "tables": [{
            "name": table,
            "columns": columns,
            "primary_key": [key],
        }]
    }
