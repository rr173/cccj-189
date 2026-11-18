"""Offline ``capture-import``: restore a capture-v1 archive into a log package.

The output package is the exact input shape already consumed by
``log-replay`` / ``log-reduce``::

    <out>/snapshot/<table>.jsonl   verbatim copy of the starting snapshot
    <out>/schema.json              verbatim copy of the schema
    <out>/changes.jsonl            full-image committed transactions
    <out>/capture-map.jsonl        per-event provenance back to the archive
    <out>/capture-report.json      counts, bindings and input/window binding

Only committed transactions with
``snapshot_commit_seq < commit_seq <= through`` are exported, ordered by
``commit_seq`` (never by transport position, file or tx_id).  Transactions
outside the window still participate in every archive completeness check, but
their row images never touch the restored state of the export window.

The output directory is created atomically: everything is written into a
hidden staging sibling directory and renamed into place only after the whole
import has succeeded.  Any failure or interruption removes the staging
directory, and an existing output path is never overwritten.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..model import Database
from ..schema import Schema, load_schema
from ..values import canonical_json
from .errors import CaptureError
from .manifest import Manifest, load_manifest
from .restore import OutputEvent, Restorer
from .store import CaptureStore, Coordinate

PACKAGE_FILES = ("snapshot", "schema.json", "changes.jsonl",
                 "capture-map.jsonl", "capture-report.json")
REPORT_FORMAT = "capture-import-report-v1"
MAP_FORMAT = "capture-map-v1"
STAGING_PREFIX = ".capture-import-staging-"


# --------------------------------------------------------------------- hashes
def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def hash_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def archive_hash(manifest: Manifest, archive_dir: Path) -> str:
    """Hash binding the manifest and every declared shard (transport order)."""
    digest = hashlib.sha256()
    digest.update(manifest.canonical_bytes())
    for rel in manifest.files:
        digest.update(rel.encode("utf-8"))
        digest.update(hash_file(archive_dir / rel).encode("ascii"))
    return digest.hexdigest()


# --------------------------------------------------------------------- result
@dataclass
class ImportResult:
    report: dict[str, Any]
    output_dir: Path
    peak_spill_bytes: int


def _coords(coordinates: tuple[Coordinate, ...]) -> list[dict[str, Any]]:
    return [c.to_dict() for c in coordinates]


def _canonical_tx_line(tx_id: Any, events: list[OutputEvent]) -> str:
    """Serialize one restored transaction in the existing changes.jsonl format."""
    clean_events = []
    for ev in events:
        obj: dict[str, Any] = {"op": ev.op, "table": ev.table, "key": ev.key}
        if ev.before is not None:
            obj["before"] = ev.before
        if ev.after is not None:
            obj["after"] = ev.after
        clean_events.append(obj)
    return json.dumps(
        {"tx_id": tx_id, "events": clean_events},
        ensure_ascii=False, sort_keys=True,
    )


def run_import(
    archive: str | Path,
    snapshot_dir: str | Path,
    schema_path: str | Path,
    output_dir: str | Path,
    through: int,
    *,
    tmp_root: str | Path | None = None,
) -> ImportResult:
    archive = Path(archive)
    snapshot_dir = Path(snapshot_dir)
    schema_path = Path(schema_path)
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise CaptureError(f"output directory already exists; refusing to overwrite: {output_dir}")
    parent = output_dir.parent if str(output_dir.parent) not in ("", ".") else Path(".")
    parent.mkdir(parents=True, exist_ok=True)
    staging = parent / f"{STAGING_PREFIX}{output_dir.name}-{os.getpid()}"
    if staging.exists():
        shutil.rmtree(staging)

    schema = load_schema(schema_path)
    schema_text = schema_path.read_text(encoding="utf-8")
    manifest = load_manifest(archive, schema)
    _check_through(manifest, through)
    snapshot = Database.load(schema, snapshot_dir)

    started = time.monotonic()
    peak_spill = 0
    try:
        with CaptureStore(manifest, schema, archive, tmp_root=tmp_root) as store:
            store.scan()
            store.assemble()
            peak_spill = max(peak_spill, store.spill_bytes())
            result = _restore_window(
                manifest, schema, snapshot, store, through, staging,
                schema_path, schema_text, snapshot_dir, archive, output_dir,
            )
            peak_spill = max(peak_spill, store.spill_bytes())
        elapsed = time.monotonic() - started
        result.report["resources"] = {
            "peak_spill_bytes": peak_spill,
            "elapsed_seconds": round(elapsed, 6),
        }
        _write_report(staging, result.report)
        os.replace(staging, output_dir)
        return ImportResult(
            report=result.report,
            output_dir=output_dir,
            peak_spill_bytes=peak_spill,
        )
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _check_through(manifest: Manifest, through: int) -> None:
    if isinstance(through, bool) or not isinstance(through, int) or through < 0:
        raise CaptureError("--through must be a non-negative integer")
    if not (manifest.snapshot_commit_seq <= through <= manifest.sealed_commit_seq):
        raise CaptureError(
            f"--through {through} outside the archive window "
            f"[snapshot_commit_seq={manifest.snapshot_commit_seq}, "
            f"sealed_commit_seq={manifest.sealed_commit_seq}]"
        )


@dataclass
class _WindowResult:
    report: dict[str, Any]


def _restore_window(
    manifest: Manifest,
    schema: Schema,
    snapshot: Database,
    store: CaptureStore,
    through: int,
    staging: Path,
    schema_path: Path,
    schema_text: str,
    snapshot_dir: Path,
    archive: Path,
    output_dir: Path,
) -> _WindowResult:
    restorer = Restorer(manifest, schema, list(snapshot.rows.values()))
    commits = store.committed_transactions()  # ordered by commit_seq

    exported_tx = 0
    exported_events = 0
    key_move_expansions = 0
    in_window_zero_event = 0
    out_of_window = 0
    changes_lines: list[str] = []
    map_lines: list[str] = []

    # Output tx position is the changes.jsonl line index among exported txs.
    output_position = -1
    for end in commits:
        seq = end.commit_seq
        assert seq is not None
        in_window = manifest.snapshot_commit_seq < seq <= through
        events = store.logical_events(end.tx_id)
        if not in_window:
            out_of_window += 1
            # Out-of-window transactions still participate in every archive
            # completeness check above, but their row images never touch the
            # restored state and they produce no output or map entry.
            continue

        if not events:
            in_window_zero_event += 1
            # Zero-event commits produce no changes.jsonl line, so they do not
            # occupy an output transaction position; their end decision and
            # coordinates are still recorded in the map for traceability.
            map_lines.append(json.dumps({
                "format": MAP_FORMAT,
                "tx_id": end.tx_id,
                "commit_seq": seq,
                "zero_event_commit": True,
                "end_coordinates": _coords(end.coordinates),
            }, ensure_ascii=False, sort_keys=True))
            continue

        output_position += 1
        output_events = restorer.apply_transaction(end.tx_id, events)
        changes_lines.append(_canonical_tx_line(end.tx_id, output_events))
        exported_tx += 1
        exported_events += len(output_events)
        for ev_idx, output_event in enumerate(output_events):
            src = output_event.source
            map_lines.append(json.dumps({
                "format": MAP_FORMAT,
                "output_tx_position": output_position,
                "tx_id": end.tx_id,
                "commit_seq": seq,
                "output_event_index": ev_idx,
                "output_op": output_event.op,
                "output_table": output_event.table,
                "output_key": output_event.key,
                "source": {
                    "tx_id": src.tx_id,
                    "tx_order": src.tx_order,
                    "source_table": src.source_table,
                    "source_op": src.op,
                    "expanded_from_key_move": src.expanded_from_key_move,
                    "coordinates": _coords(src.coordinates),
                },
                "end_coordinates": _coords(end.coordinates),
            }, ensure_ascii=False, sort_keys=True))
            if src.expanded_from_key_move:
                key_move_expansions += 1

    # Aborted transactions never apply; their events were validated structurally
    # and counted during assembly.
    _materialize_package(
        staging, schema, schema_text, snapshot, changes_lines, map_lines
    )

    changes_hash = hash_bytes(
        ("".join(line + "\n" for line in changes_lines)).encode("utf-8")
    )
    map_hash = hash_bytes(
        ("".join(line + "\n" for line in map_lines)).encode("utf-8")
    )
    report = {
        "format": REPORT_FORMAT,
        "archive": {
            "path": str(archive),
            "source_id": manifest.source_id,
            "version": manifest.version,
            "archive_hash": archive_hash(manifest, archive),
            "manifest_hash": hash_bytes(manifest.canonical_bytes()),
            "files": list(manifest.files),
            "streams": [
                {
                    "name": s.name,
                    "first_offset": s.first_offset,
                    "last_offset": s.last_offset,
                }
                for s in manifest.streams
            ],
        },
        "inputs": {
            "snapshot_dir": str(snapshot_dir),
            "snapshot_hash": snapshot.content_hash(),
            "schema_path": str(schema_path),
            "schema_hash": hash_bytes(
                canonical_json(schema.canonical()).encode("utf-8")
            ),
        },
        "bindings": manifest.raw["bindings"],
        "window": {
            "snapshot_commit_seq": manifest.snapshot_commit_seq,
            "sealed_commit_seq": manifest.sealed_commit_seq,
            "through": through,
            "export_rule": "snapshot_commit_seq < commit_seq <= through",
        },
        "counts": {
            "files_read": store.metrics.files_read,
            "physical_messages_read": store.metrics.physical_messages_read,
            "physical_duplicates": store.metrics.physical_duplicates,
            "logical_event_duplicates": store.metrics.logical_event_duplicates,
            "end_duplicates": store.metrics.end_duplicates,
            "heartbeats": store.metrics.heartbeats,
            "transactions_total": store.assembly.transactions,
            "committed_total": store.assembly.committed,
            "aborted_total": store.assembly.aborted,
            "aborted_events": store.assembly.aborted_events,
            "zero_event_commits_total": store.assembly.zero_event_commits,
            "out_of_window_commits": out_of_window,
            "in_window_zero_event_commits": in_window_zero_event,
            "exported_transactions": exported_tx,
            "exported_events": exported_events,
            "key_move_expansions": key_move_expansions,
        },
        "output": {
            "directory": str(output_dir),
            "changes_hash": changes_hash,
            "capture_map_hash": map_hash,
        },
    }
    return _WindowResult(report=report)


def _materialize_package(
    staging: Path,
    schema: Schema,
    schema_text: str,
    snapshot: Database,
    changes_lines: list[str],
    map_lines: list[str],
) -> None:
    staging.mkdir(parents=True, exist_ok=False)
    snapshot.materialize(staging / "snapshot")
    with open(staging / "schema.json", "w", encoding="utf-8") as fh:
        fh.write(schema_text)
        if not schema_text.endswith("\n"):
            fh.write("\n")
    with open(staging / "changes.jsonl", "w", encoding="utf-8") as fh:
        for line in changes_lines:
            fh.write(line)
            fh.write("\n")
    with open(staging / "capture-map.jsonl", "w", encoding="utf-8") as fh:
        for line in map_lines:
            fh.write(line)
            fh.write("\n")


def _write_report(staging: Path, report: dict[str, Any]) -> None:
    # Fix the directory name: the report is written after the rename decision
    # but before os.replace, so record the final basename.
    with open(staging / "capture-report.json", "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, sort_keys=True, indent=2)
        fh.write("\n")
