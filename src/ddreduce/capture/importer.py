"""Offline ``capture-import``: restore a capture archive into a log package.

The output package is the exact input shape already consumed by
``log-replay`` / ``log-reduce``::

    <out>/snapshot/<table>.jsonl   starting snapshot in the target schema
    <out>/schema.json              verbatim copy of the target schema
    <out>/changes.jsonl            full-image committed transactions
    <out>/capture-map.jsonl        per-event provenance back to the archive
    <out>/capture-report.json      counts, bindings, input/window and hashes

For a capture-v1 archive the snapshot is a verbatim copy.  For a capture-v2
(schema-evolving) archive it is the initial version's snapshot converted into
the unified target schema - renamed columns moved and later columns filled
from their declared backfill.

Only committed transactions with
``snapshot_commit_seq < commit_seq <= through`` are exported, ordered by
``commit_seq`` (never by transport position, file or tx_id).  Transactions
outside the window still participate in every archive completeness check,
including the capture-v2 commit-boundary check, but their row images never
touch the restored state of the export window.

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
from .errors import CaptureError, CaptureSchemaHistoryError
from .evolve import load_versioned_snapshot
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
    history_path: str | Path | None = None,
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
    _check_history_argument(manifest, history_path)
    _check_through(manifest, through)
    snapshot = _load_snapshot(manifest, schema, snapshot_dir)

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


def _check_history_argument(
    manifest: Manifest, history_path: str | Path | None
) -> None:
    """Bind the user-supplied schema history to the archive it claims.

    capture-v1 must not receive a history; capture-v2 must receive the exact
    history the archive was sealed with (byte content, not just its name).
    """
    if not manifest.evolving:
        if history_path is not None:
            raise CaptureSchemaHistoryError(
                "a schema history was supplied for a capture-v1 archive; "
                "capture-v1 archives have a single schema and carry no history"
            )
        return
    if history_path is None:
        raise CaptureSchemaHistoryError(
            "this archive declares an ordered schema history "
            f"({manifest.raw.get('schema_history')!r}); pass --schema-history "
            "with the same history it was sealed with"
        )
    from .history import load_schema_history

    supplied = load_schema_history(history_path)
    sealed = manifest.history
    assert sealed is not None
    if supplied.content_hash() != sealed.content_hash():
        raise CaptureSchemaHistoryError(
            f"supplied schema history {history_path} does not match the "
            f"archive's schema history (manifest history_hash "
            f"{manifest.raw['history_hash']}); refusing to reinterpret the "
            "archive against a different history"
        )


def _load_snapshot(
    manifest: Manifest, schema: Schema, snapshot_dir: Path
) -> Database:
    if manifest.evolving:
        from .history import SchemaHistory

        history: SchemaHistory = manifest.history  # type: ignore[assignment]
        return load_versioned_snapshot(history, schema, snapshot_dir)
    return Database.load(schema, snapshot_dir)


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


def check_commit_version_boundary(manifest: Manifest, store: CaptureStore) -> int:
    """Every committed transaction must be written wholly under the version in force.

    A schema switch happens at a commit boundary: a transaction whose commit
    point is at or after a version's ``effective_commit_seq`` must have been
    written with that version, and no transaction may mix versions.  This runs
    over *all* commits (inside and outside the export window) so a bad archive
    is never silently truncated into a "successful" package.
    """
    history = manifest.history
    if history is None:
        return 0
    checked = 0
    for end in store.committed_transactions():
        seq = end.commit_seq
        assert seq is not None
        events = store.logical_events(end.tx_id)
        declared = {e.schema_version for e in events}
        if not declared:
            continue  # zero-event commit: nothing to interpret
        if len(declared) > 1:
            raise CaptureSchemaHistoryError(
                f"tx {end.tx_id!r} (commit_seq {seq}): events declare schema "
                f"versions {sorted(declared)}; a transaction cannot straddle a "
                "schema switch",
                tx_id=end.tx_id, commit_seq=seq,
                declared_schema_versions=sorted(declared),
            )
        declared_version = next(iter(declared))
        expected = history.for_commit(seq)
        if declared_version != expected.version:
            raise CaptureSchemaHistoryError(
                f"tx {end.tx_id!r} (commit_seq {seq}): events were written with "
                f"schema version {declared_version}, but the version in force "
                f"at commit_seq {seq} is {expected.version} (effective at "
                f"commit_seq {expected.effective_commit_seq}); a schema switch "
                "only happens at a transaction commit boundary",
                tx_id=end.tx_id, commit_seq=seq,
                declared_schema_version=declared_version,
                expected_schema_version=expected.version,
                coordinates=[c.to_dict() for c in end.coordinates],
            )
        checked += 1
    return checked


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
    restorer = Restorer(
        manifest, schema, list(snapshot.rows.values()), history=manifest.history
    )
    checked_commits = check_commit_version_boundary(manifest, store)
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
                    "schema_version": src.schema_version,
                    "backfilled_columns": list(src.backfilled),
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
        "bindings": bindings_report(manifest),
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
            "commits_version_checked": checked_commits,
        },
        "output": {
            "directory": str(output_dir),
            "changes_hash": changes_hash,
            "capture_map_hash": map_hash,
        },
    }
    history_section = history_report_section(manifest, schema)
    if history_section is not None:
        report["schema_history"] = history_section["history"]
        report["target_schema"] = history_section["target"]
    return _WindowResult(report=report)


def bindings_report(manifest: Manifest) -> dict[str, Any]:
    """The declared source -> target bindings (v1) or history bindings (v2)."""
    if not manifest.evolving:
        return manifest.raw["bindings"]
    return {
        "tables": {
            b.source_table: {
                "target": b.target_table,
                "columns": dict(sorted(b.columns.items())),
            }
            for b in manifest.bindings.tables.values()
        }
    }


def history_report_section(
    manifest: Manifest, schema: Schema
) -> dict[str, Any] | None:
    """Report fields binding the schema history and unified target schema.

    capture-v1 archives have exactly one schema, already bound through
    ``inputs.schema_hash``; returning ``None`` keeps their report unchanged.
    """
    history = manifest.history
    if history is None:
        return None
    return {
        "history": {
            "file": manifest.raw["schema_history"],
            "history_hash": manifest.raw["history_hash"],
            "content_hash": history.content_hash(),
            "versions": [
                {
                    "version": v.version,
                    "effective_commit_seq": v.effective_commit_seq,
                    "tables": {
                        source_table: {
                            "target_table": table.target_table,
                            "primary_key": list(table.primary_key),
                            "columns": [
                                {
                                    "field_id": f.field_id,
                                    "name": f.name,
                                    "type": f.type,
                                    "nullable": f.nullable,
                                    **({"backfill": f.backfill}
                                       if f.has_backfill else {}),
                                }
                                for f in table.fields
                            ],
                            "unique_keys": [list(k) for k in table.unique_keys],
                            "foreign_keys": [
                                {
                                    "columns": list(fk["columns"]),
                                    "ref_table": fk["ref_table"],
                                    "ref_columns": list(fk["ref_columns"]),
                                    "nullable": fk["nullable"],
                                }
                                for fk in table.foreign_keys
                            ],
                        }
                        for source_table, table in v.tables.items()
                    },
                }
                for v in history.versions
            ],
            "switch_points": [
                {"version": version, "effective_commit_seq": seq}
                for version, seq in history.switch_points()
            ],
        },
        "target": {
            "schema_hash": hash_bytes(
                canonical_json(schema.canonical()).encode("utf-8")
            ),
            "tables": schema.canonical(),
        },
    }


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
