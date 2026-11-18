"""capture-v1 manifest (``capture.json``) parsing and validation.

A capture archive is a plain directory containing one ``capture.json``
manifest plus the JSONL shard files it declares.  The manifest shape is::

    {
      "version": 1,
      "source_id": <non-empty string>,
      "snapshot_commit_seq": <non-negative int>,
      "sealed_commit_seq": <non-negative int>,
      "streams": [
        {"name": <non-empty string>, "first_offset": <int>=0,
         "last_offset": <int> >= first_offset}, ...
      ],
      "files": ["rel/path/a.jsonl", ...],
      "bindings": {
        "tables": {
          <source table>: {"target": <target table>,
                           "columns": {<source column>: <target column>, ...}}
        }
      }
    }

Bindings are bijections: every target schema table and every target column
must be covered exactly once, with no filtering and no implicit column drops.

Only regular files with safe, non-escaping relative paths are accepted;
symlinks, duplicates, absolute paths and ``..`` traversal are rejected before
any message is read.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..schema import Schema
from ..values import canonical_json
from .errors import CaptureArchiveError, CaptureSchemaHistoryError

FORMAT_VERSION = 1
SCHEMA_EVOLVING_FORMAT_VERSION = 2
MANIFEST_NAME = "capture.json"
HISTORY_NAME = "schema-history.json"


@dataclass(frozen=True)
class StreamSpec:
    name: str
    first_offset: int
    last_offset: int

    def size(self) -> int:
        return self.last_offset - self.first_offset + 1


@dataclass(frozen=True)
class TableBinding:
    source_table: str
    target_table: str
    # source column -> target column
    columns: dict[str, str]

    @property
    def target_to_source_columns(self) -> dict[str, str]:
        return {t: s for s, t in self.columns.items()}


@dataclass(frozen=True)
class Bindings:
    tables: dict[str, TableBinding]

    def target_table(self, source_table: str) -> str:
        return self.tables[source_table].target_table

    def target_columns(self, source_table: str) -> dict[str, str]:
        return self.tables[source_table].columns


@dataclass(frozen=True)
class Manifest:
    version: int
    source_id: str
    snapshot_commit_seq: int
    sealed_commit_seq: int
    streams: tuple[StreamSpec, ...]
    files: tuple[str, ...]
    bindings: Bindings
    raw: dict
    archive_dir: str = ""
    history: Any = None  # SchemaHistory for capture-v2, None for capture-v1

    def stream(self, name: str) -> StreamSpec | None:
        for spec in self.streams:
            if spec.name == name:
                return spec
        return None

    def canonical_bytes(self) -> bytes:
        """Deterministic serialization of the manifest content (for hashing)."""
        return canonical_json(self.raw).encode("utf-8")

    @property
    def evolving(self) -> bool:
        """True for capture-v2 archives carrying an ordered schema history."""
        return self.version == SCHEMA_EVOLVING_FORMAT_VERSION

    def history_path(self) -> Path | None:
        """Path of the declared history file (capture-v2 only)."""
        if not self.evolving or not self.archive_dir:
            return None
        return Path(self.archive_dir) / HISTORY_NAME


def _is_nonneg_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _require_fields(obj: dict, fields: tuple[str, ...], where: str) -> None:
    missing = [f for f in fields if f not in obj]
    if missing:
        raise CaptureArchiveError(f"{where}: missing fields {sorted(missing)}")


def safe_relative_path(rel: object) -> str:
    """Validate an archive-relative path: no escape, no absolute form."""
    where = "manifest files"
    if not isinstance(rel, str) or not rel:
        raise CaptureArchiveError(f"{where}: file paths must be non-empty strings")
    if rel.startswith("/") or rel.startswith("\\"):
        raise CaptureArchiveError(f"{where}: absolute path not allowed: {rel!r}")
    # Windows drive prefixes are rejected as well.
    if len(rel) >= 2 and rel[1] == ":":
        raise CaptureArchiveError(f"{where}: drive-qualified path not allowed: {rel!r}")
    normalized = os.path.normpath(rel)
    if normalized == "." or normalized.startswith(".." + os.sep) or normalized == ".." or os.path.isabs(normalized):
        raise CaptureArchiveError(f"{where}: path escapes archive: {rel!r}")
    if os.sep != "/":
        parts = rel.replace("\\", "/").split("/")
    else:
        parts = rel.split("/")
    if any(part in ("", "..") for part in parts):
        raise CaptureArchiveError(f"{where}: unsafe path component in {rel!r}")
    # Reject symlink-ish reserved name only later during the filesystem check;
    # the manifest itself must not point at the manifest file.
    if normalized == MANIFEST_NAME:
        raise CaptureArchiveError(f"{where}: manifest itself is not a shard file: {rel!r}")
    if normalized == HISTORY_NAME:
        raise CaptureArchiveError(
            f"{where}: schema history is not a shard file: {rel!r}"
        )
    return normalized.replace(os.sep, "/")


def load_manifest(archive_dir: str | Path, schema: Schema) -> Manifest:
    """Read and fully validate ``capture.json`` against the target schema."""
    archive = Path(archive_dir)
    manifest_path = archive / MANIFEST_NAME
    if not manifest_path.is_file():
        raise CaptureArchiveError(f"manifest not found: {manifest_path}")
    try:
        with open(manifest_path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except json.JSONDecodeError as exc:
        raise CaptureArchiveError(f"{MANIFEST_NAME}: invalid JSON ({exc})") from exc
    if not isinstance(raw, dict):
        raise CaptureArchiveError(f"{MANIFEST_NAME}: manifest must be a JSON object")

    where = MANIFEST_NAME
    if "version" not in raw:
        raise CaptureArchiveError(f"{where}: missing fields ['version']")
    if raw["version"] not in (FORMAT_VERSION, SCHEMA_EVOLVING_FORMAT_VERSION):
        raise CaptureArchiveError(
            f"{where}: unsupported capture version {raw['version']!r}; "
            f"supported versions are {FORMAT_VERSION} (single schema) and "
            f"{SCHEMA_EVOLVING_FORMAT_VERSION} (ordered schema history)"
        )
    if raw["version"] == SCHEMA_EVOLVING_FORMAT_VERSION:
        return _load_evolving_manifest(archive, raw, schema, where)
    _require_fields(
        raw,
        ("version", "source_id", "snapshot_commit_seq", "sealed_commit_seq",
         "streams", "files", "bindings"),
        where,
    )
    unknown = set(raw) - {
        "version", "source_id", "snapshot_commit_seq", "sealed_commit_seq",
        "streams", "files", "bindings",
    }
    if unknown:
        raise CaptureArchiveError(f"{where}: unknown manifest fields {sorted(unknown)}")
    source_id = raw["source_id"]
    if not isinstance(source_id, str) or not source_id:
        raise CaptureArchiveError(f"{where}: source_id must be a non-empty string")

    for field_name in ("snapshot_commit_seq", "sealed_commit_seq"):
        if not _is_nonneg_int(raw[field_name]):
            raise CaptureArchiveError(
                f"{where}: {field_name} must be a non-negative integer "
                f"(booleans are not accepted)"
            )
    if raw["sealed_commit_seq"] < raw["snapshot_commit_seq"]:
        raise CaptureArchiveError(
            f"{where}: sealed_commit_seq ({raw['sealed_commit_seq']}) must be >= "
            f"snapshot_commit_seq ({raw['snapshot_commit_seq']})"
        )

    streams = _parse_streams(raw["streams"], where)
    files = _parse_files(archive, raw["files"], where, allow=(MANIFEST_NAME,))
    bindings = _parse_bindings(raw["bindings"], schema, where)
    return Manifest(
        version=FORMAT_VERSION,
        source_id=source_id,
        snapshot_commit_seq=raw["snapshot_commit_seq"],
        sealed_commit_seq=raw["sealed_commit_seq"],
        streams=streams,
        files=files,
        bindings=bindings,
        raw=raw,
        archive_dir=str(archive),
    )


def _load_evolving_manifest(
    archive: Path, raw: dict, schema: Schema, where: str
) -> "Manifest":
    """capture-v2: same transport envelope plus a declared schema history.

    ``bindings`` is replaced by ``schema_history`` (the history file carries
    every version's table/column/field-ID binding).  ``history_hash`` pins the
    exact history content so an archive can never be read against a different
    history than the one it was sealed with.
    """
    expected = {
        "version", "source_id", "snapshot_commit_seq", "sealed_commit_seq",
        "streams", "files", "schema_history", "history_hash",
    }
    unknown = set(raw) - expected
    missing = expected - set(raw)
    if unknown or missing:
        raise CaptureArchiveError(
            f"{where}: unsupported capture version {SCHEMA_EVOLVING_FORMAT_VERSION} "
            "manifest shape - capture version 2 requires an ordered schema "
            f"history and drops the single-schema bindings; missing fields "
            f"{sorted(missing)}, unknown manifest fields {sorted(unknown)}"
        )
    source_id = raw["source_id"]
    if not isinstance(source_id, str) or not source_id:
        raise CaptureArchiveError(f"{where}: source_id must be a non-empty string")
    for field_name in ("snapshot_commit_seq", "sealed_commit_seq"):
        if not _is_nonneg_int(raw[field_name]):
            raise CaptureArchiveError(
                f"{where}: {field_name} must be a non-negative integer "
                f"(booleans are not accepted)"
            )
    if raw["sealed_commit_seq"] < raw["snapshot_commit_seq"]:
        raise CaptureArchiveError(
            f"{where}: sealed_commit_seq ({raw['sealed_commit_seq']}) must be >= "
            f"snapshot_commit_seq ({raw['snapshot_commit_seq']})"
        )
    if raw["schema_history"] != HISTORY_NAME:
        raise CaptureArchiveError(
            f"{where}: schema_history must be {HISTORY_NAME!r}, got "
            f"{raw['schema_history']!r}"
        )
    history_hash = raw["history_hash"]
    if not isinstance(history_hash, str) or len(history_hash) != 64:
        raise CaptureArchiveError(
            f"{where}: history_hash must be a 64-character hex SHA-256 digest"
        )
    try:
        int(history_hash, 16)
    except ValueError:
        raise CaptureArchiveError(
            f"{where}: history_hash must be a 64-character hex SHA-256 digest"
        ) from None
    streams = _parse_streams(raw["streams"], where)
    files = _parse_files(archive, raw["files"], where,
                         allow=(MANIFEST_NAME, HISTORY_NAME))
    history_path = archive / HISTORY_NAME
    if not history_path.is_file():
        raise CaptureArchiveError(
            f"{where}: schema history file not found: {HISTORY_NAME}"
        )
    if history_path.is_symlink():
        raise CaptureArchiveError(
            f"{where}: symlinks are not allowed: {HISTORY_NAME}"
        )
    from .history import load_schema_history, validate_against_target
    from .importer import hash_bytes

    history = load_schema_history(history_path)
    actual = hash_bytes(history.canonical_bytes())
    if actual != history_hash:
        raise CaptureArchiveError(
            f"{where}: history_hash does not match {HISTORY_NAME} content "
            f"(manifest {history_hash}, file {actual}); the archive references "
            "a different schema history than it was sealed with"
        )
    if history.first().effective_commit_seq > raw["snapshot_commit_seq"]:
        raise CaptureSchemaHistoryError(
            f"{where}: schema version {history.first().version} becomes "
            f"effective at commit_seq {history.first().effective_commit_seq}, "
            f"after snapshot_commit_seq {raw['snapshot_commit_seq']}; the "
            "snapshot cannot be interpreted and version 1 must be initial"
        )
    for version in history.versions[1:]:
        if version.effective_commit_seq > raw["sealed_commit_seq"]:
            raise CaptureSchemaHistoryError(
                f"{where}: schema version {version.version} becomes effective at "
                f"commit_seq {version.effective_commit_seq}, beyond "
                f"sealed_commit_seq {raw['sealed_commit_seq']}"
            )
    validate_against_target(history, schema, raw)
    return Manifest(
        version=SCHEMA_EVOLVING_FORMAT_VERSION,
        source_id=source_id,
        snapshot_commit_seq=raw["snapshot_commit_seq"],
        sealed_commit_seq=raw["sealed_commit_seq"],
        streams=streams,
        files=files,
        bindings=_history_bindings(history),
        raw=raw,
        archive_dir=str(archive),
        history=history,
    )


def _history_bindings(history) -> Bindings:
    """A compatibility ``Bindings`` view over the final history version."""
    last = history.versions[-1]
    tables: dict[str, TableBinding] = {}
    for source_table, table in last.tables.items():
        tables[source_table] = TableBinding(
            source_table=source_table,
            target_table=table.target_table,
            columns={f.name: f.name for f in table.fields},
        )
    return Bindings(tables=tables)


def _parse_streams(value: object, where: str) -> tuple[StreamSpec, ...]:
    if not isinstance(value, list) or not value:
        raise CaptureArchiveError(f"{where}: streams must be a non-empty array")
    specs: list[StreamSpec] = []
    seen: set[str] = set()
    for idx, item in enumerate(value):
        sw = f"{where} streams[{idx}]"
        if not isinstance(item, dict):
            raise CaptureArchiveError(f"{sw}: stream must be an object")
        unknown = set(item) - {"name", "first_offset", "last_offset"}
        if unknown:
            raise CaptureArchiveError(f"{sw}: unknown fields {sorted(unknown)}")
        _require_fields(item, ("name", "first_offset", "last_offset"), sw)
        name = item["name"]
        if not isinstance(name, str) or not name:
            raise CaptureArchiveError(f"{sw}: name must be a non-empty string")
        if name in seen:
            raise CaptureArchiveError(f"{sw}: duplicate stream name {name!r}")
        seen.add(name)
        first, last = item["first_offset"], item["last_offset"]
        if not _is_nonneg_int(first) or not _is_nonneg_int(last):
            raise CaptureArchiveError(
                f"{sw}: offsets must be non-negative integers (booleans rejected)"
            )
        if last < first:
            raise CaptureArchiveError(f"{sw}: last_offset < first_offset")
        specs.append(StreamSpec(name=name, first_offset=first, last_offset=last))
    return tuple(specs)


def _parse_files(
    archive: Path, value: object, where: str,
    allow: tuple[str, ...] = (MANIFEST_NAME,),
) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise CaptureArchiveError(f"{where}: files must be a non-empty array")
    files: list[str] = []
    seen: set[str] = set()
    for item in value:
        rel = safe_relative_path(item)
        if rel in seen:
            raise CaptureArchiveError(f"{where}: duplicate file entry {rel!r}")
        seen.add(rel)
        path = archive / rel
        if not path.exists():
            raise CaptureArchiveError(f"{where}: declared file does not exist: {rel}")
        if path.is_symlink():
            raise CaptureArchiveError(f"{where}: symlinks are not allowed: {rel}")
        if not path.is_file():
            raise CaptureArchiveError(f"{where}: declared path is not a regular file: {rel}")
        files.append(rel)
    # No undeclared regular files may live anywhere inside the archive.
    declared = {archive / f for f in files}
    for root, dirnames, filenames in os.walk(archive):
        for fname in filenames:
            candidate = Path(root) / fname
            if candidate in {archive / name for name in allow}:
                continue
            if candidate not in declared:
                rel = candidate.relative_to(archive).as_posix()
                raise CaptureArchiveError(
                    f"{where}: undeclared file present in archive: {rel}"
                )
        for dirname in dirnames:
            candidate = Path(root) / dirname
            if candidate.is_symlink():
                rel = candidate.relative_to(archive).as_posix()
                raise CaptureArchiveError(f"{where}: symlinked directory not allowed: {rel}")
    return tuple(files)


def _parse_bindings(value: object, schema: Schema, where: str) -> Bindings:
    if not isinstance(value, dict) or set(value) != {"tables"}:
        raise CaptureArchiveError(f"{where}: bindings must be an object with only 'tables'")
    raw_tables = value["tables"]
    if not isinstance(raw_tables, dict) or not raw_tables:
        raise CaptureArchiveError(f"{where}: bindings.tables must be a non-empty object")

    bindings: dict[str, TableBinding] = {}
    target_tables_used: dict[str, str] = {}
    target_columns_used: dict[str, dict[str, str]] = {}

    for source_table, spec in raw_tables.items():
        bw = f"{where} bindings.tables[{source_table!r}]"
        if not isinstance(source_table, str) or not source_table:
            raise CaptureArchiveError(f"{bw}: source table name must be a non-empty string")
        if not isinstance(spec, dict) or set(spec) != {"target", "columns"}:
            raise CaptureArchiveError(f"{bw}: binding needs exactly 'target' and 'columns'")
        target_table = spec["target"]
        if not isinstance(target_table, str) or not target_table:
            raise CaptureArchiveError(f"{bw}: target must be a non-empty string")
        if target_table not in schema.tables:
            raise CaptureArchiveError(
                f"{bw}: target table {target_table!r} does not exist in the schema"
            )
        if target_table in target_tables_used:
            raise CaptureArchiveError(
                f"{bw}: target table {target_table!r} is already bound from source "
                f"{target_tables_used[target_table]!r}; bindings must be one-to-one"
            )
        target_tables_used[target_table] = source_table

        raw_columns = spec["columns"]
        if not isinstance(raw_columns, dict) or not raw_columns:
            raise CaptureArchiveError(f"{bw}: columns must be a non-empty object")
        target_schema_table = schema.tables[target_table]
        column_map: dict[str, str] = {}
        used_target_cols: dict[str, str] = {}
        for source_col, target_col in raw_columns.items():
            cw = f"{bw} columns[{source_col!r}]"
            if not isinstance(source_col, str) or not source_col:
                raise CaptureArchiveError(f"{cw}: source column name must be a non-empty string")
            if not isinstance(target_col, str) or not target_col:
                raise CaptureArchiveError(f"{cw}: target column must be a non-empty string")
            if target_col not in target_schema_table.columns:
                raise CaptureArchiveError(
                    f"{cw}: target column {target_col!r} does not exist on "
                    f"{target_table!r}"
                )
            if target_col in used_target_cols:
                raise CaptureArchiveError(
                    f"{cw}: target column {target_col!r} bound more than once"
                )
            used_target_cols[target_col] = source_col
            column_map[source_col] = target_col
        missing_target = set(target_schema_table.columns) - set(used_target_cols)
        if missing_target:
            raise CaptureArchiveError(
                f"{bw}: binding does not cover target columns "
                f"{sorted(missing_target)}; no implicit column drops are allowed"
            )
        target_columns_used[target_table] = used_target_cols
        bindings[source_table] = TableBinding(
            source_table=source_table,
            target_table=target_table,
            columns=column_map,
        )

    missing_tables = set(schema.tables) - set(target_tables_used)
    if missing_tables:
        raise CaptureArchiveError(
            f"{where}: bindings do not cover target tables {sorted(missing_tables)}"
        )
    return Bindings(tables=bindings)
