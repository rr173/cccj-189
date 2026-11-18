"""Schema-version adapters for capture-v2 (schema-evolving) archives.

Each change message declares the schema version it was written with.  A
message's row image is interpreted with **that** version's columns, types and
primary key, then mapped onto the unified target schema through stable field
IDs:

* a rename keeps the field ID, so old and new names land in the same target
  column;
* a target column whose field did not exist yet in the message's version is
  filled from that field's declared constant *backfill*;
* ``int`` values widen to ``float`` without changing the representable value.

Nothing here guesses from column names alone: two different field IDs are
never merged merely because their names coincide, and a target column is never
written by a field with a different ID.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..model import Database, Row
from ..schema import Column, ForeignKey, Schema, TableSchema
from ..values import DataError, make_row_id, normalize_scalar
from .errors import CaptureSchemaHistoryError, CaptureRestoreError
from .history import FieldSpec, SchemaHistory, SchemaVersion


# ------------------------------------------------------------ version schemas
def version_schema(history: SchemaHistory, version: SchemaVersion) -> Schema:
    """The complete relational schema as it stood at ``version``.

    Column and primary-key names are that version's names, types are exactly
    as declared and constraints are the version's constraints.  Used to parse
    the snapshot and to validate message values *before* they are widened or
    renamed, so e.g. an ``int`` column still rejects ``1.5``.
    """
    parts: dict[str, tuple[dict[str, Column], tuple[str, ...],
                          list[tuple[str, ...]], list[ForeignKey]]] = {}
    for source_table, table in version.tables.items():
        columns = {f.name: Column(f.name, f.type, f.nullable) for f in table.fields}
        pk = tuple(table.by_id(fid).name for fid in table.primary_key)
        unique_keys = [tuple(table.by_id(fid).name for fid in uk)
                       for uk in table.unique_keys]
        if pk not in unique_keys:
            unique_keys.insert(0, pk)
        fks = []
        for fk in table.foreign_keys:
            ref = version.tables[fk["ref_table"]]
            fks.append(ForeignKey(
                columns=tuple(table.by_id(fid).name for fid in fk["columns"]),
                ref_table=ref.target_table,
                ref_columns=tuple(ref.by_id(fid).name for fid in fk["ref_columns"]),
                nullable=fk["nullable"],
            ))
        if table.target_table in parts:
            raise CaptureSchemaHistoryError(
                f"schema version {version.version}: target table "
                f"{table.target_table!r} is bound more than once"
            )
        parts[table.target_table] = (columns, pk, unique_keys, fks)
    return Schema(tables={
        name: TableSchema(name=name, columns=cols, primary_key=pk,
                          unique_keys=tuple(uks), foreign_keys=fks)
        for name, (cols, pk, uks, fks) in parts.items()
    })


# -------------------------------------------------------------- event layout
@dataclass(frozen=True)
class AddedField:
    """A target column whose field did not exist in the message's version."""

    field: FieldSpec
    target_column: str

    @property
    def fill_value(self) -> Any:
        return self.field.backfill if self.field.has_backfill else None


@dataclass(frozen=True)
class VersionLayout:
    """How one source table at one schema version maps onto the target schema."""

    source_table: str
    target_table: str
    schema_version: int | None  # None for capture-v1 (single schema)
    source_pk: tuple[str, ...]  # source column names, target PK order
    source_columns: tuple[str, ...]
    field_of_source: dict[str, FieldSpec]  # source column name -> field
    target_of_field: dict[str, str]  # field ID -> final target column
    added: tuple[AddedField, ...]  # fields absent from this version

    def target_for(self, source_column: str) -> str:
        field = self.field_of_source.get(source_column)
        if field is None:
            raise KeyError(source_column)
        return self.target_of_field[field.field_id]

    def added_targets(self) -> dict[str, AddedField]:
        return {a.target_column: a for a in self.added}


def layout_for(
    history: SchemaHistory | None,
    manifest,
    target_schema: Schema,
    source_table: str,
    schema_version: int | None,
) -> VersionLayout:
    """Build the mapping for ``source_table`` at ``schema_version``.

    ``history is None`` selects capture-v1 semantics: one manifest binding,
    no added fields.
    """
    if history is None:
        binding = manifest.bindings.tables.get(source_table)
        if binding is None:
            raise CaptureRestoreError(
                f"source table {source_table!r} has no binding"
            )
        target_table = binding.target_table
        target = target_schema.tables[target_table]
        target_to_source = binding.target_to_source_columns
        source_pk = tuple(
            target_to_source[col] for col in target.primary_key
        )
        fields = {
            src: FieldSpec(
                field_id=f"v1:{src}",
                name=src,
                type=target.columns[tgt].type,
                nullable=target.columns[tgt].nullable,
            )
            for src, tgt in binding.columns.items()
        }
        return VersionLayout(
            source_table=source_table,
            target_table=target_table,
            schema_version=None,
            source_pk=source_pk,
            source_columns=tuple(binding.columns),
            field_of_source=fields,
            target_of_field={f"v1:{s}": t for s, t in binding.columns.items()},
            added=(),
        )

    assert history is not None and schema_version is not None
    version = history.by_number(schema_version)
    if source_table not in version.tables:
        raise CaptureRestoreError(
            f"schema version {schema_version}: source table "
            f"{source_table!r} is not defined"
        )
    table = version.tables[source_table]
    target_table = table.target_table
    target = target_schema.tables[target_table]
    field_of_source = {f.name: f for f in table.fields}
    current_ids = {f.field_id for f in table.fields}
    # A field keeps its identity across renames, so its target column is
    # always the name the *final* version binds to that field ID.
    final_table = history.versions[-1].tables[source_table]
    target_of_field = {f.field_id: f.name for f in final_table.fields}
    source_pk = tuple(table.by_id(fid).name for fid in table.primary_key)
    added = []
    for field in _last_fields(history, source_table):
        if field.field_id in current_ids:
            continue
        target_col = target_of_field[field.field_id]
        added.append(AddedField(field=field, target_column=target_col))
    return VersionLayout(
        source_table=source_table,
        target_table=target_table,
        schema_version=schema_version,
        source_pk=source_pk,
        source_columns=tuple(field_of_source),
        field_of_source=field_of_source,
        target_of_field=target_of_field,
        added=tuple(added),
    )


def _last_fields(history: SchemaHistory, source_table: str) -> tuple[FieldSpec, ...]:
    return history.versions[-1].tables[source_table].fields


# --------------------------------------------------------------- conversion
def convert_scalar(
    value: Any,
    field: FieldSpec,
    target_column: Column,
    where: str,
    *,
    is_key: bool = False,
) -> Any:
    """Validate under the *version's* type, then widen to the target type."""
    if value is None:
        if not field.nullable or is_key:
            raise CaptureRestoreError(
                f"{where}: explicit null in NOT NULL column {field.name!r}"
                + (" (primary key)" if is_key else "")
            )
        return None
    try:
        normalized = normalize_scalar(value, field.type)
    except DataError as exc:
        raise CaptureRestoreError(f"{where}: column {field.name!r}: {exc}") from exc
    if field.type == target_column.type:
        return normalized
    if field.type == "int" and target_column.type == "float":
        # Lossless widening only; normalize_scalar already proved an int.
        return float(normalized)
    raise CaptureRestoreError(
        f"{where}: column {field.name!r}: cannot map declared type "
        f"{field.type!r} onto target type {target_column.type!r}"
    )


def convert_image(
    layout: VersionLayout,
    image: dict[str, Any],
    target_schema: Schema,
    where: str,
    *,
    complete: bool,
) -> dict[str, Any]:
    """Convert a (possibly partial) source image into target columns.

    ``complete`` images (insert after, delete before completion) receive the
    backfill values of fields that did not exist yet in this version.  Partial
    images never do: an absent column means *preserve*, never *overwrite with
    the default*.
    """
    target = target_schema.tables[layout.target_table]
    unknown = set(image) - set(layout.source_columns)
    if unknown:
        raise CaptureRestoreError(
            f"{where}: unknown source columns {sorted(unknown)} on "
            f"{layout.source_table!r} at schema version "
            f"{layout.schema_version if layout.schema_version is not None else 1}"
        )
    out: dict[str, Any] = {}
    for source_col, value in image.items():
        field = layout.field_of_source[source_col]
        target_col = layout.target_of_field[field.field_id]
        out[target_col] = convert_scalar(
            value, field, target.columns[target_col], f"{where} {source_col!r}"
        )
    if complete:
        missing_version = set(layout.source_columns) - set(image)
        if missing_version:
            raise CaptureRestoreError(
                f"{where}: incomplete source image, missing columns "
                f"{sorted(missing_version)}"
            )
        for added in layout.added:
            if target.columns[added.target_column].nullable is False and \
                    not added.field.has_backfill:
                raise CaptureRestoreError(
                    f"{where}: no value for NON NULL target column "
                    f"{layout.target_table}.{added.target_column!r} and field "
                    f"{added.field.field_id!r} declares no backfill"
                )
            out[added.target_column] = added.field.backfill if \
                added.field.has_backfill else None
        absent = set(target.columns) - set(out)
        if absent:
            for col_name in absent:
                if not target.columns[col_name].nullable:
                    raise CaptureRestoreError(
                        f"{where}: missing NOT NULL column "
                        f"{layout.target_table}.{col_name!r}"
                    )
                out[col_name] = None
    return out


def convert_key(
    layout: VersionLayout,
    key: dict[str, Any],
    target_schema: Schema,
    where: str,
) -> dict[str, Any]:
    if set(key) != set(layout.source_pk):
        raise CaptureRestoreError(
            f"{where}: key columns {sorted(key)} do not match the source "
            f"primary key {sorted(layout.source_pk)} at schema version "
            f"{layout.schema_version if layout.schema_version is not None else 1}"
        )
    target = target_schema.tables[layout.target_table]
    out: dict[str, Any] = {}
    for source_col, value in key.items():
        field = layout.field_of_source[source_col]
        target_col = layout.target_of_field[field.field_id]
        out[target_col] = convert_scalar(
            value, field, target.columns[target_col], f"{where} key {source_col!r}",
            is_key=True,
        )
    return out


def load_versioned_snapshot(
    history: SchemaHistory,
    target_schema: Schema,
    snapshot_dir: str | Path,
) -> Database:
    """Load the starting snapshot written under the initial schema version.

    Files are named ``<target table>.jsonl``; each row's columns are the
    *first* schema version's source columns (its historical names and types).
    Rows are converted into the unified target schema - which backfills every
    field that only exists in later versions - and then validated with the
    ordinary target-schema unique/FK rules.
    """
    snapshot_dir = Path(snapshot_dir)
    if not snapshot_dir.is_dir():
        raise CaptureRestoreError(
            f"snapshot directory not found: {snapshot_dir}"
        )
    first = history.versions[0]
    rows: list[Row] = []
    for target_table in sorted(target_schema.tables):
        source_table = _source_for_target(first, target_table)
        path = snapshot_dir / f"{target_table}.jsonl"
        if not path.exists():
            raise CaptureRestoreError(f"missing table file: {path}")
        layout = _snapshot_layout(history, target_schema, source_table, first)
        with open(path, "r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise CaptureRestoreError(
                        f"{path.name}:{line_no}: invalid JSON ({exc})"
                    ) from exc
                if not isinstance(obj, dict):
                    raise CaptureRestoreError(
                        f"{path.name}:{line_no}: row must be a JSON object"
                    )
                where = f"snapshot {target_table} row {line_no}"
                try:
                    data = convert_image(
                        layout, obj, target_schema, where, complete=True
                    )
                except CaptureRestoreError as exc:
                    exc.with_locator(file=path.name, line=line_no,
                                     table=target_table)
                    raise
                table = target_schema.tables[target_table]
                rows.append(Row(
                    table=target_table,
                    row_id=make_row_id(target_table, table.pk_types, data),
                    data=data,
                ))
    db = Database(target_schema, rows)
    try:
        db.validate()
    except DataError as exc:
        raise CaptureRestoreError(f"snapshot: {exc}") from exc
    return db


def _source_for_target(version: SchemaVersion, target_table: str) -> str:
    for source_table, table in version.tables.items():
        if table.target_table == target_table:
            return source_table
    raise CaptureSchemaHistoryError(
        f"schema version {version.version}: no source table maps to target "
        f"table {target_table!r}"
    )


def _snapshot_layout(history: SchemaHistory, target_schema: Schema,
                     source_table: str, version: SchemaVersion) -> VersionLayout:
    """Layout as of the snapshot version (its fields, plus later additions)."""
    table = version.tables[source_table]
    target_table = table.target_table
    target = target_schema.tables[target_table]
    field_of_source = {f.name: f for f in table.fields}
    final_table = history.versions[-1].tables[source_table]
    target_of_field = {f.field_id: f.name for f in final_table.fields}
    current_ids = {f.field_id for f in table.fields}
    added = []
    for field in final_table.fields:
        if field.field_id in current_ids:
            continue
        added.append(AddedField(
            field=field, target_column=target_of_field[field.field_id],
        ))
    return VersionLayout(
        source_table=source_table,
        target_table=target_table,
        schema_version=version.version,
        source_pk=tuple(table.by_id(fid).name for fid in table.primary_key),
        source_columns=tuple(field_of_source),
        field_of_source=field_of_source,
        target_of_field=target_of_field,
        added=tuple(added),
    )
