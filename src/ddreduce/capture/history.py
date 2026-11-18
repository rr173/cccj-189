"""Ordered schema history for capture-v2 schema-evolving archives.

A capture-v1 archive is written against exactly one schema.  Real CDC feeds
are not: upstream tables are renamed and extended while the same archive keeps
events written before *and* after the change.  Reading an old row image with
the newest schema silently turns old columns into missing values; reading a
new row image with the oldest schema silently drops the new information.

A capture-v2 archive therefore carries an explicit, ordered **schema
history**.  The history file is::

    {
      "format": "capture-schema-history-v1",
      "versions": [
        {"version": 1,
         "effective_commit_seq": 0,
         "tables": {
           "accounts": {
             "target_table": "accounts",
             "primary_key": ["f-key"],
             "columns": [
               {"field_id": "f-key", "name": "key", "type": "string",
                "nullable": false},
               {"field_id": "f-amount", "name": "gross", "type": "int",
                "nullable": false}
             ],
             "unique_keys": [],
             "foreign_keys": []
           }
         }},
        {"version": 2,
         "effective_commit_seq": 2,
         "tables": {
           "accounts": {
             "target_table": "accounts",
             "primary_key": ["f-key"],
             "columns": [
               {"field_id": "f-key", "name": "key", "type": "string",
                "nullable": false},
               {"field_id": "f-amount", "name": "amount", "type": "float",
                "nullable": false},
               {"field_id": "f-currency", "name": "currency", "type": "string",
                "nullable": false, "backfill": "USD"}
             ],
             "unique_keys": [],
             "foreign_keys": []
           }
         }}
      ]
    }

Every version binds:

* a strictly increasing **version number** (1, 2, 3, ... in file order);
* ``effective_commit_seq`` - the commit point at which the version becomes
  the write schema.  Version 1 must start at or before
  ``snapshot_commit_seq``; later versions must be strictly increasing and
  must lie inside the archive's sealed range.  A schema switch happens at a
  transaction commit boundary, so no transaction can straddle two versions;
* per table: the target table, the ordered primary key (as *field IDs*), the
  column list (stable ``field_id``, current ``name``, scalar ``type``,
  ``nullable`` and, for columns introduced by this version, an explicit
  ``backfill`` constant), unique keys and foreign keys (by field ID).

The *stable field ID* is the identity of a business field across renames.  A
``capture-v2`` change message declares the schema version it was written
with; column names in its images are resolved through that version, then
mapped to the final target schema by field ID.

Supported evolution (see :func:`validate_evolution`) is deliberately narrow:

* a field ID keeps the same business identity forever - it can never be
  replaced, and two different field IDs are never merged even when their
  current names coincide;
* columns may be renamed (same field ID, different name);
* new columns may be added, but only with an explicit constant backfill value
  and only if they are nullable or carry that backfill;
* ``int`` may widen losslessly to ``float``;
* deleting a column, changing a field ID, narrowing a type, changing an
  existing column's nullability or a key/constraint definition are refused.

These are the only transformations the importer can replay exactly: a rename
does not change values, an added column has one known value for all older
rows, and int -> float preserves every representable input value exactly.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..schema import Column, Schema, TableSchema
from ..values import (
    DataError,
    SCALAR_TYPES,
    canonical_json,
    normalize_scalar,
)
from .errors import CaptureSchemaHistoryError

HISTORY_FORMAT = "capture-schema-history-v1"
WIDENING = {("int", "float")}  # only lossless widening we support


# ---------------------------------------------------------------- data model
@dataclass(frozen=True)
class FieldSpec:
    field_id: str
    name: str
    type: str
    nullable: bool
    backfill: Any = None
    has_backfill: bool = False


@dataclass(frozen=True)
class TableVersion:
    target_table: str
    fields: tuple[FieldSpec, ...]
    primary_key: tuple[str, ...]  # field IDs, ordered
    unique_keys: tuple[tuple[str, ...], ...]  # field-ID tuples
    foreign_keys: tuple[dict[str, Any], ...]  # structure preserved for hashing

    def by_id(self, field_id: str) -> FieldSpec:
        for f in self.fields:
            if f.field_id == field_id:
                return f
        raise KeyError(field_id)

    def by_name(self, name: str) -> FieldSpec:
        for f in self.fields:
            if f.name == name:
                return f
        raise KeyError(name)

    @property
    def field_ids(self) -> tuple[str, ...]:
        return tuple(f.field_id for f in self.fields)


@dataclass(frozen=True)
class SchemaVersion:
    version: int
    effective_commit_seq: int
    tables: dict[str, TableVersion]  # source table -> structure

    def table(self, source_table: str) -> TableVersion:
        try:
            return self.tables[source_table]
        except KeyError:
            raise CaptureSchemaHistoryError(
                f"schema version {self.version}: source table "
                f"{source_table!r} is not defined"
            ) from None


@dataclass(frozen=True)
class SchemaHistory:
    versions: tuple[SchemaVersion, ...]
    raw: dict[str, Any]

    def canonical_bytes(self) -> bytes:
        return canonical_json(self.raw).encode("utf-8")

    def content_hash(self) -> str:
        import hashlib

        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    def version_numbers(self) -> list[int]:
        return [v.version for v in self.versions]

    def by_number(self, number: int) -> SchemaVersion:
        for v in self.versions:
            if v.version == number:
                return v
        raise CaptureSchemaHistoryError(f"schema version {number} is not defined")

    def first(self) -> SchemaVersion:
        return self.versions[0]

    def for_commit(self, commit_seq: int) -> SchemaVersion:
        """The version in force for a transaction committing at ``commit_seq``."""
        chosen = self.versions[0]
        for v in self.versions:
            if v.effective_commit_seq <= commit_seq:
                chosen = v
            else:
                break
        return chosen

    def switch_points(self) -> list[tuple[int, int]]:
        """(version, effective_commit_seq) of every switch after the first."""
        return [
            (v.version, v.effective_commit_seq) for v in self.versions[1:]
        ]

    def target_tables(self) -> set[str]:
        out: set[str] = set()
        for v in self.versions:
            for t in v.tables.values():
                out.add(t.target_table)
        return out


# ------------------------------------------------------------------- parsing
def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_nonneg_int(value: object) -> bool:
    return _is_int(value) and value >= 0


def _require_obj(value: object, where: str) -> dict:
    if not isinstance(value, dict):
        raise CaptureSchemaHistoryError(f"{where}: must be a JSON object")
    return value


def _check_fields(obj: dict, required: tuple[str, ...], optional: tuple[str, ...],
                  where: str) -> None:
    missing = [f for f in required if f not in obj]
    if missing:
        raise CaptureSchemaHistoryError(f"{where}: missing fields {sorted(missing)}")
    unknown = set(obj) - set(required) - set(optional)
    if unknown:
        raise CaptureSchemaHistoryError(f"{where}: unknown fields {sorted(unknown)}")


def load_schema_history(path: str | Path) -> SchemaHistory:
    """Read and structurally validate a schema-history file (no archive yet)."""
    path = Path(path)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise CaptureSchemaHistoryError(f"schema history not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise CaptureSchemaHistoryError(
            f"{path}: invalid JSON ({exc})"
        ) from exc
    raw = _require_obj(raw, str(path))
    _check_fields(raw, ("format", "versions"), (), str(path))
    if raw["format"] != HISTORY_FORMAT:
        raise CaptureSchemaHistoryError(
            f"{path}: unsupported schema history format {raw['format']!r}; "
            f"only {HISTORY_FORMAT!r} is supported"
        )
    versions_raw = raw["versions"]
    if not isinstance(versions_raw, list) or not versions_raw:
        raise CaptureSchemaHistoryError(
            f"{path}: versions must be a non-empty array"
        )
    versions: list[SchemaVersion] = []
    prev_number: int | None = None
    prev_effective: int | None = None
    for idx, item in enumerate(versions_raw):
        vw = f"{path} versions[{idx}]"
        item = _require_obj(item, vw)
        _check_fields(item, ("version", "effective_commit_seq", "tables"), (), vw)
        number = item["version"]
        if not _is_int(number) or number < 1:
            raise CaptureSchemaHistoryError(
                f"{vw}: version must be an integer >= 1 (booleans rejected)"
            )
        effective = item["effective_commit_seq"]
        if not _is_nonneg_int(effective):
            raise CaptureSchemaHistoryError(
                f"{vw}: effective_commit_seq must be a non-negative integer "
                "(booleans rejected)"
            )
        if prev_number is not None:
            if number == prev_number:
                raise CaptureSchemaHistoryError(
                    f"{vw}: duplicate schema version number {number}; "
                    "version numbers must be unique"
                )
            if number < prev_number:
                raise CaptureSchemaHistoryError(
                    f"{vw}: schema versions must be in strictly increasing "
                    f"order; {number} follows {prev_number}"
                )
        if prev_effective is not None and effective <= prev_effective:
            raise CaptureSchemaHistoryError(
                f"{vw}: commit point conflict - version {number} becomes "
                f"effective at commit_seq {effective}, which is not strictly "
                f"after the previous version's {prev_effective}"
            )
        tables = _parse_version_tables(item["tables"], vw)
        versions.append(
            SchemaVersion(
                version=number,
                effective_commit_seq=effective,
                tables=tables,
            )
        )
        prev_number, prev_effective = number, effective

    if versions[0].effective_commit_seq != 0:
        raise CaptureSchemaHistoryError(
            f"{path}: the first schema version must be effective at "
            f"commit_seq 0 (the initial schema); got "
            f"{versions[0].effective_commit_seq}"
        )
    history = SchemaHistory(versions=tuple(versions), raw=raw)
    validate_evolution(history)
    return history


def _parse_version_tables(value: object, where: str) -> dict[str, TableVersion]:
    value = _require_obj(value, f"{where} tables")
    if not value:
        raise CaptureSchemaHistoryError(f"{where}: tables must be a non-empty object")
    tables: dict[str, TableVersion] = {}
    target_names: dict[str, str] = {}
    for source_table, spec in value.items():
        tw = f"{where} tables[{source_table!r}]"
        if not isinstance(source_table, str) or not source_table:
            raise CaptureSchemaHistoryError(
                f"{tw}: source table name must be a non-empty string"
            )
        spec = _require_obj(spec, tw)
        _check_fields(
            spec, ("target_table", "columns", "primary_key"),
            ("unique_keys", "foreign_keys"), tw,
        )
        target_table = spec["target_table"]
        if not isinstance(target_table, str) or not target_table:
            raise CaptureSchemaHistoryError(
                f"{tw}: target_table must be a non-empty string"
            )
        if target_table in target_names:
            raise CaptureSchemaHistoryError(
                f"{tw}: target table {target_table!r} is already bound from "
                f"source table {target_names[target_table]!r}; table bindings "
                "must be one-to-one"
            )
        target_names[target_table] = source_table
        fields = _parse_fields(spec["columns"], tw)
        pks = _parse_id_list(spec["primary_key"], fields, f"{tw} primary_key")
        if not pks:
            raise CaptureSchemaHistoryError(f"{tw}: primary_key must be non-empty")
        for pk in pks:
            if fields[pk].nullable:
                raise CaptureSchemaHistoryError(
                    f"{tw}: primary-key field {pk!r} must not be nullable"
                )
        term = TableVersion(
            target_table=target_table,
            fields=tuple(fields.values()),
            primary_key=pks,
            unique_keys=_parse_unique_keys(spec.get("unique_keys", []), fields, tw),
            foreign_keys=(),
        )
        fks = _parse_foreign_keys(spec.get("foreign_keys", []), tw, term, value)
        term = TableVersion(
            target_table=target_table,
            fields=tuple(fields.values()),
            primary_key=pks,
            unique_keys=term.unique_keys,
            foreign_keys=fks,
        )
        tables[source_table] = term
    return tables


def _parse_fields(value: object, where: str) -> dict[str, FieldSpec]:
    if not isinstance(value, list) or not value:
        raise CaptureSchemaHistoryError(f"{where}: columns must be a non-empty array")
    fields: dict[str, FieldSpec] = {}
    names: dict[str, str] = {}
    for idx, item in enumerate(value):
        fw = f"{where} columns[{idx}]"
        item = _require_obj(item, fw)
        _check_fields(item, ("field_id", "name", "type", "nullable"),
                      ("backfill",), fw)
        field_id, name, typ = item["field_id"], item["name"], item["type"]
        if not isinstance(field_id, str) or not field_id:
            raise CaptureSchemaHistoryError(f"{fw}: field_id must be a non-empty string")
        if not isinstance(name, str) or not name:
            raise CaptureSchemaHistoryError(f"{fw}: name must be a non-empty string")
        if typ not in SCALAR_TYPES or typ == "null":
            raise CaptureSchemaHistoryError(f"{fw}: unsupported column type {typ!r}")
        if not isinstance(item["nullable"], bool):
            raise CaptureSchemaHistoryError(f"{fw}: nullable must be a boolean")
        if field_id in fields:
            raise CaptureSchemaHistoryError(f"{fw}: duplicate field_id {field_id!r}")
        if name in names:
            raise CaptureSchemaHistoryError(
                f"{fw}: duplicate column name {name!r} in one schema version"
            )
        names[name] = field_id
        has_backfill = "backfill" in item
        backfill = item.get("backfill")
        if has_backfill:
            if backfill is None:
                raise CaptureSchemaHistoryError(
                    f"{fw}: backfill must be an explicit non-null constant"
                )
            try:
                backfill = normalize_scalar(backfill, typ)
            except DataError as exc:
                raise CaptureSchemaHistoryError(f"{fw}: backfill: {exc}") from exc
        fields[field_id] = FieldSpec(
            field_id=field_id, name=name, type=typ,
            nullable=item["nullable"], backfill=backfill,
            has_backfill=has_backfill,
        )
    return fields


def _parse_id_list(value: object, fields: dict[str, FieldSpec], where: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise CaptureSchemaHistoryError(f"{where}: must be an array of field IDs")
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str) or not item:
            raise CaptureSchemaHistoryError(
                f"{where}: field IDs must be non-empty strings"
            )
        if item in seen:
            raise CaptureSchemaHistoryError(f"{where}: duplicate field ID {item!r}")
        if item not in fields:
            raise CaptureSchemaHistoryError(
                f"{where}: unknown field ID {item!r}"
            )
        seen.add(item)
        out.append(item)
    return tuple(out)


def _parse_unique_keys(value: object, fields: dict[str, FieldSpec],
                       where: str) -> tuple[tuple[str, ...], ...]:
    if not isinstance(value, list):
        raise CaptureSchemaHistoryError(f"{where}: unique_keys must be an array")
    out: list[tuple[str, ...]] = []
    for idx, item in enumerate(value):
        key = _parse_id_list(item, fields, f"{where} unique_keys[{idx}]")
        if not key:
            raise CaptureSchemaHistoryError(
                f"{where} unique_keys[{idx}]: must be non-empty"
            )
        if key in out:
            raise CaptureSchemaHistoryError(
                f"{where} unique_keys[{idx}]: duplicate unique key {list(key)}"
            )
        out.append(key)
    return tuple(out)


def _parse_foreign_keys(value: object, where: str, table: TableVersion,
                        all_tables: dict) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        raise CaptureSchemaHistoryError(f"{where}: foreign_keys must be an array")
    out: list[dict[str, Any]] = []
    for idx, item in enumerate(value):
        fw = f"{where} foreign_keys[{idx}]"
        item = _require_obj(item, fw)
        _check_fields(
            item, ("columns", "ref_table", "ref_columns", "nullable"), (), fw
        )
        cols = _parse_id_list(item["columns"], {f.field_id: f for f in table.fields},
                              f"{fw} columns")
        ref_table = item["ref_table"]
        if not isinstance(ref_table, str) or not ref_table:
            raise CaptureSchemaHistoryError(
                f"{fw}: ref_table must be a non-empty string"
            )
        if ref_table not in all_tables:
            raise CaptureSchemaHistoryError(
                f"{fw}: ref_table {ref_table!r} is not a table of this schema "
                "version"
            )
        if not cols:
            raise CaptureSchemaHistoryError(f"{fw}: columns must be non-empty")
        if not isinstance(item["nullable"], bool):
            raise CaptureSchemaHistoryError(f"{fw}: nullable must be a boolean")
        out.append({
            "columns": cols,
            "ref_table": ref_table,
            "ref_columns": tuple(item["ref_columns"]) if isinstance(
                item["ref_columns"], list) else item["ref_columns"],
            "nullable": item["nullable"],
        })
    return tuple(out)


# ---------------------------------------------------------------- evolution
def validate_evolution(history: SchemaHistory) -> None:
    """Check the supported schema-evolution rules across the version chain."""
    first = history.versions[0]
    prev = first
    for cur in history.versions[1:]:
        _validate_step(prev, cur)
        prev = cur
    # First version must be internally closed; later versions were checked
    # against their predecessor, and constraints are stable, so the last one
    # captures the whole history.
    _validate_constraints(first)


def _validate_step(prev: SchemaVersion, cur: SchemaVersion) -> None:
    if set(prev.tables) != set(cur.tables):
        added = sorted(set(cur.tables) - set(prev.tables))
        removed = sorted(set(prev.tables) - set(cur.tables))
        raise CaptureSchemaHistoryError(
            f"schema version {cur.version}: tables cannot be added or removed; "
            f"added={added}, removed={removed}"
        )
    for source_table, old in prev.tables.items():
        new = cur.tables[source_table]
        if old.target_table != new.target_table:
            raise CaptureSchemaHistoryError(
                f"schema version {cur.version}: table {source_table!r} target "
                f"changed from {old.target_table!r} to {new.target_table!r}"
            )
        old_ids = set(old.field_ids)
        new_ids = set(new.field_ids)
        removed = sorted(old_ids - new_ids)
        if removed:
            raise CaptureSchemaHistoryError(
                f"schema version {cur.version}: table {source_table!r} deletes "
                f"field IDs {removed}; deleting a column is not supported"
            )
        for field_id in sorted(old_ids & new_ids, key=old.field_ids.index):
            before = old.by_id(field_id)
            after = new.by_id(field_id)
            if before.type != after.type:
                if (before.type, after.type) not in WIDENING:
                    raise CaptureSchemaHistoryError(
                        f"schema version {cur.version}: field {field_id!r} on "
                        f"{source_table!r} changes type {before.type!r} -> "
                        f"{after.type!r}; only int -> float widening is allowed "
                        "(no narrowing, no unrelated retyping)"
                    )
            if before.nullable != after.nullable:
                raise CaptureSchemaHistoryError(
                    f"schema version {cur.version}: field {field_id!r} on "
                    f"{source_table!r} changes its nullability rule "
                    f"({before.nullable} -> {after.nullable})"
                )
            if before.has_backfill:
                if not after.has_backfill or after.backfill != before.backfill:
                    raise CaptureSchemaHistoryError(
                        f"schema version {cur.version}: field {field_id!r} on "
                        f"{source_table!r} changes or drops the backfill value "
                        "declared when it was introduced"
                    )
        for field_id in sorted(new_ids - old_ids):
            field = new.by_id(field_id)
            if not field.has_backfill and not field.nullable:
                raise CaptureSchemaHistoryError(
                    f"schema version {cur.version}: new NON NULL field "
                    f"{field_id!r} ({field.name!r}) on {source_table!r} has no "
                    "explicit backfill value; older rows cannot be filled"
                )
        # The same field ID must not silently change its target column: the
        # mapping is by field ID and is fixed by the final version.
        if old.primary_key != new.primary_key:
            raise CaptureSchemaHistoryError(
                f"schema version {cur.version}: primary key of "
                f"{source_table!r} changed; primary-key fields are immutable"
            )
        if old.unique_keys != new.unique_keys:
            raise CaptureSchemaHistoryError(
                f"schema version {cur.version}: unique keys of "
                f"{source_table!r} changed"
            )
        if _fk_signature(old) != _fk_signature(new):
            raise CaptureSchemaHistoryError(
                f"schema version {cur.version}: foreign keys of "
                f"{source_table!r} changed"
            )


def _fk_signature(table: TableVersion) -> tuple:
    return tuple(sorted(
        (tuple(fk["columns"]), fk["ref_table"], tuple(fk["ref_columns"]),
         fk["nullable"])
        for fk in table.foreign_keys
    ))


def _validate_constraints(version: SchemaVersion) -> None:
    for source_table, table in version.tables.items():
        fields = {f.field_id: f for f in table.fields}
        for uk in table.unique_keys:
            for field_id in uk:
                # Known by construction; kept for defence in depth.
                if field_id not in fields:
                    raise CaptureSchemaHistoryError(
                        f"schema version {version.version}: unique key of "
                        f"{source_table!r} names unknown field {field_id!r}"
                    )
        for idx, fk in enumerate(table.foreign_keys):
            where = (f"schema version {version.version}: foreign key {idx} of "
                     f"{source_table!r}")
            local = fk["columns"]
            ref_cols = fk["ref_columns"]
            if not isinstance(ref_cols, (list, tuple)):
                raise CaptureSchemaHistoryError(
                    f"{where}: ref_columns must be an array of field IDs"
                )
            if len(local) != len(ref_cols):
                raise CaptureSchemaHistoryError(
                    f"{where}: column count mismatch"
                )
            ref = version.tables[fk["ref_table"]]
            for field_id in local:
                if field_id not in fields:
                    raise CaptureSchemaHistoryError(
                        f"{where}: unknown local field {field_id!r}"
                    )
            for field_id in ref_cols:
                if not isinstance(field_id, str) or field_id not in {
                    f.field_id for f in ref.fields
                }:
                    raise CaptureSchemaHistoryError(
                        f"{where}: unknown referenced field {field_id!r}"
                    )
            for lf, rf in zip(local, ref_cols):
                if fields[lf].type != ref.by_id(rf).type:
                    raise CaptureSchemaHistoryError(
                        f"{where}: local field {lf!r} type {fields[lf].type!r} "
                        f"does not match referenced {rf!r} type "
                        f"{ref.by_id(rf).type!r}"
                    )
            nullable = all(fields[f].nullable for f in local)
            if fk["nullable"] != nullable:
                raise CaptureSchemaHistoryError(
                    f"{where}: nullable flag {fk['nullable']} does not match "
                    f"the local column nullability ({nullable})"
                )
            ref_key = tuple(ref_cols)
            if ref_key != ref.primary_key and ref_key not in ref.unique_keys:
                raise CaptureSchemaHistoryError(
                    f"{where}: referenced fields {list(ref_key)} of "
                    f"{fk['ref_table']!r} are not a unique key"
                )


# ------------------------------------------------------- target binding
def effective_backfill(history: SchemaHistory, source_table: str,
                       field_id: str) -> tuple[bool, Any]:
    """Backfill declared when ``field_id`` was introduced (first appearance)."""
    for version in history.versions:
        table = version.tables[source_table]
        for field in table.fields:
            if field.field_id == field_id:
                return field.has_backfill, field.backfill
    raise CaptureSchemaHistoryError(
        f"field {field_id!r} on table {source_table!r} never appears in the "
        "schema history"
    )


def build_expected_schema(history: SchemaHistory) -> Schema:
    """The unified target schema implied by the last history version."""
    last = history.versions[-1]
    tables: dict[str, TableSchema] = {}
    for source_table, table in last.tables.items():
        columns = {
            f.name: Column(f.name, f.type, f.nullable) for f in table.fields
        }
        pk = tuple(table.by_id(f).name for f in table.primary_key)
        unique_keys = [
            tuple(table.by_id(f).name for f in uk) for uk in table.unique_keys
        ]
        if pk not in unique_keys:
            unique_keys.insert(0, pk)
        fks = []
        for fk in table.foreign_keys:
            ref_source = fk["ref_table"]
            ref_table = last.tables[ref_source]
            fks.append((
                tuple(table.by_id(f).name for f in fk["columns"]),
                ref_table.target_table,
                tuple(ref_table.by_id(f).name for f in fk["ref_columns"]),
                fk["nullable"],
            ))
        if table.target_table in tables:
            raise CaptureSchemaHistoryError(
                f"schema version {last.version}: target table "
                f"{table.target_table!r} is bound more than once"
            )
        tables[table.target_table] = (columns, pk, unique_keys, fks)
    return _schema_from_parts(tables)


def _schema_from_parts(parts: dict) -> Schema:
    from ..schema import ForeignKey

    tables: dict[str, TableSchema] = {}
    for name, (columns, pk, unique_keys, fks) in parts.items():
        tables[name] = TableSchema(
            name=name,
            columns=columns,
            primary_key=pk,
            unique_keys=tuple(unique_keys),
            foreign_keys=[
                ForeignKey(columns=c, ref_table=rt, ref_columns=rc, nullable=n)
                for c, rt, rc, n in fks
            ],
        )
    return Schema(tables=tables)


def validate_against_target(history: SchemaHistory, schema: Schema,
                            manifest=None) -> None:
    """Bind the history's final version to the user-supplied target schema."""
    last = history.versions[-1]
    target_tables = {t.target_table for t in last.tables.values()}
    if set(schema.tables) != target_tables:
        raise CaptureSchemaHistoryError(
            "schema history target tables do not match the target schema: "
            f"history={sorted(target_tables)}, schema={sorted(schema.tables)}"
        )
    expected = build_expected_schema(history)
    for source_table, table in last.tables.items():
        name = table.target_table
        target = schema.tables[name]
        field_targets = {f.name: f for f in table.fields}
        if set(target.columns) != set(field_targets):
            raise CaptureSchemaHistoryError(
                f"target table {name!r}: schema columns "
                f"{sorted(target.columns)} do not match the final schema "
                f"version's field bindings {sorted(field_targets)}"
            )
        for col_name, field in field_targets.items():
            col = target.columns[col_name]
            if col.type != field.type:
                raise CaptureSchemaHistoryError(
                    f"target table {name!r}.{col_name}: schema type {col.type!r} "
                    f"does not match field {field.field_id!r} type {field.type!r}"
                )
            if col.nullable != field.nullable:
                raise CaptureSchemaHistoryError(
                    f"target table {name!r}.{col_name}: schema nullability "
                    f"{col.nullable} does not match field {field.field_id!r} "
                    f"nullability {field.nullable}"
                )
    # PK / unique / FK must agree after the field-ID -> column-name rename.
    exp = expected.canonical()
    got = schema.canonical()
    for name in sorted(exp):
        for section in ("primary_key", "unique_keys", "foreign_keys"):
            if exp[name][section] != got[name][section]:
                raise CaptureSchemaHistoryError(
                    f"target table {name!r}: {section} {got[name][section]} "
                    f"does not match the final schema version's "
                    f"{exp[name][section]} (field identities are not preserved)"
                )
