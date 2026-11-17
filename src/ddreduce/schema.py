"""Relational schema model: tables, typed columns, primary/unique keys and FKs."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from .values import SCALAR_TYPES, SchemaError


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool


@dataclass(frozen=True)
class ForeignKey:
    columns: tuple[str, ...]
    ref_table: str
    ref_columns: tuple[str, ...]
    nullable: bool  # true iff EVERY local column is nullable


@dataclass
class TableSchema:
    name: str
    columns: dict[str, Column]
    primary_key: tuple[str, ...]
    unique_keys: tuple[tuple[str, ...], ...]
    foreign_keys: list[ForeignKey] = field(default_factory=list)

    @property
    def pk_types(self) -> list[tuple[str, str]]:
        return [(c, self.columns[c].type) for c in self.primary_key]

    def column_types(self, names: tuple[str, ...]) -> list[str]:
        return [self.columns[n].type for n in names]


@dataclass
class Schema:
    tables: dict[str, TableSchema]

    def canonical(self) -> dict:
        """A stable, hashable representation of the whole schema."""
        out = {}
        for name in sorted(self.tables):
            t = self.tables[name]
            out[name] = {
                "columns": [
                    {"name": c.name, "type": c.type, "nullable": c.nullable}
                    for c in sorted(t.columns.values(), key=lambda c: c.name)
                ],
                "primary_key": list(t.primary_key),
                "unique_keys": [list(k) for k in t.unique_keys],
                "foreign_keys": [
                    {
                        "columns": list(f.columns),
                        "ref_table": f.ref_table,
                        "ref_columns": list(f.ref_columns),
                        "nullable": f.nullable,
                    }
                    for f in t.foreign_keys
                ],
            }
        return out


def load_schema(path: str | Path) -> Schema:
    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    if not isinstance(raw, dict) or "tables" not in raw or not isinstance(raw["tables"], list):
        raise SchemaError("schema file must be an object with a 'tables' list")

    tables: dict[str, TableSchema] = {}
    for item in raw["tables"]:
        t = _parse_table(item)
        if t.name in tables:
            raise SchemaError(f"duplicate table definition: {t.name}")
        tables[t.name] = t

    schema = Schema(tables=tables)
    _validate_references(schema)
    return schema


def _parse_table(item: object) -> TableSchema:
    if not isinstance(item, dict) or "name" not in item or "columns" not in item:
        raise SchemaError("each table needs 'name' and 'columns'")
    name = item["name"]
    if not isinstance(name, str) or not name:
        raise SchemaError("table name must be a non-empty string")

    columns: dict[str, Column] = {}
    for c in item["columns"]:
        if not isinstance(c, dict) or "name" not in c or "type" not in c:
            raise SchemaError(f"table {name!r}: column needs name and type")
        cname, ctype = c["name"], c["type"]
        if ctype not in SCALAR_TYPES:
            raise SchemaError(f"table {name!r}: unsupported type {ctype!r}")
        if ctype == "null":
            raise SchemaError(f"table {name!r}: column {cname!r} cannot be typed null")
        if cname in columns:
            raise SchemaError(f"table {name!r}: duplicate column {cname!r}")
        columns[cname] = Column(cname, ctype, bool(c.get("nullable", False)))

    pk = tuple(item.get("primary_key", ()))
    if not pk:
        raise SchemaError(f"table {name!r}: primary_key is required and non-empty")
    for col in pk:
        if col not in columns:
            raise SchemaError(f"table {name!r}: unknown PK column {col!r}")
        if columns[col].nullable:
            raise SchemaError(f"table {name!r}: PK column {col!r} must not be nullable")

    unique_keys: list[tuple[str, ...]] = []
    for key in item.get("unique_keys", []):
        if not isinstance(key, list) or not key:
            raise SchemaError(f"table {name!r}: unique keys must be non-empty lists")
        uk = tuple(key)
        for col in uk:
            if col not in columns:
                raise SchemaError(f"table {name!r}: unknown unique-key column {col!r}")
        if uk in unique_keys:
            raise SchemaError(f"table {name!r}: duplicate unique key {uk}")
        unique_keys.append(uk)
    # The primary key is itself a uniqueness constraint.
    if pk not in unique_keys:
        unique_keys.insert(0, pk)

    fks: list[ForeignKey] = []
    for fk in item.get("foreign_keys", []):
        if not isinstance(fk, dict):
            raise SchemaError(f"table {name!r}: malformed foreign key")
        cols = fk.get("columns")
        ref_table = fk.get("ref_table")
        ref_cols = fk.get("ref_columns")
        if not (isinstance(cols, list) and isinstance(ref_cols, list) and cols and ref_cols):
            raise SchemaError(f"table {name!r}: FK needs columns/ref_table/ref_columns")
        if len(cols) != len(ref_cols):
            raise SchemaError(f"table {name!r}: FK column count mismatch")
        for col in cols:
            if col not in columns:
                raise SchemaError(f"table {name!r}: unknown FK column {col!r}")
        if len(set(cols)) != len(cols):
            raise SchemaError(f"table {name!r}: FK columns must be distinct")
        nullable = all(columns[c].nullable for c in cols)
        fks.append(
            ForeignKey(
                columns=tuple(cols),
                ref_table=ref_table,
                ref_columns=tuple(ref_cols),
                nullable=nullable,
            )
        )

    return TableSchema(
        name=name,
        columns=columns,
        primary_key=pk,
        unique_keys=tuple(unique_keys),
        foreign_keys=fks,
    )


def _validate_references(schema: Schema) -> None:
    for t in schema.tables.values():
        for fk in t.foreign_keys:
            if fk.ref_table not in schema.tables:
                raise SchemaError(
                    f"table {t.name!r}: FK references unknown table {fk.ref_table!r}"
                )
            ref = schema.tables[fk.ref_table]
            for local, remote in zip(fk.columns, fk.ref_columns):
                if remote not in ref.columns:
                    raise SchemaError(
                        f"table {t.name!r}: FK references unknown column "
                        f"{fk.ref_table}.{remote}"
                    )
                if t.columns[local].type != ref.columns[remote].type:
                    raise SchemaError(
                        f"table {t.name!r}.{local} and {fk.ref_table}.{remote} "
                        "have mismatched types"
                    )
            # FK targets must be unique on the referenced side.
            if fk.ref_columns not in ref.unique_keys:
                raise SchemaError(
                    f"table {t.name!r}: referenced columns {list(fk.ref_columns)} "
                    f"on {fk.ref_table!r} are not a unique key"
                )
