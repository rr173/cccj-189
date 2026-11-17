"""In-memory relational database: JSONL loading, constraint validation, materialization."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .schema import Schema, TableSchema
from .values import DataError, canonical_json, encode_key_values, make_row_id, normalize_scalar


@dataclass(frozen=True)
class Row:
    table: str
    row_id: str
    data: dict[str, Any]  # full JSON object as read from the JSONL line


class Database:
    """A validated set of rows plus precomputed dependency edges.

    Edges follow required (non-nullable) foreign keys only: a child row points
    at the parent rows it cannot exist without.  Nullable FKs never create an
    edge, even when currently populated, because the contract permits setting
    them to NULL semantically — and this reducer may only delete rows, never
    edit columns, so a child whose FK is nullable stays valid after its parent
    disappears (the reference simply becomes dangling-nullable, which the
    contract explicitly allows).
    """

    def __init__(self, schema: Schema, rows: Iterable[Row]):
        self.schema = schema
        self.rows: dict[str, Row] = {}
        for row in rows:
            if row.row_id in self.rows:
                raise DataError(f"duplicate row id {row.row_id!r}")
            self.rows[row.row_id] = row
        # row_id -> set of row_ids that depend on it (required-FK reverse edges)
        self.children: dict[str, set[str]] = {rid: set() for rid in self.rows}
        self._build_edges()

    # ------------------------------------------------------------------ load
    @classmethod
    def load(cls, schema: Schema, input_dir: str | Path) -> "Database":
        input_dir = Path(input_dir)
        if not input_dir.is_dir():
            raise DataError(f"input directory not found: {input_dir}")
        rows: list[Row] = []
        for table_name in sorted(schema.tables):
            table = schema.tables[table_name]
            path = input_dir / f"{table_name}.jsonl"
            if not path.exists():
                raise DataError(f"missing table file: {path}")
            with open(path, "r", encoding="utf-8") as fh:
                for line_no, line in enumerate(fh, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise DataError(
                            f"{path.name}:{line_no}: invalid JSON ({exc})"
                        ) from exc
                    if not isinstance(obj, dict):
                        raise DataError(f"{path.name}:{line_no}: row must be a JSON object")
                    rows.append(_materialize_row(table, obj))
        db = cls(schema, rows)
        db.validate()
        return db

    # -------------------------------------------------------------- validation
    def validate(self) -> None:
        """Run all uniqueness and foreign-key checks over current rows."""
        for table_name, table in self.schema.tables.items():
            seen_unique: dict[tuple[str, ...], set[str]] = {
                k: set() for k in table.unique_keys
            }
            for row in self._rows_of(table_name):
                _check_columns(table, row)
                for key in table.unique_keys:
                    token = self._unique_token(table, key, row)
                    if token is not None and token in seen_unique[key]:
                        raise DataError(
                            f"unique constraint violation on {table_name}{list(key)}: "
                            f"{row.row_id}"
                        )
                    if token is not None:
                        seen_unique[key].add(token)
        self._check_foreign_keys()

    def _rows_of(self, table_name: str) -> list[Row]:
        return [r for r in self.rows.values() if r.table == table_name]

    @staticmethod
    def _unique_token(
        table: TableSchema, key: tuple[str, ...], row: Row
    ) -> str | None:
        """SQL-style uniqueness: NULLs are distinct, so any-NULL key is exempt."""
        values, types = [], []
        for col in key:
            val = row.data.get(col)
            if val is None:
                if not table.columns[col].nullable:
                    raise DataError(f"{row.row_id}: null in NOT NULL column {col!r}")
                return None
            values.append(val)
            types.append(table.columns[col].type)
        return encode_key_values(values, types)

    def _check_foreign_keys(self) -> None:
        index = self._parent_lookup()
        for row in self.rows.values():
            table = self.schema.tables[row.table]
            for fk in table.foreign_keys:
                local_values = [row.data.get(c) for c in fk.columns]
                if any(v is None for v in local_values):
                    if not fk.nullable:
                        raise DataError(
                            f"{row.row_id}: null in NOT NULL foreign key {list(fk.columns)}"
                        )
                    # Nullable FK with NULL (or partially NULL) => no dependency.
                    continue
                token = encode_key_values(
                    local_values, table.column_types(fk.columns)
                )
                if token not in index[(fk.ref_table, fk.ref_columns)]:
                    if fk.nullable:
                        # The reducer may only delete rows, never edit columns:
                        # when a parent is deleted, a nullable-FK child stays
                        # and its reference becomes dangling. The contract says
                        # nullable FKs create no dependency, so this is legal.
                        continue
                    raise DataError(
                        f"{row.row_id}: foreign key {list(fk.columns)} -> "
                        f"{fk.ref_table}{list(fk.ref_columns)} has no parent ({token})"
                    )

    def _parent_lookup(
        self,
    ) -> dict[tuple[str, tuple[str, ...]], set[str]]:
        index: dict[tuple[str, tuple[str, ...]], set[str]] = {}
        for table_name, table in self.schema.tables.items():
            for key in table.unique_keys:
                index[(table_name, key)] = set()
            for row in self._rows_of(table_name):
                for key in table.unique_keys:
                    token = self._unique_token(table, key, row)
                    if token is not None:
                        index[(table_name, key)].add(token)
        return index

    # ------------------------------------------------------------------- edges
    def _build_edges(self) -> None:
        """Create child->parent dependency edges for populated required FKs."""
        index = self._parent_lookup()
        # Map parent token back to parent row id.
        parent_rows: dict[tuple[str, tuple[str, ...]], dict[str, str]] = {}
        for table_name, table in self.schema.tables.items():
            for row in self._rows_of(table_name):
                for key in table.unique_keys:
                    token = self._unique_token(table, key, row)
                    if token is None:
                        continue
                    parent_rows.setdefault((table_name, key), {})[token] = row.row_id

        for row in self.rows.values():
            table = self.schema.tables[row.table]
            for fk in table.foreign_keys:
                if fk.nullable:
                    continue  # nullable FK never creates a hard dependency
                local_values = [row.data.get(c) for c in fk.columns]
                if any(v is None for v in local_values):
                    continue  # validated as illegal for NOT NULL FKs later
                token = encode_key_values(
                    local_values, table.column_types(fk.columns)
                )
                if token not in index[(fk.ref_table, fk.ref_columns)]:
                    continue  # invalid DB; validate() reports it
                parent_id = parent_rows[(fk.ref_table, fk.ref_columns)][token]
                if parent_id != row.row_id:  # self-FK row is its own parent
                    self.children[parent_id].add(row.row_id)

    def required_parents(self, row_id: str) -> set[str]:
        """Reverse of ``children``: parent row ids this row hard-depends on."""
        out: set[str] = set()
        for parent, kids in self.children.items():
            if row_id in kids:
                out.add(parent)
        return out

    # ------------------------------------------------------------- closures
    def deletion_closure(self, seeds: Iterable[str]) -> set[str]:
        """Legal deletion set for ``seeds`` (row ids).

        Remove the seeds, then every child that loses a *required* parent,
        iterating to a fixpoint.  Cycles are handled as a whole closure via
        reverse-edge reachability: if any member of an FK cycle is removed,
        every transitively dependent member is removed too.
        """
        removed: set[str] = set()
        work = [s for s in seeds if s in self.rows and s not in removed]
        while work:
            current = work.pop()
            if current in removed:
                continue
            removed.add(current)
            for child in sorted(self.children.get(current, ())):
                if child not in removed:
                    work.append(child)
        return removed

    def apply_deletion(self, removed: set[str]) -> "Database":
        remaining = [r for rid, r in self.rows.items() if rid not in removed]
        return Database(self.schema, remaining)

    def subset(self, keep_ids: Iterable[str]) -> "Database":
        keep = set(keep_ids)
        return self.apply_deletion(set(self.rows) - keep)

    # -------------------------------------------------------- materialization
    def materialize(self, target_dir: str | Path) -> Path:
        """Write one JSONL file per schema table (empty file for empty tables)."""
        target_dir = Path(target_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        by_table: dict[str, list[Row]] = {t: [] for t in self.schema.tables}
        for row in sorted(self.rows.values(), key=lambda r: r.row_id):
            by_table[row.table].append(row)
        for table_name in sorted(self.schema.tables):
            path = target_dir / f"{table_name}.jsonl"
            with open(path, "w", encoding="utf-8") as fh:
                for row in by_table[table_name]:
                    fh.write(json.dumps(row.data, ensure_ascii=False, sort_keys=True))
                    fh.write("\n")
        return target_dir

    def content_hash(self) -> str:
        """Hash bound to the exact rows (schema is hashed separately by callers)."""
        payload = [
            [row.table, row.row_id, canonical_json(row.data)]
            for row in sorted(self.rows.values(), key=lambda r: r.row_id)
        ]
        digest = hashlib.sha256(canonical_json(payload).encode("utf-8"))
        return digest.hexdigest()

    def table_counts(self) -> dict[str, int]:
        counts = {t: 0 for t in self.schema.tables}
        for row in self.rows.values():
            counts[row.table] += 1
        return counts

    def __len__(self) -> int:
        return len(self.rows)


def _materialize_row(table: TableSchema, obj: dict[str, Any]) -> Row:
    return Row(
        table=table.name,
        row_id=make_row_id(table.name, table.pk_types, obj),
        data=obj,
    )


def _check_columns(table: TableSchema, row: Row) -> None:
    unknown = set(row.data) - set(table.columns)
    if unknown:
        raise DataError(f"{row.row_id}: unknown columns {sorted(unknown)}")
    for col in table.columns.values():
        if col.name not in row.data:
            if col.nullable:
                continue
            raise DataError(f"{row.row_id}: missing NOT NULL column {col.name!r}")
        value = row.data[col.name]
        if value is None:
            if not col.nullable:
                raise DataError(f"{row.row_id}: null in NOT NULL column {col.name!r}")
            continue
        normalize_scalar(value, col.type)


def all_legal_deletions(db: Database) -> list[set[str]]:
    """Every distinct legal deletion closure (small-data exhaustive helper).

    Returns closure sets including the empty set, deduplicated, used by tests
    to enumerate certificates by hand on small fixtures.
    """
    ids = sorted(db.rows)
    seen: set[frozenset[str]] = set()
    result: list[set[str]] = []
    for mask in range(1 << len(ids)):
        seeds = [ids[i] for i in range(len(ids)) if mask & (1 << i)]
        closure = frozenset(db.deletion_closure(seeds))
        if closure not in seen:
            seen.add(closure)
            result.append(set(closure))
    return sorted(result, key=lambda s: (len(s), sorted(s)))
