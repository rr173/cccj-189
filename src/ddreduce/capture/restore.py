"""Row-state restoration from de-duplicated capture events.

The restorer consumes logical events in *commit order* (never transport
order) and rebuilds complete before/after row images in the target schema.

Source columns are renamed through the manifest bindings and every value is
checked with the existing typed-scalar rules.  Missing columns mean
*preserve*; an explicit JSON ``null`` means *write null* - the two are never
confused.  Update events may carry partial images; an empty ``after`` is
still an update and must not be dropped.

Cross-row constraints (unique keys, foreign keys) are evaluated only when the
whole transaction commits, reusing the existing schema semantics: a
transaction may temporarily violate a constraint across its own rows (for
example swapping two unique values) as long as the committed state is legal.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any

from ..model import Row
from ..schema import Schema, TableSchema
from ..values import (
    DataError,
    canonical_scalar,
    encode_key_values,
    make_row_id,
    normalize_scalar,
)
from .errors import CaptureRestoreError
from .manifest import Bindings, Manifest
from .store import LogicalEvent


def source_primary_key(
    manifest: Manifest, source_table: str, target_table: str, schema: Schema
) -> tuple[str, ...]:
    """Source-level primary key columns for a bound table (target PK order)."""
    binding = manifest.bindings.tables[source_table]
    if binding.target_table != target_table:
        raise CaptureRestoreError(
            f"binding inconsistency: {source_table!r} maps to "
            f"{binding.target_table!r}, not {target_table!r}"
        )
    target_to_source = binding.target_to_source_columns
    return tuple(
        target_to_source[col] for col in schema.tables[target_table].primary_key
    )


@dataclass(frozen=True)
class OutputEvent:
    """One old-format event (full images), plus full capture provenance."""

    op: str
    table: str
    key: dict[str, Any]
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    source: "EventSource"


@dataclass(frozen=True)
class EventSource:
    tx_id: str
    tx_order: int
    source_table: str
    op: str
    coordinates: tuple  # tuple[Coordinate]
    expanded_from_key_move: bool = False


class _RowStore:
    """In-memory current row state keyed by (target table, pk token)."""

    def __init__(self, schema: Schema, snapshot_rows: list[Row]):
        self.schema = schema
        self.entries: dict[tuple[str, str], Row] = {}
        for row in snapshot_rows:
            token = self.pk_token(row.table, row.data)
            self.entries[(row.table, token)] = row

    def pk_token(self, table_name: str, row: dict[str, Any]) -> str:
        table = self.schema.tables[table_name]
        return encode_key_values(
            [row[c] for c in table.primary_key], table.column_types(table.primary_key)
        )

    def lookup(
        self,
        overlay: dict[tuple[str, str], Row | None],
        table: str,
        token: str,
    ) -> Row | None:
        """``None`` overlay value means an explicit absence (deleted key).

        An overlay entry whose value is ``None`` must NOT fall through to the
        global store: the key was deleted inside the current transaction and
        stays absent until a later event of the same transaction re-inserts it.
        """
        key = (table, token)
        if key in overlay:
            return overlay[key]
        return self.entries.get(key)

    def merged(self, overlay: dict[tuple[str, str], Row | None]) -> "_RowStore":
        out = _RowStore.__new__(_RowStore)
        out.schema = self.schema
        merged = dict(self.entries)
        for key, value in overlay.items():
            if value is None:
                merged.pop(key, None)
            else:
                merged[key] = value
        out.entries = merged
        return out

    def rows_of(self, table: str) -> list[Row]:
        return [r for r in self.entries.values() if r.table == table]


class Restorer:
    """Applies one transaction's logical events against in-memory row state."""

    def __init__(self, manifest: Manifest, schema: Schema, snapshot_rows: list[Row]):
        self.manifest = manifest
        self.schema = schema
        self.bindings: Bindings = manifest.bindings
        self.store = _RowStore(schema, snapshot_rows)
        # Target table -> source table, for provenance/error reporting.
        self.source_of_target = {
            b.target_table: b.source_table for b in self.bindings.tables.values()
        }

    # ------------------------------------------------------------ mappings
    def source_pk(self, source_table: str) -> tuple[str, ...]:
        binding = self.bindings.tables[source_table]
        target_table = self.schema.tables[binding.target_table]
        return tuple(
            binding.target_to_source_columns[col] for col in target_table.primary_key
        )

    def _target_table(self, source_table: str) -> TableSchema:
        binding = self.bindings.tables[source_table]
        return self.schema.tables[binding.target_table]

    def _convert_partial(
        self,
        source_table: str,
        image: dict[str, Any],
        where: str,
        *,
        allow_unknown: bool = False,
    ) -> dict[str, Any]:
        """Rename a partial source image into target columns; validate types."""
        binding = self.bindings.tables[source_table]
        table = self.schema.tables[binding.target_table]
        unknown = set(image) - set(binding.columns)
        if unknown:
            raise CaptureRestoreError(
                f"{where}: unknown source columns {sorted(unknown)} on "
                f"{source_table!r}"
            )
        out: dict[str, Any] = {}
        for source_col, value in image.items():
            target_col = binding.columns[source_col]
            column = table.columns[target_col]
            if value is None:
                if not column.nullable:
                    raise CaptureRestoreError(
                        f"{where}: explicit null in NOT NULL column "
                        f"{table.name}.{target_col!r}"
                    )
                out[target_col] = None
                continue
            try:
                out[target_col] = normalize_scalar(value, column.type)
            except DataError as exc:
                raise CaptureRestoreError(
                    f"{where}: column {table.name}.{target_col!r}: {exc}"
                ) from exc
        return out

    def _convert_key(self, source_table: str, source_key: dict, where: str) -> dict[str, Any]:
        binding = self.bindings.tables[source_table]
        table = self.schema.tables[binding.target_table]
        if set(source_key) != set(self.source_pk(source_table)):
            raise CaptureRestoreError(
                f"{where}: key columns {sorted(source_key)} do not match the source "
                f"primary key {sorted(self.source_pk(source_table))}"
            )
        target_key: dict[str, Any] = {}
        for source_col, value in source_key.items():
            target_col = binding.columns[source_col]
            try:
                target_key[target_col] = normalize_scalar(
                    value, table.columns[target_col].type
                )
            except DataError as exc:
                raise CaptureRestoreError(
                    f"{where}: key column {table.name}.{target_col!r}: {exc}"
                ) from exc
        return target_key

    def _make_row(self, table_name: str, data: dict[str, Any]) -> Row:
        table = self.schema.tables[table_name]
        return Row(
            table=table_name,
            row_id=make_row_id(table_name, table.pk_types, data),
            data=data,
        )

    def _full_target_row(
        self, source_table: str, source_row: dict[str, Any], where: str
    ) -> dict[str, Any]:
        """Convert a complete source row image into a complete target row."""
        binding = self.bindings.tables[source_table]
        table = self.schema.tables[binding.target_table]
        missing = set(binding.columns) - set(source_row)
        if missing:
            raise CaptureRestoreError(
                f"{where}: incomplete source image, missing columns {sorted(missing)}"
            )
        target = self._convert_partial(source_table, source_row, where)
        # Complete image: every target column must be present (nullable may be null).
        absent = set(table.columns) - set(target)
        for col_name in absent:
            if not table.columns[col_name].nullable:
                raise CaptureRestoreError(
                    f"{where}: missing NOT NULL column {table.name}.{col_name!r}"
                )
            target[col_name] = None
        return target

    # ------------------------------------------------------------- applying
    def apply_transaction(
        self, tx_id: str, events: list[LogicalEvent]
    ) -> list[OutputEvent]:
        """Apply one committed transaction; return old-format full-image events.

        Raises :class:`CaptureRestoreError` naming the transaction and logical
        order on the first failure; nothing is committed to the global store.
        """
        overlay: dict[tuple[str, str], Row | None] = {}
        outputs: list[OutputEvent] = []
        for event in events:
            self._apply_event(tx_id, event, overlay, outputs)
        merged = self.store.merged(overlay)
        self._check_commit(tx_id, merged, overlay)
        self.store = merged
        return outputs

    def _apply_event(
        self,
        tx_id: str,
        event: LogicalEvent,
        overlay: dict[tuple[str, str], Row | None],
        outputs: list[OutputEvent],
    ) -> None:
        where = f"tx {tx_id!r} tx_order {event.tx_order}"
        source_table = event.table
        if source_table not in self.bindings.tables:
            raise CaptureRestoreError(
                f"{where}: source table {source_table!r} has no binding"
            )
        table = self._target_table(source_table)
        target_key = self._convert_key(source_table, event.key, where)
        token = encode_key_values(
            [target_key[c] for c in table.primary_key],
            table.column_types(table.primary_key),
        )
        cur = self.store.lookup(overlay, table.name, token)
        source = EventSource(
            tx_id=tx_id,
            tx_order=event.tx_order,
            source_table=source_table,
            op=event.op,
            coordinates=event.coordinates,
        )

        if event.op == "insert":
            if cur is not None:
                raise CaptureRestoreError(
                    f"{where}: insert requires an absent key "
                    f"{table.name}{target_key!r}, but a row exists"
                )
            target_after = self._full_target_row(source_table, event.after, where)
            insert_key_token = encode_key_values(
                [target_after[c] for c in table.primary_key],
                table.column_types(table.primary_key),
            )
            if insert_key_token != token:
                raise CaptureRestoreError(
                    f"{where}: insert after-image key does not match event key"
                )
            row = self._make_row(table.name, target_after)
            overlay[(table.name, token)] = row
            outputs.append(
                OutputEvent(
                    op="insert", table=table.name, key=target_key,
                    before=None, after=target_after, source=source,
                )
            )
            return

        if cur is None:
            raise CaptureRestoreError(
                f"{where}: {event.op} requires an existing row at "
                f"{table.name}{target_key!r}, but the key is absent"
            )

        if event.op == "delete":
            before_partial = self._convert_partial(
                source_table, event.before, f"{where} before"
            )
            # Partial before assertions: every named column must match.
            for col_name, value in before_partial.items():
                if not _scalar_equal(
                    cur.data[col_name], value, table.columns[col_name].type
                ):
                    raise CaptureRestoreError(
                        f"{where}: delete before-image mismatch on column "
                        f"{col_name!r}: expected {cur.data[col_name]!r}, got "
                        f"{value!r}"
                    )
            full_before = copy.deepcopy(cur.data)
            overlay[(table.name, token)] = None
            outputs.append(
                OutputEvent(
                    op="delete", table=table.name, key=target_key,
                    before=full_before, after=None, source=source,
                )
            )
            return

        # update -----------------------------------------------------------
        assert event.op == "update"
        before_partial = self._convert_partial(
            source_table, event.before, f"{where} before"
        )
        for col_name, value in before_partial.items():
            if not _scalar_equal(
                cur.data[col_name], value, table.columns[col_name].type
            ):
                raise CaptureRestoreError(
                    f"{where}: stale before-image on column {col_name!r}: current "
                    f"value {cur.data[col_name]!r} does not match asserted {value!r}"
                )
        after_partial = self._convert_partial(
            source_table, event.after, f"{where} after"
        )

        if event.new_key is None:
            # Ordinary update: the primary key stays immutable.
            for pk_col in table.primary_key:
                if pk_col in after_partial:
                    if not _scalar_equal(
                        cur.data[pk_col],
                        after_partial[pk_col],
                        table.columns[pk_col].type,
                    ):
                        raise CaptureRestoreError(
                            f"{where}: update changes primary-key column "
                            f"{pk_col!r} without new_key"
                        )
            full_before = copy.deepcopy(cur.data)
            full_after = copy.deepcopy(cur.data)
            full_after.update(after_partial)
            self._validate_full_row(table, full_after, where)
            overlay[(table.name, token)] = self._make_row(table.name, full_after)
            outputs.append(
                OutputEvent(
                    op="update", table=table.name, key=target_key,
                    before=full_before, after=full_after, source=source,
                )
            )
            return

        # Key move: explicit new_key expands into adjacent delete + insert.
        new_target_key = self._convert_key(source_table, event.new_key, where)
        # after key values, when present, must agree with new_key.
        for pk_col in table.primary_key:
            if pk_col in after_partial:
                if not _scalar_equal(
                    after_partial[pk_col],
                    new_target_key[pk_col],
                    table.columns[pk_col].type,
                ):
                    raise CaptureRestoreError(
                        f"{where}: after-image key column {pk_col!r} disagrees "
                        "with new_key"
                    )
        new_token = encode_key_values(
            [new_target_key[c] for c in table.primary_key],
            table.column_types(table.primary_key),
        )
        key_changes = new_token != token
        if key_changes:
            target_at_new = self.store.lookup(overlay, table.name, new_token)
            if target_at_new is not None:
                raise CaptureRestoreError(
                    f"{where}: key move target {table.name}{new_target_key!r} "
                    "already exists; refusing to overwrite"
                )
        full_before = copy.deepcopy(cur.data)
        full_after = copy.deepcopy(cur.data)
        full_after.update(after_partial)
        for pk_col in table.primary_key:
            full_after[pk_col] = new_target_key[pk_col]
        self._validate_full_row(table, full_after, where)

        if not key_changes:
            # new_key equal to the old key: still one ordinary update.
            overlay[(table.name, token)] = self._make_row(table.name, full_after)
            outputs.append(
                OutputEvent(
                    op="update", table=table.name, key=target_key,
                    before=full_before, after=full_after, source=source,
                )
            )
            return

        # Expand at the same logical position into delete(old) then insert(new).
        overlay[(table.name, token)] = None
        overlay[(table.name, new_token)] = self._make_row(table.name, full_after)
        move_source = EventSource(
            tx_id=tx_id,
            tx_order=event.tx_order,
            source_table=source_table,
            op=event.op,
            coordinates=event.coordinates,
            expanded_from_key_move=True,
        )
        outputs.append(
            OutputEvent(
                op="delete", table=table.name, key=target_key,
                before=full_before, after=None, source=move_source,
            )
        )
        outputs.append(
            OutputEvent(
                op="insert", table=table.name, key=new_target_key,
                before=None, after=full_after, source=move_source,
            )
        )

    # ---------------------------------------------------------- commit checks
    def _validate_full_row(
        self, table: TableSchema, row: dict[str, Any], where: str
    ) -> None:
        unknown = set(row) - set(table.columns)
        if unknown:
            raise CaptureRestoreError(
                f"{where}: unknown target columns {sorted(unknown)} on {table.name!r}"
            )
        for col in table.columns.values():
            if col.name not in row:
                if not col.nullable:
                    raise CaptureRestoreError(
                        f"{where}: missing NOT NULL column {table.name}.{col.name!r}"
                    )
                continue
            value = row[col.name]
            if value is None:
                if not col.nullable:
                    raise CaptureRestoreError(
                        f"{where}: null in NOT NULL column {table.name}.{col.name!r}"
                    )
                continue
            try:
                normalize_scalar(value, col.type)
            except DataError as exc:
                raise CaptureRestoreError(
                    f"{where}: column {table.name}.{col.name!r}: {exc}"
                ) from exc

    def _check_commit(
        self,
        tx_id: str,
        merged: "_RowStore",
        overlay: dict[tuple[str, str], Row | None],
    ) -> None:
        """Uniqueness and foreign keys over committed state, touched tables only.

        Constraints may temporarily fail between events of the same
        transaction; only the state at commit is judged.
        """
        touched_tables = {key[0] for key in overlay}
        where = f"tx {tx_id!r} commit"
        for table_name in sorted(touched_tables):
            table = self.schema.tables[table_name]
            seen_unique: dict[tuple[str, ...], set[str]] = {
                k: set() for k in table.unique_keys
            }
            for row in merged.rows_of(table_name):
                for key in table.unique_keys:
                    token = _unique_token(table, key, row)
                    if token is None:
                        continue
                    if token in seen_unique[key]:
                        raise CaptureRestoreError(
                            f"{where}: unique constraint violation on "
                            f"{table_name}{list(key)} at key token {token}"
                        )
                    seen_unique[key].add(token)
        # Foreign keys need whole-schema parent lookups; build them once.
        parent_index = self._parent_lookup(merged)
        for (table_name, _pk), entry in overlay.items():
            if entry is None:
                continue
            table = self.schema.tables[table_name]
            for fk in table.foreign_keys:
                local_values = [entry.data.get(c) for c in fk.columns]
                if any(v is None for v in local_values):
                    if not fk.nullable:
                        raise CaptureRestoreError(
                            f"{where}: null in NOT NULL foreign key "
                            f"{table_name}{list(fk.columns)}"
                        )
                    continue
                token = encode_key_values(
                    local_values, table.column_types(tuple(fk.columns))
                )
                if token not in parent_index[(fk.ref_table, tuple(fk.ref_columns))]:
                    if fk.nullable:
                        continue
                    raise CaptureRestoreError(
                        f"{where}: foreign key {table_name}{list(fk.columns)} -> "
                        f"{fk.ref_table}{list(fk.ref_columns)} has no parent "
                        f"({token})"
                    )

    def _parent_lookup(
        self, merged: "_RowStore"
    ) -> dict[tuple[str, tuple[str, ...]], set[str]]:
        index: dict[tuple[str, tuple[str, ...]], set[str]] = {}
        for table_name, table in self.schema.tables.items():
            for key in table.unique_keys:
                index[(table_name, tuple(key))] = set()
            for row in merged.rows_of(table_name):
                for key in table.unique_keys:
                    token = _unique_token(table, tuple(key), row)
                    if token is not None:
                        index[(table_name, tuple(key))].add(token)
        return index


def _unique_token(
    table: TableSchema, key: tuple[str, ...], row: Row
) -> str | None:
    values, types = [], []
    for col in key:
        val = row.data.get(col)
        if val is None:
            return None  # SQL-style: any-NULL unique key is exempt
        values.append(val)
        types.append(table.columns[col].type)
    return encode_key_values(values, types)


def _scalar_equal(a: Any, b: Any, declared: str) -> bool:
    try:
        return canonical_scalar(a, declared) == canonical_scalar(b, declared)
    except DataError:
        return a == b
