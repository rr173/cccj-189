"""Version-aware replay of CDC change logs.

Causal model
------------
Every write in the *original* log creates a new row **version**, even when it
writes back a value identical to some older row — a version can never
impersonate another one.  Every delete creates a new **absence source** for
its key; the initial absence of a key is a different source again.

Version / absence identities are fixed by the baseline or by the *original*
transaction/event position and are never renumbered per candidate:

    B|<row_id>            baseline row version
    T|<tx_pos>|<ev_idx>   row version written by that original event
    A0|<table>|<pk>       initial absence of the key
    AD|<tx_pos>|<ev_idx>  absence produced by that original delete event

``build_witness`` replays the full original log from the fixed snapshot and
records, per transaction:

  * for every event, the exact row version (update/delete) or absence source
    (insert) it read;
  * for every inserted/updated row surviving at commit, the version of each
    *required* (non-nullable, non-null) foreign-key parent at commit time.

The original log itself must be fully legal: any event or commit violation is
reported with its transaction and event position and nothing else starts.

``normalize`` replays a *selection* of transactions in original order from the
same fixed snapshot.  A selected transaction commits only when every event
reads the identical version/absence as witnessed, every required parent
version at commit matches the witness, and all event and commit constraints
hold; otherwise the whole transaction is skipped (its private, uncommitted
writes stay invisible to later transactions).  Missing premises are never
rebound to same-key or same-value records, events are never inserted,
rewritten, split or reordered.  The result is the unique normalized candidate
for that selection: replaying the same selection twice yields the same
transaction subsequence and data state.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from .logmodel import ChangeLog, LogContractError, Transaction, canonical_tx_line, row_image_token
from .model import Database, Row
from .schema import Schema, TableSchema
from .values import DataError, canonical_json, encode_key_values


# ------------------------------------------------------------- ident helpers
def baseline_version(row_id: str) -> str:
    return f"B|{row_id}"


def tx_version(tx_pos: int, event_idx: int) -> str:
    return f"T|{tx_pos}|{event_idx}"


def init_absence(table: str, pk_token: str) -> str:
    return f"A0|{table}|{pk_token}"


def delete_absence(tx_pos: int, event_idx: int) -> str:
    return f"AD|{tx_pos}|{event_idx}"


def describe_ident(ident: str) -> str:
    """Human-readable, traceable description of a version/absence identity."""
    kind, _, rest = ident.partition("|")
    if kind == "B":
        return f"baseline row {rest}"
    if kind == "T":
        pos, _, idx = rest.partition("|")
        return f"row version written by tx position {pos} event {idx}"
    if kind == "A0":
        table, _, token = rest.partition("|")
        return f"initial absence of {table} key {token}"
    if kind == "AD":
        pos, _, idx = rest.partition("|")
        return f"absence from delete at tx position {pos} event {idx}"
    return ident


# ------------------------------------------------------------------ witness
@dataclass(frozen=True)
class EventWitness:
    event_idx: int
    read: str  # version id (update/delete) or absence id (insert)


@dataclass(frozen=True)
class ParentRef:
    """Required-FK parent version referenced by a surviving written row."""

    child_key: tuple[str, str]  # (table, pk_token)
    child_row_id: str
    child_version: str
    fk_columns: tuple[str, ...]
    ref_table: str
    ref_columns: tuple[str, ...]
    ref_token: str  # token of the referenced parent key (child image is fixed)
    parent_version: str


@dataclass(frozen=True)
class TxWitness:
    tx_pos: int
    tx_id: Any
    event_reads: tuple[EventWitness, ...]
    parent_refs: tuple[ParentRef, ...]


@dataclass(frozen=True)
class StoreEntry:
    is_row: bool
    ident: str  # version id or absence id
    row: Row | None = None


def _absent_entry(table: str, pk_token: str) -> StoreEntry:
    return StoreEntry(is_row=False, ident=init_absence(table, pk_token))


class _Store:
    """Versioned store: (table, pk_token) -> StoreEntry."""

    def __init__(self, schema: Schema, snapshot: Database):
        self.schema = schema
        self.entries: dict[tuple[str, str], StoreEntry] = {}
        for row in snapshot.rows.values():
            token = self.pk_token(row.table, row.data)
            self.entries[(row.table, token)] = StoreEntry(
                is_row=True, ident=baseline_version(row.row_id), row=row
            )

    def pk_token(self, table_name: str, row: dict[str, Any]) -> str:
        table = self.schema.tables[table_name]
        return encode_key_values(
            [row[c] for c in table.primary_key], table.column_types(table.primary_key)
        )

    def lookup(
        self, overlay: dict[tuple[str, str], StoreEntry] | None, table: str, pk_token: str
    ) -> StoreEntry:
        key = (table, pk_token)
        if overlay is not None and key in overlay:
            return overlay[key]
        return self.entries.get(key, _absent_entry(table, pk_token))

    def merged(self, overlay: dict[tuple[str, str], StoreEntry]) -> "_Store":
        out = _Store.__new__(_Store)
        out.schema = self.schema
        out.entries = {**self.entries, **overlay}
        return out

    def rows(self) -> list[Row]:
        return [e.row for e in self.entries.values() if e.is_row]

    def database(self) -> Database:
        return Database(self.schema, self.rows())

    def unique_index(self) -> dict[tuple[str, tuple[str, ...]], dict[str, StoreEntry]]:
        """(table, unique key) -> token -> entry, for parent lookups."""
        index: dict[tuple[str, tuple[str, ...]], dict[str, StoreEntry]] = {}
        for table_name, table in self.schema.tables.items():
            for key in table.unique_keys:
                index.setdefault((table_name, key), {})
        for (table_name, _token), entry in self.entries.items():
            if not entry.is_row:
                continue
            table = self.schema.tables[table_name]
            for key in table.unique_keys:
                token = _unique_token(table, key, entry.row.data)
                if token is not None:
                    index[(table_name, key)][token] = entry
        return index


def _unique_token(table: TableSchema, key: tuple[str, ...], row: dict[str, Any]) -> str | None:
    values, types = [], []
    for col in key:
        val = row.get(col)
        if val is None:
            return None  # SQL-style: any-NULL unique key is exempt
        values.append(val)
        types.append(table.columns[col].type)
    return encode_key_values(values, types)


def _event_pk_token(schema: Schema, event) -> str:
    table = schema.tables[event.table]
    return encode_key_values(
        [event.key[c] for c in table.primary_key], table.column_types(table.primary_key)
    )


def _make_row(schema: Schema, table_name: str, data: dict[str, Any]) -> Row:
    from .values import make_row_id

    table = schema.tables[table_name]
    return Row(
        table=table_name,
        row_id=make_row_id(table_name, table.pk_types, data),
        data=data,
    )


def build_witness(
    schema: Schema, snapshot: Database, log: ChangeLog
) -> list[TxWitness]:
    """Replay the full original log; return per-transaction witnesses.

    Raises LogContractError naming the transaction and event position on the
    first event-level or commit-level violation: an illegal original log never
    reaches the oracle command.
    """
    store = _Store(schema, snapshot)
    witnesses: list[TxWitness] = []
    for tx in log:
        overlay: dict[tuple[str, str], StoreEntry] = {}
        reads: list[EventWitness] = []
        for ev_idx, ev in enumerate(tx.events):
            token = _event_pk_token(schema, ev)
            key = (ev.table, token)
            cur = store.lookup(overlay, ev.table, token)
            where = f"tx {tx.tx_id!r} (position {tx.position}) event {ev_idx}"
            if ev.op == "insert":
                if cur.is_row:
                    raise LogContractError(
                        f"{where}: insert requires an absent key, found "
                        f"{describe_ident(cur.ident)}"
                    )
                reads.append(EventWitness(ev_idx, cur.ident))
                overlay[key] = StoreEntry(
                    is_row=True,
                    ident=tx_version(tx.position, ev_idx),
                    row=_make_row(schema, ev.table, ev.after),
                )
            else:
                image = ev.before
                if not cur.is_row:
                    raise LogContractError(
                        f"{where}: {ev.op} requires an existing row, found "
                        f"{describe_ident(cur.ident)}"
                    )
                table = schema.tables[ev.table]
                if row_image_token(table, cur.row.data) != row_image_token(table, image):
                    raise LogContractError(
                        f"{where}: {ev.op} before-image does not match the current "
                        f"row ({describe_ident(cur.ident)})"
                    )
                reads.append(EventWitness(ev_idx, cur.ident))
                if ev.op == "update":
                    overlay[key] = StoreEntry(
                        is_row=True,
                        ident=tx_version(tx.position, ev_idx),
                        row=_make_row(schema, ev.table, ev.after),
                    )
                else:
                    overlay[key] = StoreEntry(
                        is_row=False, ident=delete_absence(tx.position, ev_idx)
                    )
        merged = store.merged(overlay)
        _check_commit(merged, tx)
        parent_refs = _commit_parent_refs(schema, merged, overlay, tx)
        store = merged
        witnesses.append(
            TxWitness(
                tx_pos=tx.position,
                tx_id=tx.tx_id,
                event_reads=tuple(reads),
                parent_refs=tuple(parent_refs),
            )
        )
    return witnesses


def _check_commit(store: _Store, tx: Transaction) -> None:
    """Cross-row uniqueness and foreign keys over the whole committed state."""
    try:
        store.database().validate()
    except DataError as exc:
        raise LogContractError(
            f"tx {tx.tx_id!r} (position {tx.position}): commit violates schema "
            f"constraints: {exc}"
        ) from exc


def _commit_parent_refs(
    schema: Schema,
    merged: _Store,
    overlay: dict[tuple[str, str], StoreEntry],
    tx: Transaction,
) -> list[ParentRef]:
    """Required-FK parent versions for this tx's surviving written rows."""
    refs: list[ParentRef] = []
    index = merged.unique_index()
    for (table_name, pk_token), entry in sorted(overlay.items()):
        if not entry.is_row:
            continue
        table = schema.tables[table_name]
        for fk in table.foreign_keys:
            if fk.nullable:
                continue  # nullable FK: original project semantics, no hard edge
            local_values = [entry.row.data.get(c) for c in fk.columns]
            if any(v is None for v in local_values):
                continue  # validated as illegal for required FKs at commit
            token = encode_key_values(local_values, table.column_types(fk.columns))
            parent = index[(fk.ref_table, fk.ref_columns)].get(token)
            if parent is None or not parent.is_row:
                continue  # commit validation already reported this
            refs.append(
                ParentRef(
                    child_key=(table_name, pk_token),
                    child_row_id=entry.row.row_id,
                    child_version=entry.ident,
                    fk_columns=fk.columns,
                    ref_table=fk.ref_table,
                    ref_columns=fk.ref_columns,
                    ref_token=token,
                    parent_version=parent.ident,
                )
            )
    return refs


# -------------------------------------------------------------- normalize
@dataclass(frozen=True)
class TxDisposition:
    tx_pos: int
    tx_id: Any
    status: str  # "committed" / "deselected" / "skipped"
    reason: str = ""

    def to_dict(self) -> dict:
        return {
            "tx_pos": self.tx_pos,
            "tx_id": self.tx_id,
            "status": self.status,
            "reason": self.reason,
        }


@dataclass
class NormalizeResult:
    committed: list[int]  # original tx positions, ascending
    dispositions: list[TxDisposition]
    final_store: _Store

    def final_database(self) -> Database:
        return self.final_store.database()

    def state_hash(self) -> str:
        """Hash of the final data state (versions ignored: row content only)."""
        return self.final_store.database().content_hash()


def normalize(
    schema: Schema,
    snapshot: Database,
    log: ChangeLog,
    witnesses: list[TxWitness],
    selection: Iterable[int],
) -> NormalizeResult:
    """Replay ``selection`` (original tx positions) from the fixed snapshot.

    Deterministic: the same selection always yields the same committed
    subsequence and the same final data state.
    """
    selected = set(selection)
    store = _Store(schema, snapshot)
    committed: list[int] = []
    dispositions: list[TxDisposition] = []
    for tx in log:
        witness = witnesses[tx.position]
        if tx.position not in selected:
            dispositions.append(
                TxDisposition(tx.position, tx.tx_id, "deselected", "actively deleted")
            )
            continue
        overlay: dict[tuple[str, str], StoreEntry] = {}
        skip_reason: str | None = None
        for ev_idx, ev in enumerate(tx.events):
            token = _event_pk_token(schema, ev)
            key = (ev.table, token)
            cur = store.lookup(overlay, ev.table, token)
            want = witness.event_reads[ev_idx].read
            got = cur.ident
            if ev.op == "insert":
                if cur.is_row or got != want:
                    skip_reason = (
                        f"tx {tx.tx_id!r} (position {tx.position}) event {ev_idx} "
                        f"(insert {ev.table}): requires {describe_ident(want)}, "
                        f"found {describe_ident(got)}"
                    )
                    break
                overlay[key] = StoreEntry(
                    is_row=True,
                    ident=tx_version(tx.position, ev_idx),
                    row=_make_row(schema, ev.table, ev.after),
                )
            else:
                # A matching version identity carries the identical row image
                # (versions are immutable), so the before-image check holds.
                if not cur.is_row or got != want:
                    skip_reason = (
                        f"tx {tx.tx_id!r} (position {tx.position}) event {ev_idx} "
                        f"({ev.op} {ev.table}): requires {describe_ident(want)}, "
                        f"found {describe_ident(got)}"
                    )
                    break
                if ev.op == "update":
                    overlay[key] = StoreEntry(
                        is_row=True,
                        ident=tx_version(tx.position, ev_idx),
                        row=_make_row(schema, ev.table, ev.after),
                    )
                else:
                    overlay[key] = StoreEntry(
                        is_row=False, ident=delete_absence(tx.position, ev_idx)
                    )
        merged = store.merged(overlay) if skip_reason is None else None
        if skip_reason is None:
            try:
                merged.database().validate()
            except DataError as exc:
                skip_reason = (
                    f"tx {tx.tx_id!r} (position {tx.position}): commit violates "
                    f"schema constraints: {exc}"
                )
        if skip_reason is None:
            skip_reason = _check_parent_versions(schema, merged, witness, tx)
        if skip_reason is None:
            store = merged
            committed.append(tx.position)
            dispositions.append(TxDisposition(tx.position, tx.tx_id, "committed"))
        else:
            # The whole transaction is skipped; its private writes and
            # versions stay invisible to every later transaction.
            dispositions.append(
                TxDisposition(tx.position, tx.tx_id, "skipped", skip_reason)
            )
    return NormalizeResult(committed, dispositions, store)


def _check_parent_versions(
    schema: Schema, merged: _Store, witness: TxWitness, tx: Transaction
) -> str | None:
    if not witness.parent_refs:
        return None
    index = merged.unique_index()
    for ref in witness.parent_refs:
        child = merged.entries.get(ref.child_key)
        if child is None or not child.is_row or child.ident != ref.child_version:
            return (
                f"tx {tx.tx_id!r} (position {tx.position}): internal version "
                f"mismatch for written row {ref.child_row_id}"
            )
        parent = index[(ref.ref_table, ref.ref_columns)].get(ref.ref_token)
        got = parent.ident if parent is not None and parent.is_row else None
        if got != ref.parent_version:
            found = describe_ident(got) if got else "no such parent row"
            return (
                f"tx {tx.tx_id!r} (position {tx.position}): commit requires parent "
                f"{describe_ident(ref.parent_version)} for {ref.child_row_id} via "
                f"fk {list(ref.fk_columns)}->{ref.ref_table}{list(ref.ref_columns)}; "
                f"found {found}"
            )
    return None


# -------------------------------------------------------------- candidates
@dataclass
class LogCandidate:
    """A self-contained oracle input: snapshot/ + schema.json + changes.jsonl."""

    schema: Schema
    schema_text: str  # original schema.json content, copied verbatim
    snapshot: Database
    transactions: list[Transaction]  # committed subsequence, original order

    def content_hash(self) -> str:
        payload = {
            "mode": "log-candidate",
            "snapshot": self.snapshot.content_hash(),
            "changes": [tx.canonical() for tx in self.transactions],
        }
        return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()

    def tx_ids(self) -> list[Any]:
        return [tx.tx_id for tx in self.transactions]

    def positions(self) -> list[int]:
        return [tx.position for tx in self.transactions]

    def materialize(self, target_dir: str | Path) -> Path:
        target = Path(target_dir)
        target.mkdir(parents=True, exist_ok=True)
        self.snapshot.materialize(target / "snapshot")
        with open(target / "schema.json", "w", encoding="utf-8") as fh:
            fh.write(self.schema_text)
            if not self.schema_text.endswith("\n"):
                fh.write("\n")
        with open(target / "changes.jsonl", "w", encoding="utf-8") as fh:
            for tx in self.transactions:
                fh.write(canonical_tx_line(tx))
                fh.write("\n")
        return target


@dataclass
class ValidatedLog:
    """Everything derived from the fixed inputs before any oracle call."""

    schema: Schema
    schema_text: str
    snapshot: Database
    log: ChangeLog
    witnesses: list[TxWitness]
    snapshot_hash: str
    log_hash: str
    schema_hash: str

    def candidate(self, positions: Iterable[int]) -> LogCandidate:
        pos = sorted(positions)
        return LogCandidate(
            schema=self.schema,
            schema_text=self.schema_text,
            snapshot=self.snapshot,
            transactions=[self.log.transactions[p] for p in pos],
        )

    def normalize(self, selection: Iterable[int]) -> NormalizeResult:
        return normalize(self.schema, self.snapshot, self.log, self.witnesses, selection)

    def input_binding_hash(self) -> str:
        """Binds snapshot + ordered original log + schema for resume checks."""
        payload = {
            "snapshot": self.snapshot_hash,
            "log": self.log_hash,
            "schema": self.schema_hash,
        }
        return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def validate_log_inputs(
    schema: Schema, schema_text: str, snapshot: Database, log: ChangeLog
) -> ValidatedLog:
    """Validate the original log against the fixed snapshot (no oracle yet)."""
    witnesses = build_witness(schema, snapshot, log)
    schema_hash = hashlib.sha256(
        canonical_json(schema.canonical()).encode("utf-8")
    ).hexdigest()
    return ValidatedLog(
        schema=schema,
        schema_text=schema_text,
        snapshot=snapshot,
        log=log,
        witnesses=witnesses,
        snapshot_hash=snapshot.content_hash(),
        log_hash=log.content_hash(),
        schema_hash=schema_hash,
    )
