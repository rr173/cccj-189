"""CDC change-log contract: ``changes.jsonl`` parsing and structural validation.

Each non-empty line of a change log is one *committed transaction*:

    {"tx_id": <unique id>, "events": [<event>, ...]}   # events non-empty

Events execute in array order; file order is the business order (transactions
are never re-sorted by ``tx_id``).  Every event carries ``op``, ``table`` and
``key``:

    insert: {"op":"insert","table":T,"key":K,"after":ROW}
    update: {"op":"update","table":T,"key":K,"before":ROW,"after":ROW}
    delete: {"op":"delete","table":T,"key":K,"before":ROW}

``before``/``after`` are *complete* row images (every schema column present,
no unknown columns, values typed per the schema).  ``key`` holds exactly the
primary-key columns and must agree with the corresponding row image; an update
never changes the primary key.

Per-event checks (row structure, primary-key presence) run while the event
executes; cross-row uniqueness and foreign keys are checked when the whole
transaction commits.  Any violation reports the transaction and event
position and refuses the log before any oracle command is started.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

from .schema import Schema, TableSchema
from .values import DataError, canonical_json, canonical_scalar, normalize_scalar

OPS = ("insert", "update", "delete")


class LogContractError(ValueError):
    """A change log violates the contract; the message names tx and event."""


def _where(tx_id: Any, position: int, event_idx: int | None) -> str:
    base = f"tx {tx_id!r} (position {position})"
    if event_idx is not None:
        base += f" event {event_idx}"
    return base


@dataclass(frozen=True)
class Event:
    op: str  # insert / update / delete
    table: str
    key: dict[str, Any]
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    raw: dict[str, Any]  # the exact event object as read from the log line

    def canonical(self) -> dict[str, Any]:
        """Canonical event object used for hashing and candidate files."""
        return self.raw


@dataclass(frozen=True)
class Transaction:
    tx_id: Any
    events: tuple[Event, ...]
    position: int  # file order (business order), 0-based

    def canonical(self) -> dict[str, Any]:
        return {"tx_id": self.tx_id, "events": [e.canonical() for e in self.events]}


@dataclass
class ChangeLog:
    transactions: list[Transaction]
    source_path: str = ""

    def __len__(self) -> int:
        return len(self.transactions)

    def __iter__(self) -> Iterator[Transaction]:
        return iter(self.transactions)

    def content_hash(self) -> str:
        """Hash of the ordered transactions incl. boundaries (not the file text)."""
        import hashlib

        payload = [tx.canonical() for tx in self.transactions]
        return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def canonical_tx_line(tx: Transaction) -> str:
    """The canonical single-line serialization used in candidate logs."""
    return json.dumps(tx.canonical(), ensure_ascii=False, sort_keys=True)


# --------------------------------------------------------------------- parsing
def load_changelog(path: str | Path, schema: Schema) -> ChangeLog:
    """Parse and structurally validate a ``changes.jsonl`` file.

    Structural (per-event) checks only: row images, key/image agreement, PK
    immutability, types and nullability.  Execution-time checks (key presence,
    before-image match, commit-time uniqueness/FK) belong to the replay layer.
    """
    path = Path(path)
    if not path.is_file():
        raise LogContractError(f"change log not found: {path}")
    transactions: list[Transaction] = []
    seen_ids: set[str] = set()
    with open(path, "r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise LogContractError(
                    f"{path.name}:{line_no}: invalid JSON ({exc})"
                ) from exc
            tx = _parse_transaction(obj, len(transactions), schema, path.name, line_no)
            token = canonical_json(tx.tx_id)
            if token in seen_ids:
                raise LogContractError(
                    f"{path.name}:{line_no}: duplicate tx_id {tx.tx_id!r}"
                )
            seen_ids.add(token)
            transactions.append(tx)
    return ChangeLog(transactions=transactions, source_path=str(path))


def _parse_transaction(
    obj: Any, position: int, schema: Schema, fname: str, line_no: int
) -> Transaction:
    where = f"{fname}:{line_no} (position {position})"
    if not isinstance(obj, dict):
        raise LogContractError(f"{where}: transaction must be a JSON object")
    unknown = set(obj) - {"tx_id", "events"}
    if unknown:
        raise LogContractError(f"{where}: unknown transaction fields {sorted(unknown)}")
    if "tx_id" not in obj:
        raise LogContractError(f"{where}: missing tx_id")
    tx_id = obj["tx_id"]
    if isinstance(tx_id, bool) or not isinstance(tx_id, (str, int)):
        raise LogContractError(
            f"{where}: tx_id must be a string or integer, got {type(tx_id).__name__}"
        )
    if isinstance(tx_id, str) and not tx_id:
        raise LogContractError(f"{where}: tx_id must be non-empty")
    events_raw = obj.get("events")
    if not isinstance(events_raw, list) or not events_raw:
        raise LogContractError(f"tx {tx_id!r} (position {position}): events must be a non-empty array")
    events = []
    for idx, ev in enumerate(events_raw):
        events.append(_parse_event(ev, tx_id, position, idx, schema))
    return Transaction(tx_id=tx_id, events=tuple(events), position=position)


def _parse_event(
    obj: Any, tx_id: Any, position: int, idx: int, schema: Schema
) -> Event:
    where = _where(tx_id, position, idx)
    if not isinstance(obj, dict):
        raise LogContractError(f"{where}: event must be a JSON object")
    op = obj.get("op")
    if op not in OPS:
        raise LogContractError(f"{where}: op must be one of {list(OPS)}, got {op!r}")
    table_name = obj.get("table")
    if not isinstance(table_name, str) or table_name not in schema.tables:
        raise LogContractError(f"{where}: unknown table {table_name!r}")
    table = schema.tables[table_name]

    allowed = {"op", "table", "key"}
    if op in ("insert", "update"):
        allowed.add("after")
    if op in ("update", "delete"):
        allowed.add("before")
    unknown = set(obj) - allowed
    if unknown:
        raise LogContractError(f"{where}: unknown event fields {sorted(unknown)}")
    missing = allowed - {"op", "table"} - set(obj)
    if missing:
        raise LogContractError(f"{where}: missing event fields {sorted(missing)}")

    key = obj["key"]
    if not isinstance(key, dict):
        raise LogContractError(f"{where}: key must be a JSON object")
    _check_key(table, key, where)

    before = obj.get("before")
    after = obj.get("after")
    if before is not None:
        _check_row_image(table, before, f"{where} before")
    if after is not None:
        _check_row_image(table, after, f"{where} after")

    # key must agree with the corresponding row image(s).
    if op == "insert":
        _check_key_matches_row(table, key, after, where)
    elif op == "delete":
        _check_key_matches_row(table, key, before, where)
    else:
        for col in table.primary_key:
            if canonical_scalar(before[col], table.columns[col].type) != canonical_scalar(
                after[col], table.columns[col].type
            ):
                raise LogContractError(
                    f"{where}: update must not change primary key column {col!r}"
                )
        _check_key_matches_row(table, key, before, where)
        _check_key_matches_row(table, key, after, where)
    return Event(op=op, table=table_name, key=key, before=before, after=after, raw=obj)


def _check_key(table: TableSchema, key: dict[str, Any], where: str) -> None:
    expected = set(table.primary_key)
    if set(key) != expected:
        raise LogContractError(
            f"{where}: key must contain exactly the primary key columns "
            f"{sorted(expected)}, got {sorted(key)}"
        )
    for col in table.primary_key:
        try:
            normalize_scalar(key[col], table.columns[col].type)
        except DataError as exc:
            raise LogContractError(f"{where}: key column {col!r}: {exc}") from exc


def _check_row_image(table: TableSchema, row: Any, where: str) -> None:
    """A *complete* row image: every schema column present, none unknown."""
    if not isinstance(row, dict):
        raise LogContractError(f"{where}: row image must be a JSON object")
    unknown = set(row) - set(table.columns)
    if unknown:
        raise LogContractError(f"{where}: unknown columns {sorted(unknown)}")
    missing = set(table.columns) - set(row)
    if missing:
        raise LogContractError(
            f"{where}: incomplete row image, missing columns {sorted(missing)}"
        )
    for col in table.columns.values():
        value = row[col.name]
        if value is None:
            if not col.nullable:
                raise LogContractError(
                    f"{where}: null in NOT NULL column {col.name!r}"
                )
            continue
        try:
            normalize_scalar(value, col.type)
        except DataError as exc:
            raise LogContractError(f"{where}: column {col.name!r}: {exc}") from exc


def _check_key_matches_row(
    table: TableSchema, key: dict[str, Any], row: dict[str, Any], where: str
) -> None:
    for col in table.primary_key:
        if canonical_scalar(key[col], table.columns[col].type) != canonical_scalar(
            row[col], table.columns[col].type
        ):
            raise LogContractError(
                f"{where}: key does not match row image primary key column {col!r}"
            )


def row_image_token(table: TableSchema, row: dict[str, Any]) -> str:
    """Typed, order-independent token of a complete row image."""
    def _token(name: str) -> str:
        value = row[name]
        if value is None:
            return "null:"
        return canonical_scalar(value, table.columns[name].type)

    return canonical_json({name: _token(name) for name in sorted(table.columns)})
