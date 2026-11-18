"""Streaming archive scan with on-disk de-duplication and assembly.

The whole capture history is never held in memory.  Shards are read one line
at a time and every physical message is written into a temporary SQLite
database; only the current row state (snapshot-sized) and per-transaction
restore structures live in memory.

Three levels of identity are tracked:

* **physical message** - one ``(stream, offset)`` coordinate.  The same
  coordinate repeated with identical canonical content is a physical
  retransmission and merges; different content at the same coordinate is a
  hard conflict.
* **logical change** - one ``(tx_id, tx_order)`` business event.  The same
  business payload delivered at several coordinates is one logical event and
  *all* coordinates are retained for provenance; differing payloads conflict.
* **transaction end** - the single commit/abort decision of a transaction,
  with the same retransmission/conflict rules.  A commit and an abort for the
  same transaction always conflict.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .errors import CaptureArchiveError
from .manifest import Manifest
from .messages import (
    canonical_business,
    canonical_message,
    validate_message,
)

if TYPE_CHECKING:
    from ..schema import Schema

# ------------------------------------------------------------------ DTOs
@dataclass(frozen=True)
class Coordinate:
    stream: str
    offset: int

    def to_dict(self) -> dict[str, Any]:
        return {"stream": self.stream, "offset": self.offset}


@dataclass(frozen=True)
class LogicalEvent:
    tx_id: str
    tx_order: int
    table: str
    op: str
    key: dict[str, Any]
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    new_key: dict[str, Any] | None
    coordinates: tuple[Coordinate, ...]
    schema_version: int | None = None  # declared write schema (capture-v2 only)

    def where(self) -> str:
        """Stable source coordinate for error reports.

        Names the logical position *and* the first transport coordinate, so a
        rejected row image can be found in the archive.
        """
        base = f"tx {self.tx_id!r} tx_order {self.tx_order}"
        if self.coordinates:
            first = self.coordinates[0]
            base += f" at {first.stream}@{first.offset}"
        if self.schema_version is not None:
            base += f" (schema_version {self.schema_version})"
        return base


@dataclass(frozen=True)
class EndDecision:
    tx_id: str
    kind: str  # commit / abort
    payload: dict[str, Any]
    coordinates: tuple[Coordinate, ...]

    @property
    def commit_seq(self) -> int | None:
        return self.payload.get("commit_seq")

    @property
    def event_count(self) -> int:
        return self.payload["event_count"]

    @property
    def table_counts(self) -> dict[str, int]:
        return self.payload["table_counts"]


@dataclass
class ScanMetrics:
    physical_messages_read: int = 0
    physical_duplicates: int = 0
    logical_event_duplicates: int = 0
    end_duplicates: int = 0
    heartbeats: int = 0
    files_read: int = 0


@dataclass
class AssemblyMetrics:
    transactions: int = 0
    committed: int = 0
    aborted: int = 0
    zero_event_commits: int = 0
    logical_events_total: int = 0
    aborted_events: int = 0
    out_of_window_commits: int = 0
    distinct_commit_seq: int = 0


_SCHEMA = """
CREATE TABLE physical_messages (
    stream          TEXT NOT NULL,
    offset          INTEGER NOT NULL,
    canonical       TEXT NOT NULL,
    PRIMARY KEY (stream, offset)
) WITHOUT ROWID;

CREATE TABLE logical_events (
    event_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    tx_id           TEXT NOT NULL,
    tx_order        INTEGER NOT NULL,
    schema_version  INTEGER,
    business_canon  TEXT NOT NULL,
    payload         TEXT NOT NULL,
    UNIQUE (tx_id, tx_order, business_canon)
);

CREATE TABLE event_coordinates (
    event_id        INTEGER NOT NULL REFERENCES logical_events(event_id),
    stream          TEXT NOT NULL,
    offset          INTEGER NOT NULL,
    PRIMARY KEY (event_id, stream, offset)
) WITHOUT ROWID;

CREATE TABLE tx_ends (
    end_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    tx_id           TEXT NOT NULL,
    kind            TEXT NOT NULL,
    business_canon  TEXT NOT NULL,
    payload         TEXT NOT NULL,
    UNIQUE (tx_id, kind, business_canon)
);

CREATE TABLE end_coordinates (
    end_id          INTEGER NOT NULL REFERENCES tx_ends(end_id),
    stream          TEXT NOT NULL,
    offset          INTEGER NOT NULL,
    PRIMARY KEY (end_id, stream, offset)
) WITHOUT ROWID;

CREATE INDEX idx_logical_tx ON logical_events(tx_id, tx_order);
CREATE INDEX idx_ends_tx ON tx_ends(tx_id);
"""


class CaptureStore:
    """Owns the spill database; use it as a context manager for cleanup."""

    def __init__(
        self,
        manifest: Manifest,
        schema: "Schema",
        archive_dir: str | Path,
        tmp_root: str | Path | None = None,
    ):
        self.manifest = manifest
        self.schema = schema
        self.archive_dir = Path(archive_dir)
        self.tmp_root = Path(tmp_root) if tmp_root is not None else None
        self._tmpdir: tempfile.TemporaryDirectory | None = None
        self.db_path: Path | None = None
        self.conn: sqlite3.Connection | None = None
        self.metrics = ScanMetrics()
        self.assembly = AssemblyMetrics()

    # ------------------------------------------------------------- lifecycle
    def __enter__(self) -> "CaptureStore":
        self._tmpdir = tempfile.TemporaryDirectory(
            prefix="ddreduce-capture-", dir=str(self.tmp_root) if self.tmp_root else None
        )
        self.db_path = Path(self._tmpdir.name) / "capture.sqlite"
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.execute("PRAGMA journal_mode=MEMORY")
        self.conn.execute("PRAGMA synchronous=OFF")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.executescript(_SCHEMA)
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        if self._tmpdir is not None:
            self._tmpdir.cleanup()
            self._tmpdir = None
            self.db_path = None

    def spill_bytes(self) -> int:
        if self.db_path is None or not self.db_path.exists():
            return 0
        total = self.db_path.stat().st_size
        for suffix in ("-wal", "-shm"):
            side = self.db_path.with_name(self.db_path.name + suffix)
            if side.exists():
                total += side.stat().st_size
        return total

    # ------------------------------------------------------------------ scan
    def scan(self) -> None:
        """Read every declared shard in transport order; validate and spill."""
        assert self.conn is not None
        for rel in self.manifest.files:
            self._scan_file(self.archive_dir / rel, rel)
        self.metrics.files_read = len(self.manifest.files)
        self._check_stream_coverage()

    def _scan_file(self, path: Path, rel: str) -> None:
        with open(path, "r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise CaptureArchiveError(
                        f"{rel}:{line_no}: invalid JSON ({exc})"
                    ) from exc
                try:
                    validate_message(
                        obj, self.manifest, self.schema, rel, line_no
                    )
                except CaptureArchiveError as exc:
                    exc.with_locator(
                        file=rel, line=line_no,
                        stream=obj.get("stream") if isinstance(obj, dict) else None,
                        offset=obj.get("offset") if isinstance(obj, dict) else None,
                        tx_id=obj.get("tx_id") if isinstance(obj, dict) else None,
                        tx_order=obj.get("tx_order") if isinstance(obj, dict) else None,
                        schema_version=obj.get("schema_version")
                        if isinstance(obj, dict) else None,
                    )
                    raise
                self._ingest(obj)

    def _ingest(self, msg: dict[str, Any]) -> None:
        conn = self.conn
        stream, offset = msg["stream"], msg["offset"]
        canon = canonical_message(msg)
        self.metrics.physical_messages_read += 1

        existing = conn.execute(
            "SELECT canonical FROM physical_messages WHERE stream=? AND offset=?",
            (stream, offset),
        ).fetchone()
        if existing is not None:
            if existing[0] != canon:
                raise CaptureArchiveError(
                    f"physical coordinate conflict at {stream}@{offset}: messages "
                    "with the same (stream, offset) differ; retransmissions must be "
                    "identical apart from JSON formatting"
                )
            self.metrics.physical_duplicates += 1
            # Even identical retransmissions must already reference their
            # logical rows; the coordinate link inserts below are idempotent.
        else:
            conn.execute(
                "INSERT INTO physical_messages(stream, offset, canonical) "
                "VALUES (?, ?, ?)",
                (stream, offset, canon),
            )

        kind = msg["kind"]
        if kind == "heartbeat":
            self.metrics.heartbeats += 1
            return

        biz_canon = canonical_business(msg)
        if kind == "change":
            self._ingest_change(msg, biz_canon, stream, offset)
        else:
            self._ingest_end(msg, biz_canon, stream, offset)

    def _ingest_change(
        self, msg: dict[str, Any], biz_canon: str, stream: str, offset: int
    ) -> None:
        conn = self.conn
        row = conn.execute(
            "SELECT event_id FROM logical_events WHERE tx_id=? AND tx_order=? "
            "AND business_canon=?",
            (msg["tx_id"], msg["tx_order"], biz_canon),
        ).fetchone()
        if row is None:
            # A different payload for the same (tx_id, tx_order) is a conflict.
            conflict = conn.execute(
                "SELECT business_canon FROM logical_events WHERE tx_id=? AND tx_order=?",
                (msg["tx_id"], msg["tx_order"]),
            ).fetchone()
            if conflict is not None:
                raise CaptureArchiveError(
                    f"logical event conflict for tx {msg['tx_id']!r} tx_order "
                    f"{msg['tx_order']}: distinct business payloads delivered at "
                    "different coordinates; cannot choose one"
                )
            cur = conn.execute(
                "INSERT INTO logical_events(tx_id, tx_order, schema_version, "
                "business_canon, payload) VALUES (?, ?, ?, ?, ?)",
                (
                    msg["tx_id"], msg["tx_order"], msg.get("schema_version"),
                    biz_canon, json.dumps(msg, ensure_ascii=False),
                ),
            )
            event_id = cur.lastrowid
        else:
            event_id = row[0]
            self.metrics.logical_event_duplicates += 1
        conn.execute(
            "INSERT OR IGNORE INTO event_coordinates(event_id, stream, offset) "
            "VALUES (?, ?, ?)",
            (event_id, stream, offset),
        )

    def _ingest_end(
        self, msg: dict[str, Any], biz_canon: str, stream: str, offset: int
    ) -> None:
        conn = self.conn
        kind = msg["kind"]
        row = conn.execute(
            "SELECT end_id FROM tx_ends WHERE tx_id=? AND kind=? AND business_canon=?",
            (msg["tx_id"], kind, biz_canon),
        ).fetchone()
        if row is None:
            # A different end payload, or an end of a different kind, conflicts.
            conflict = conn.execute(
                "SELECT kind, business_canon FROM tx_ends WHERE tx_id=?",
                (msg["tx_id"],),
            ).fetchone()
            if conflict is not None:
                if conflict[0] != kind:
                    raise CaptureArchiveError(
                        f"transaction {msg['tx_id']!r} has conflicting end decisions: "
                        f"{conflict[0]} vs {kind}"
                    )
                raise CaptureArchiveError(
                    f"transaction {msg['tx_id']!r}: retransmitted {kind} content "
                    "differs from the first decision; end decisions are immutable"
                )
            cur = conn.execute(
                "INSERT INTO tx_ends(tx_id, kind, business_canon, payload) "
                "VALUES (?, ?, ?, ?)",
                (msg["tx_id"], kind, biz_canon, json.dumps(msg, ensure_ascii=False)),
            )
            end_id = cur.lastrowid
        else:
            end_id = row[0]
            self.metrics.end_duplicates += 1
        conn.execute(
            "INSERT OR IGNORE INTO end_coordinates(end_id, stream, offset) "
            "VALUES (?, ?, ?)",
            (end_id, stream, offset),
        )

    def _check_stream_coverage(self) -> None:
        """Every integer offset in every declared closed interval must exist."""
        conn = self.conn
        for spec in self.manifest.streams:
            present = {
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT offset FROM physical_messages WHERE stream=?",
                    (spec.name,),
                )
            }
            if not present:
                raise CaptureArchiveError(
                    f"stream {spec.name!r}: declared range "
                    f"[{spec.first_offset}, {spec.last_offset}] has no messages"
                )
            expected = set(range(spec.first_offset, spec.last_offset + 1))
            if present != expected:
                missing = sorted(expected - present)
                extra = sorted(present - expected)
                if missing:
                    raise CaptureArchiveError(
                        f"stream {spec.name!r}: coverage gap; expected "
                        f"{spec.size()} unique offsets, found {len(present)}; "
                        f"first missing offset {missing[0]}"
                    )
                raise CaptureArchiveError(
                    f"stream {spec.name!r}: offsets outside declared range; "
                    f"first extra offset {extra[0]}"
                )

    # --------------------------------------------------------------- assembly
    def assemble(self) -> None:
        """Close the archive: every transaction gets one consistent decision."""
        conn = self.conn
        tx_ids = {
            r[0]
            for r in conn.execute("SELECT DISTINCT tx_id FROM logical_events")
        } | {
            r[0]
            for r in conn.execute("SELECT DISTINCT tx_id FROM tx_ends")
        }
        seen_commit_seq: dict[int, str] = {}
        for tx_id in sorted(tx_ids):
            self._assemble_tx(tx_id, seen_commit_seq)
        self.assembly.transactions = len(tx_ids)
        self.assembly.logical_events_total = conn.execute(
            "SELECT COUNT(*) FROM logical_events"
        ).fetchone()[0]
        self.assembly.aborted_events = conn.execute(
            "SELECT COUNT(*) FROM logical_events e WHERE EXISTS ("
            "SELECT 1 FROM tx_ends t WHERE t.tx_id=e.tx_id AND t.kind='abort')"
        ).fetchone()[0]
        self.assembly.committed = conn.execute(
            "SELECT COUNT(*) FROM tx_ends WHERE kind='commit'"
        ).fetchone()[0]
        self.assembly.aborted = conn.execute(
            "SELECT COUNT(*) FROM tx_ends WHERE kind='abort'"
        ).fetchone()[0]
        self.assembly.zero_event_commits = conn.execute(
            "SELECT COUNT(*) FROM tx_ends WHERE kind='commit' AND "
            "json_extract(payload, '$.event_count')=0"
        ).fetchone()[0]
        self.assembly.distinct_commit_seq = len(seen_commit_seq)

    def _assemble_tx(self, tx_id: str, seen_commit_seq: dict[int, str]) -> None:
        conn = self.conn
        ends = conn.execute(
            "SELECT end_id, kind, payload FROM tx_ends WHERE tx_id=?", (tx_id,)
        ).fetchall()
        event_rows = conn.execute(
            "SELECT event_id, payload FROM logical_events WHERE tx_id=? "
            "ORDER BY tx_order",
            (tx_id,),
        ).fetchall()
        if not ends:
            raise CaptureArchiveError(
                f"transaction {tx_id!r}: {len(event_rows)} logical events but no "
                "commit/abort decision; closed archives must not leave transactions open"
            )
        if len(ends) != 1:
            # Conflicts across different payloads/kinds were rejected at ingest;
            # multiple rows here means duplicate-but-distinct canonical forms.
            raise CaptureArchiveError(
                f"transaction {tx_id!r}: multiple distinct end decisions present"
            )
        end_id, kind, end_payload_json = ends[0]
        end = json.loads(end_payload_json)
        event_count = end["event_count"]

        orders = [json.loads(p)["tx_order"] for _, p in event_rows]
        if orders and len(set(orders)) != len(orders):
            raise CaptureArchiveError(
                f"transaction {tx_id!r}: duplicate tx_order values after de-duplication"
            )
        if orders != list(range(1, event_count + 1)):
            raise CaptureArchiveError(
                f"transaction {tx_id!r}: tx_order must be exactly 1..event_count "
                f"(1..{event_count}); found {orders}"
            )

        # A schema switch happens at a transaction commit boundary: one
        # transaction can never contain events written under two versions.
        versions = {json.loads(p).get("schema_version") for _, p in event_rows}
        if len(versions) > 1:
            raise CaptureArchiveError(
                f"transaction {tx_id!r}: events declare conflicting schema "
                f"versions {sorted(v for v in versions if v is not None)}; a "
                "transaction cannot straddle a schema switch"
            )

        # table_counts must accurately count logical row changes per source table.
        actual_counts: dict[str, int] = {}
        for _, payload_json in event_rows:
            payload = json.loads(payload_json)
            actual_counts[payload["table"]] = actual_counts.get(payload["table"], 0) + 1
        declared = {t: c for t, c in end["table_counts"].items()}
        if declared != actual_counts:
            raise CaptureArchiveError(
                f"transaction {tx_id!r}: end table_counts {declared} do not match the "
                f"logical events {actual_counts}"
            )

        if kind == "commit":
            seq = end["commit_seq"]
            if seq in seen_commit_seq:
                raise CaptureArchiveError(
                    f"commit_seq {seq} used by both {seen_commit_seq[seq]!r} and "
                    f"{tx_id!r}; committed transactions need unique commit_seq"
                )
            seen_commit_seq[seq] = tx_id

    # ---------------------------------------------------------------- queries
    def committed_transactions(self) -> list[EndDecision]:
        """All commit decisions ordered by commit_seq (the business order)."""
        conn = self.conn
        out: list[EndDecision] = []
        rows = conn.execute(
            "SELECT end_id, payload FROM tx_ends WHERE kind='commit' "
            "ORDER BY json_extract(payload, '$.commit_seq')"
        ).fetchall()
        for end_id, payload_json in rows:
            payload = json.loads(payload_json)
            coords = tuple(
                Coordinate(s, o)
                for s, o in conn.execute(
                    "SELECT stream, offset FROM end_coordinates WHERE end_id=? "
                    "ORDER BY stream, offset",
                    (end_id,),
                )
            )
            out.append(
                EndDecision(
                    tx_id=payload["tx_id"],
                    kind="commit",
                    payload=payload,
                    coordinates=coords,
                )
            )
        return out

    def end_for(self, tx_id: str) -> EndDecision:
        conn = self.conn
        row = conn.execute(
            "SELECT end_id, kind, payload FROM tx_ends WHERE tx_id=?", (tx_id,)
        ).fetchone()
        if row is None:
            raise CaptureArchiveError(f"transaction {tx_id!r}: no end decision")
        end_id, kind, payload_json = row
        coords = tuple(
            Coordinate(s, o)
            for s, o in conn.execute(
                "SELECT stream, offset FROM end_coordinates WHERE end_id=? "
                "ORDER BY stream, offset",
                (end_id,),
            )
        )
        return EndDecision(
            tx_id=tx_id, kind=kind, payload=json.loads(payload_json), coordinates=coords
        )

    def logical_events(self, tx_id: str) -> list[LogicalEvent]:
        conn = self.conn
        out: list[LogicalEvent] = []
        rows = conn.execute(
            "SELECT event_id, payload FROM logical_events WHERE tx_id=? ORDER BY tx_order",
            (tx_id,),
        ).fetchall()
        for event_id, payload_json in rows:
            payload = json.loads(payload_json)
            coords = tuple(
                Coordinate(s, o)
                for s, o in conn.execute(
                    "SELECT stream, offset FROM event_coordinates WHERE event_id=? "
                    "ORDER BY stream, offset",
                    (event_id,),
                )
            )
            out.append(
                LogicalEvent(
                    tx_id=payload["tx_id"],
                    tx_order=payload["tx_order"],
                    table=payload["table"],
                    op=payload["op"],
                    key=payload["key"],
                    before=payload.get("before"),
                    after=payload.get("after"),
                    new_key=payload.get("new_key"),
                    coordinates=coords,
                    schema_version=payload.get("schema_version"),
                )
            )
        return out

    def all_tx_ids(self) -> list[str]:
        return [
            r[0]
            for r in self.conn.execute(
                "SELECT DISTINCT tx_id FROM ("
                "SELECT tx_id FROM logical_events UNION SELECT tx_id FROM tx_ends) "
                "ORDER BY tx_id"
            )
        ]
