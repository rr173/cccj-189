"""Naive, independent capture-v1 reference replayer.

This module deliberately shares *no* code with the product capture package:
it parses shards itself, de-duplicates with plain dictionaries, assembles
transactions in memory and restores row state with simple dict operations.
It exists so the test suite can cross-check the streaming SQLite-backed
implementation against an obviously-correct reference on small archives.

The reference is intentionally simple and holds everything in memory - it is
a test oracle, not a production path.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class ReferenceError(ValueError):
    pass


def _canon(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


class NaiveCaptureReference:
    def __init__(self, archive: str | Path, schema: dict, snapshot_rows: dict[str, list[dict]]):
        self.archive = Path(archive)
        self.schema = schema
        self.manifest = self._load_json(self.archive / "capture.json")
        self._validate_manifest_shape()
        self.table_cols = {
            t["name"]: {c["name"]: c for c in t["columns"]}
            for t in schema["tables"]
        }
        self.pk = {t["name"]: list(t["primary_key"]) for t in schema["tables"]}
        # bindings: source table -> (target table, {src col: tgt col})
        self.bindings = {
            src: (spec["target"], dict(spec["columns"]))
            for src, spec in self.manifest["bindings"]["tables"].items()
        }
        self.target_of_source = {s: b[0] for s, b in self.bindings.items()}
        # state[target_table][pk-token] = full target row
        self.state: dict[str, dict[str, dict]] = {
            t["name"]: {} for t in schema["tables"]
        }
        for table, rows in snapshot_rows.items():
            for row in rows:
                self.state[table][self._pk_token(table, row)] = dict(row)
        self.scan_metrics = {}
        self.assembly_metrics = {}

    # ------------------------------------------------------------- manifest
    @staticmethod
    def _load_json(path: Path) -> dict:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def _validate_manifest_shape(self) -> None:
        m = self.manifest
        if m.get("version") != 1:
            raise ReferenceError(f"unsupported version {m.get('version')}")
        for field in ("source_id", "snapshot_commit_seq", "sealed_commit_seq",
                      "streams", "files", "bindings"):
            if field not in m:
                raise ReferenceError(f"manifest missing {field}")

    # ------------------------------------------------------------- scanning
    def scan_and_assemble(self) -> None:
        physical: dict[tuple[str, int], str] = {}
        # logical event key (tx_id, tx_order) -> (business canon, payload)
        logical: dict[tuple[str, int], tuple[str, dict]] = {}
        logical_coords: dict[tuple[str, int], list[tuple[str, int]]] = {}
        # tx_id -> (kind, business canon, payload)
        ends: dict[str, tuple[str, str, dict]] = {}
        end_coords: dict[str, list[tuple[str, int]]] = {}
        heartbeats = 0
        physical_dups = logical_dups = end_dups = 0
        total = 0

        stream_ranges = {
            s["name"]: (s["first_offset"], s["last_offset"])
            for s in self.manifest["streams"]
        }
        for rel in self.manifest["files"]:
            with open(self.archive / rel, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    msg = json.loads(line)
                    total += 1
                    coord = (msg["stream"], msg["offset"])
                    if msg["stream"] not in stream_ranges:
                        raise ReferenceError(f"undeclared stream {msg['stream']!r}")
                    lo, hi = stream_ranges[msg["stream"]]
                    if not (lo <= msg["offset"] <= hi):
                        raise ReferenceError(f"offset out of range: {coord}")
                    if coord in physical:
                        if physical[coord] != _canon(msg):
                            raise ReferenceError(f"physical conflict at {coord}")
                        physical_dups += 1
                        continue
                    physical[coord] = _canon(msg)

                    kind = msg["kind"]
                    if kind == "heartbeat":
                        heartbeats += 1
                        continue
                    business = {k: v for k, v in msg.items()
                                if k not in ("source_id", "stream", "offset")}
                    if kind == "change":
                        key = (msg["tx_id"], msg["tx_order"])
                        if key in logical:
                            if logical[key][0] != _canon(business):
                                raise ReferenceError(
                                    f"logical conflict at {key}"
                                )
                            logical_dups += 1
                        else:
                            logical[key] = (_canon(business), msg)
                        logical_coords.setdefault(key, []).append(coord)
                    else:
                        tx_id = msg["tx_id"]
                        if tx_id in ends:
                            old_kind, old_canon, _ = ends[tx_id]
                            if old_kind != kind or old_canon != _canon(business):
                                raise ReferenceError(
                                    f"end conflict for {tx_id}: "
                                    f"{old_kind}/{kind}"
                                )
                            end_dups += 1
                        else:
                            ends[tx_id] = (kind, _canon(business), msg)
                        end_coords.setdefault(tx_id, []).append(coord)

        # coverage
        for name, (lo, hi) in stream_ranges.items():
            present = {off for (stream, off) in physical if stream == name}
            expected = set(range(lo, hi + 1))
            if present != expected:
                raise ReferenceError(
                    f"stream {name} coverage gap: missing {sorted(expected - present)[:3]}"
                )

        # assembly
        tx_ids = {tx_id for (tx_id, _order) in logical} | set(ends)
        per_tx_events: dict[str, list[dict]] = {}
        for (tx_id, order), (_canon_b, payload) in logical.items():
            per_tx_events.setdefault(tx_id, []).append(payload)
        for tx_id in tx_ids:
            if tx_id not in ends:
                raise ReferenceError(f"transaction {tx_id} has no end decision")
            kind, _c, end = ends[tx_id]
            events = sorted(per_tx_events.get(tx_id, []),
                            key=lambda e: e["tx_order"])
            orders = [e["tx_order"] for e in events]
            if orders != list(range(1, end["event_count"] + 1)):
                raise ReferenceError(
                    f"tx {tx_id}: orders {orders} != 1..{end['event_count']}"
                )
            counts: dict[str, int] = {}
            for ev in events:
                counts[ev["table"]] = counts.get(ev["table"], 0) + 1
            if counts != dict(end["table_counts"]):
                raise ReferenceError(
                    f"tx {tx_id}: table_counts mismatch {counts} vs {end['table_counts']}"
                )
        commit_seqs = [
            end[2]["commit_seq"] for end in ends.values() if end[0] == "commit"
        ]
        if len(commit_seqs) != len(set(commit_seqs)):
            raise ReferenceError("duplicate commit_seq")

        self._physical = physical
        self._logical = logical
        self._logical_coords = logical_coords
        self._ends = ends
        self._end_coords = end_coords
        self._per_tx_events = per_tx_events
        self.scan_metrics = {
            "physical_messages_read": total,
            "physical_duplicates": physical_dups,
            "logical_event_duplicates": logical_dups,
            "end_duplicates": end_dups,
            "heartbeats": heartbeats,
        }
        self.assembly_metrics = {
            "transactions_total": len(tx_ids),
            "committed_total": sum(1 for e in ends.values() if e[0] == "commit"),
            "aborted_total": sum(1 for e in ends.values() if e[0] == "abort"),
        }

    # ------------------------------------------------------------- restore
    def _pk_token(self, table: str, row: dict) -> str:
        # Tokens must match between snapshot inserts and converted event keys.
        return "|".join(
            f"{c}={json.dumps(row[c], sort_keys=True)}" for c in self.pk[table]
        )

    def _convert_key(self, source_table: str, source_key: dict) -> tuple[str, dict]:
        target_table, col_map = self.bindings[source_table]
        return target_table, {col_map[c]: v for c, v in source_key.items()}

    def _convert_partial(self, source_table: str, image: dict) -> tuple[str, dict]:
        target_table, col_map = self.bindings[source_table]
        return target_table, {col_map[c]: v for c, v in image.items()}

    def restore(self, through: int) -> list[dict]:
        """Return old-format full-image transactions in commit order."""
        m = self.manifest
        commits = sorted(
            (e for e in self._ends.values() if e[0] == "commit"),
            key=lambda e: e[2]["commit_seq"],
        )
        output: list[dict] = []
        for _kind, _c, end in commits:
            seq = end["commit_seq"]
            if not (m["snapshot_commit_seq"] < seq <= through):
                continue
            tx_id = end["tx_id"]
            events = sorted(self._per_tx_events.get(tx_id, []),
                            key=lambda e: e["tx_order"])
            if not events:
                continue
            tx_events = self._apply_tx(tx_id, events)
            output.append({"tx_id": tx_id, "events": tx_events})
        return output

    def _apply_tx(self, tx_id: str, events: list[dict]) -> list[dict]:
        overlay: dict[tuple[str, str], dict | None] = {}
        out_events: list[dict] = []
        for ev in events:
            src_table = ev["table"]
            target_table, key = self._convert_key(src_table, ev["key"])
            token = self._pk_token(target_table, key)
            cur = overlay.get((target_table, token))
            if cur is None and (target_table, token) not in overlay:
                cur = self.state[target_table].get(token)

            if ev["op"] == "insert":
                if cur is not None:
                    raise ReferenceError(f"{tx_id}: insert on existing key {key}")
                _t, after = self._convert_partial(src_table, ev["after"])
                full = self._complete(target_table, after)
                overlay[(target_table, token)] = full
                out_events.append({
                    "op": "insert", "table": target_table, "key": key, "after": full,
                })
            elif ev["op"] == "delete":
                if cur is None:
                    raise ReferenceError(f"{tx_id}: delete on absent key {key}")
                _t, before = self._convert_partial(src_table, ev.get("before", {}))
                for c, v in before.items():
                    if cur[c] != v:
                        raise ReferenceError(
                            f"{tx_id}: stale before on {c}: {cur[c]!r} != {v!r}"
                        )
                overlay[(target_table, token)] = None
                out_events.append({
                    "op": "delete", "table": target_table, "key": key,
                    "before": dict(cur),
                })
            else:
                if cur is None:
                    raise ReferenceError(f"{tx_id}: update on absent key {key}")
                _t, before = self._convert_partial(src_table, ev["before"])
                for c, v in before.items():
                    if cur[c] != v:
                        raise ReferenceError(
                            f"{tx_id}: stale before on {c}: {cur[c]!r} != {v!r}"
                        )
                _t, after = self._convert_partial(src_table, ev["after"])
                if "new_key" in ev:
                    new_target_table, new_key = self._convert_key(
                        src_table, ev["new_key"]
                    )
                    new_token = self._pk_token(new_target_table, new_key)
                    new_cur = overlay.get((new_target_table, new_token))
                    if new_cur is None and (new_target_table, new_token) not in overlay:
                        new_cur = self.state[new_target_table].get(new_token)
                    if new_cur is not None:
                        raise ReferenceError(
                            f"{tx_id}: key move target exists {new_key}"
                        )
                    full_after = dict(cur)
                    full_after.update(after)
                    for c in self.pk[target_table]:
                        full_after[c] = new_key[c]
                    if new_token == token:
                        overlay[(target_table, token)] = full_after
                        out_events.append({
                            "op": "update", "table": target_table, "key": key,
                            "before": dict(cur), "after": full_after,
                        })
                    else:
                        overlay[(target_table, token)] = None
                        overlay[(target_table, new_token)] = full_after
                        out_events.append({
                            "op": "delete", "table": target_table, "key": key,
                            "before": dict(cur),
                        })
                        out_events.append({
                            "op": "insert", "table": target_table, "key": new_key,
                            "after": full_after,
                        })
                else:
                    full_after = dict(cur)
                    full_after.update(after)
                    overlay[(target_table, token)] = full_after
                    out_events.append({
                        "op": "update", "table": target_table, "key": key,
                        "before": dict(cur), "after": full_after,
                    })
        # commit overlay
        for (table, token), row in overlay.items():
            if row is None:
                self.state[table].pop(token, None)
            else:
                self.state[table][token] = row
        return out_events

    def _complete(self, table: str, partial: dict) -> dict:
        full = {}
        for col in self.table_cols[table]:
            if col in partial:
                full[col] = partial[col]
            elif self.table_cols[table][col].get("nullable"):
                full[col] = None
            else:
                raise ReferenceError(f"missing NOT NULL column {table}.{col}")
        return full
