"""Naive, independent capture-v2 (schema-evolving) reference converter.

Shares no code with the product ``ddreduce.capture`` package: it parses the
shards itself, de-duplicates with plain dictionaries, groups transactions in
memory, interprets each row image with the schema version the message
declares, and maps fields to the target schema by stable field ID with plain
dict operations.

It exists so the test suite can enumerate small version/transaction
combinations and check the product importer's backfill, rename, widening and
commit-boundary behaviour against an obviously-correct reference.
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


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


class NaiveEvolvingReference:
    def __init__(self, archive: str | Path, history: dict, target_schema: dict,
                 snapshot_rows: dict[str, list[dict]]):
        self.archive = Path(archive)
        self.history = history
        self.target = target_schema
        self.manifest = json.loads(
            (self.archive / "capture.json").read_text("utf-8")
        )
        if self.manifest.get("version") != 2:
            raise ReferenceError(f"not a v2 archive: {self.manifest.get('version')}")
        self.versions = {v["version"]: v for v in history["versions"]}
        self.target_cols = {
            t["name"]: {c["name"]: c for c in t["columns"]}
            for t in target_schema["tables"]
        }
        # workspace state: target table -> pk token -> row
        self.state: dict[str, dict[str, dict]] = {
            t["name"]: {} for t in target_schema["tables"]
        }
        self._snapshot_rows = snapshot_rows
        self.final_fields: dict[str, dict[str, dict]] = {}
        for source_table, table in self.versions[history["versions"][-1]
                                                 ["version"]]["tables"].items():
            self.final_fields[source_table] = {
                f["field_id"]: f for f in table["columns"]
            }

    # ------------------------------------------------------------- helpers
    def _target_table(self, source_table: str) -> str:
        return self.versions[self._last_version()]["tables"][source_table][
            "target_table"
        ]

    def _last_version(self) -> int:
        return max(self.versions)

    def _version_table(self, version: int, source_table: str) -> dict:
        try:
            return self.versions[version]["tables"][source_table]
        except KeyError:
            raise ReferenceError(
                f"version {version}: no table {source_table!r}"
            ) from None

    def _pk_names(self, version: int, source_table: str) -> list[str]:
        table = self._version_table(version, source_table)
        by_id = {f["field_id"]: f for f in table["columns"]}
        return [by_id[fid]["name"] for fid in table["primary_key"]]

    def _final_target_column(self, source_table: str, field_id: str) -> str:
        field = self.final_fields[source_table][field_id]
        return field["name"]

    def _added_fields(self, version: int, source_table: str) -> list[dict]:
        """Fields existing in the final version but not in ``version``."""
        present = {f["field_id"] for f in self._version_table(version, source_table)[
            "columns"]}
        return [self.final_fields[source_table][fid]
                for fid in self.final_fields[source_table]
                if fid not in present]

    # --------------------------------------------------------------- scan
    def scan_and_assemble(self) -> None:
        physical: dict[tuple[str, int], str] = {}
        logical: dict[tuple[str, int], dict] = {}
        logical_coords: dict[tuple[str, int], list] = {}
        ends: dict[str, tuple[str, dict]] = {}
        ranges = {s["name"]: (s["first_offset"], s["last_offset"])
                  for s in self.manifest["streams"]}
        for rel in self.manifest["files"]:
            for line in (self.archive / rel).read_text("utf-8").splitlines():
                if not line.strip():
                    continue
                msg = json.loads(line)
                coord = (msg["stream"], msg["offset"])
                if msg["stream"] not in ranges:
                    raise ReferenceError(f"undeclared stream {coord}")
                lo, hi = ranges[msg["stream"]]
                if not (lo <= msg["offset"] <= hi):
                    raise ReferenceError(f"offset out of range {coord}")
                if coord in physical:
                    if physical[coord] != _canon(msg):
                        raise ReferenceError(f"physical conflict {coord}")
                    continue
                physical[coord] = _canon(msg)
                kind = msg["kind"]
                if kind == "heartbeat":
                    continue
                biz = {k: v for k, v in msg.items()
                       if k not in ("source_id", "stream", "offset")}
                if kind == "change":
                    if "schema_version" not in msg:
                        raise ReferenceError(f"missing schema_version at {coord}")
                    if msg["schema_version"] not in self.versions:
                        raise ReferenceError(
                            f"unknown schema version {msg['schema_version']} at "
                            f"{coord}"
                        )
                    key = (msg["tx_id"], msg["tx_order"])
                    if key in logical and _canon(logical[key]) != _canon(biz):
                        raise ReferenceError(f"logical conflict {key}")
                    logical.setdefault(key, msg)
                    logical_coords.setdefault(key, []).append(coord)
                else:
                    tx_id = msg["tx_id"]
                    if tx_id in ends:
                        old_kind, old = ends[tx_id]
                        if old_kind != kind or _canon(old) != _canon(biz):
                            raise ReferenceError(f"end conflict {tx_id}")
                        continue
                    ends[tx_id] = (kind, msg)
        for name, (lo, hi) in ranges.items():
            present = {o for (s, o) in physical if s == name}
            if present != set(range(lo, hi + 1)):
                raise ReferenceError(f"coverage gap in {name}")

        tx_ids = {tx for (tx, _o) in logical} | set(ends)
        per_tx: dict[str, list[dict]] = {}
        for (tx_id, _order), msg in logical.items():
            per_tx.setdefault(tx_id, []).append(msg)
        for tx_id in tx_ids:
            if tx_id not in ends:
                raise ReferenceError(f"tx {tx_id} has no end decision")
            kind, end = ends[tx_id]
            events = sorted(per_tx.get(tx_id, []), key=lambda e: e["tx_order"])
            if [e["tx_order"] for e in events] != list(
                range(1, end["event_count"] + 1)
            ):
                raise ReferenceError(f"tx {tx_id}: bad tx_order sequence")
            versions = {e.get("schema_version") for e in events}
            if len(versions) > 1:
                raise ReferenceError(f"tx {tx_id} straddles schema versions")
            if kind == "commit" and events:
                declared = versions.pop()
                want = self._version_for_commit(end["commit_seq"])
                if declared != want:
                    raise ReferenceError(
                        f"tx {tx_id}: version {declared} at commit "
                        f"{end['commit_seq']} (expected {want})"
                    )
        self._logical = logical
        self._ends = ends
        self._per_tx = per_tx
        self._logical_coords = logical_coords

    def _version_for_commit(self, commit_seq: int) -> int:
        chosen = self.history["versions"][0]["version"]
        for v in self.history["versions"]:
            if v["effective_commit_seq"] <= commit_seq:
                chosen = v["version"]
        return chosen

    # ------------------------------------------------------------ convert
    def _convert_value(self, version: int, source_table: str, col: str,
                       value: Any) -> tuple[str, Any]:
        table = self._version_table(version, source_table)
        by_name = {f["name"]: f for f in table["columns"]}
        if col not in by_name:
            raise ReferenceError(
                f"version {version}: unknown column {col!r} on {source_table!r}"
            )
        spec = by_name[col]
        if value is None:
            if not spec["nullable"]:
                raise ReferenceError(f"{source_table}.{col}: null in NOT NULL")
            return self._final_target_column(source_table, spec["field_id"]), None
        if spec["type"] == "int":
            if isinstance(value, bool) or not isinstance(value, int):
                raise ReferenceError(f"{source_table}.{col}: not an int: {value!r}")
            out = value
        elif spec["type"] == "float":
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ReferenceError(f"{source_table}.{col}: not a float")
            out = float(value)
        elif spec["type"] == "string":
            if not isinstance(value, str):
                raise ReferenceError(f"{source_table}.{col}: not a string")
            out = value
        elif spec["type"] == "bool":
            if not isinstance(value, bool):
                raise ReferenceError(f"{source_table}.{col}: not a bool")
            out = value
        else:
            raise ReferenceError(f"unsupported type {spec['type']!r}")
        target_col = self._final_target_column(source_table, spec["field_id"])
        target_type = self.target_cols[self._target_table(source_table)][
            target_col]["type"]
        if spec["type"] == "int" and target_type == "float":
            out = float(out)
        return target_col, out

    def _convert_image(self, version: int, source_table: str, image: dict,
                       complete: bool) -> dict:
        table = self._version_table(version, source_table)
        columns = {f["name"] for f in table["columns"]}
        unknown = set(image) - columns
        if unknown:
            raise ReferenceError(
                f"version {version}: unknown columns {sorted(unknown)}"
            )
        out = {}
        for col, value in image.items():
            target_col, converted = self._convert_value(
                version, source_table, col, value
            )
            out[target_col] = converted
        if complete:
            missing = columns - set(image)
            if missing:
                raise ReferenceError(f"incomplete image, missing {sorted(missing)}")
            for field in self._added_fields(version, source_table):
                out[field["name"]] = field.get("backfill") if "backfill" in field \
                    else None
        return out

    def _pk(self, version: int, source_table: str, key: dict) -> tuple[str, dict]:
        names = self._pk_names(version, source_table)
        if set(key) != set(names):
            raise ReferenceError(
                f"version {version}: key {sorted(key)} != {sorted(names)}"
            )
        target_key = {}
        for col, value in key.items():
            target_col, converted = self._convert_value(
                version, source_table, col, value
            )
            target_key[target_col] = converted
        return self._target_table(source_table), target_key

    # ------------------------------------------------------------- restore
    def restore(self, through: int) -> list[dict]:
        if not hasattr(self, "_logical"):
            self.scan_and_assemble()
        self._load_snapshot()
        commits = sorted(
            (end for kind, end in self._ends.values() if kind == "commit"),
            key=lambda e: e["commit_seq"],
        )
        out: list[dict] = []
        for end in commits:
            seq = end["commit_seq"]
            if not (self.manifest["snapshot_commit_seq"] < seq <= through):
                continue
            events = sorted(self._per_tx.get(end["tx_id"], []),
                            key=lambda e: e["tx_order"])
            if not events:
                continue
            out.append(self._apply_tx(end["tx_id"], events))
        return out

    def _load_snapshot(self) -> None:
        version = self.history["versions"][0]["version"]
        first = self.versions[version]
        for source_table, table in first["tables"].items():
            target_table = table["target_table"]
            rows = self._snapshot_rows.get(target_table, [])
            for row in rows:
                data = self._convert_image(version, source_table, row,
                                           complete=True)
                self.state[target_table][self._token(target_table, data)] = data

    def _token(self, target_table: str, row: dict) -> str:
        pk = self.target_cols[target_table]
        table_schema = next(
            t for t in self.target["tables"] if t["name"] == target_table
        )
        return "|".join(
            _canon(row[c]) for c in table_schema["primary_key"]
        )

    def _apply_tx(self, tx_id: str, events: list[dict]) -> dict:
        working = {t: dict(rows) for t, rows in self.state.items()}
        clean = []
        for ev in events:
            version = ev["schema_version"]
            source_table = ev["table"]
            target_table, target_key = self._pk(version, source_table, ev["key"])
            token = "|".join(_canon(target_key[c]) for c in
                             self._target_pk(target_table))
            op = ev["op"]
            if op == "insert":
                after = self._convert_image(version, source_table, ev["after"],
                                            complete=True)
                working[target_table][token] = after
                clean.append({"op": "insert", "table": target_table,
                              "key": target_key, "after": after})
            elif op == "delete":
                before = dict(working[target_table][token])
                working[target_table].pop(token)
                clean.append({"op": "delete", "table": target_table,
                              "key": target_key, "before": before})
            else:
                cur = working[target_table][token]
                before = dict(cur)
                after = dict(cur)
                after.update(self._convert_image(version, source_table,
                                                 ev["after"], complete=False))
                working[target_table][token] = after
                clean.append({"op": "update", "table": target_table,
                              "key": target_key, "before": before,
                              "after": after})
        self.state = working
        return {"tx_id": tx_id, "events": clean}

    def _target_pk(self, target_table: str) -> list[str]:
        for t in self.target["tables"]:
            if t["name"] == target_table:
                return list(t["primary_key"])
        raise ReferenceError(f"unknown target table {target_table!r}")
