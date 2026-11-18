"""A deliberately naive, independent CDC replayer used to cross-check ddreduce.

This module shares no code with the product.  It re-implements the change-log
contract and the version/absence causality model from first principles with
plain dicts and full-state validation at every commit, then enumerates
transaction selections exhaustively for small logs.  Tests compare its kept
sequences, per-commit states and deletion neighbors against the product's
replay engine and certificates.

Version model (same contract, independent implementation):
  row version:   ("BASE",)                 snapshot row
                 ("TX", tx_pos, ev_idx)    written by that original event
  absence:       ("INIT",)                 the key never existed
                 ("DEL", tx_pos, ev_idx)   produced by that original delete
"""

from __future__ import annotations

import json
from pathlib import Path

ROW = "row"
ABSENT = "absent"


def load_schema(path):
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    tables = {}
    for t in raw["tables"]:
        cols = {c["name"]: c for c in t["columns"]}
        tables[t["name"]] = {
            "pk": list(t["primary_key"]),
            "columns": cols,
            "unique_keys": [list(t["primary_key"])]
            + [list(k) for k in t.get("unique_keys", [])],
            "fks": [
                {
                    "columns": list(f["columns"]),
                    "ref_table": f["ref_table"],
                    "ref_columns": list(f["ref_columns"]),
                    "nullable": all(cols[c].get("nullable", False) for c in f["columns"]),
                }
                for f in t.get("foreign_keys", [])
            ],
        }
    return tables


def pk_of(table, row):
    return tuple(json.dumps(row[c], sort_keys=True) for c in table["pk"])


def load_snapshot(tables, snapshot_dir):
    state = {}
    for name, table in tables.items():
        rows = {}
        path = Path(snapshot_dir) / f"{name}.jsonl"
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                row = json.loads(line)
                rows[pk_of(table, row)] = row
        state[name] = rows
    return state


def load_log(path):
    txs = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip():
            obj = json.loads(line)
            txs.append(obj)
    return txs


class NaiveReplayer:
    def __init__(self, schema_path, snapshot_dir, changes_path):
        self.tables = load_schema(schema_path)
        self.txs = load_log(changes_path)
        # cur[table][pk] = (ROW, version, row) | (ABSENT, absence_id)
        self.initial = {
            name: {pk: (ROW, ("BASE",), row) for pk, row in rows.items()}
            for name, rows in load_snapshot(self.tables, snapshot_dir).items()
        }
        self.witnesses = self._build_witnesses()

    # ------------------------------------------------------------- helpers
    def _get(self, cur, overlay, table, pk):
        if (table, pk) in overlay:
            return overlay[(table, pk)]
        return cur[table].get(pk, (ABSENT, ("INIT",), None))

    def _validate_state(self, cur):
        # Uniqueness over the whole committed state (NULLs are distinct).
        for name, table in self.tables.items():
            for uk in table["unique_keys"]:
                seen = set()
                for pk, entry in cur[name].items():
                    if entry[0] != ROW:
                        continue
                    row = entry[2]
                    vals = tuple(row.get(c) for c in uk)
                    if any(v is None for v in vals):
                        continue
                    token = json.dumps(vals)
                    if token in seen:
                        return f"unique violation on {name}{uk}"
                    seen.add(token)
        # Foreign keys over the whole committed state.
        for name, table in self.tables.items():
            for fk in table["fks"]:
                ref = self.tables[fk["ref_table"]]
                for pk, entry in cur[name].items():
                    if entry[0] != ROW:
                        continue
                    row = entry[2]
                    vals = [row.get(c) for c in fk["columns"]]
                    if any(v is None for v in vals):
                        if not fk["nullable"]:
                            return f"null in required fk {name}{fk['columns']}"
                        continue
                    found = False
                    for _pk2, pentry in cur[fk["ref_table"]].items():
                        if pentry[0] != ROW:
                            continue
                        prow = pentry[2]
                        if [prow.get(c) for c in fk["ref_columns"]] == vals:
                            found = True
                            break
                    if not found and not fk["nullable"]:
                        return f"fk violation {name}{fk['columns']} -> {fk['ref_table']}"
        return None

    def _final_rows(self, cur):
        return {
            name: {pk: entry[2] for pk, entry in entries.items() if entry[0] == ROW}
            for name, entries in cur.items()
        }

    # ------------------------------------------------------------- witness
    def _build_witnesses(self):
        cur = {t: dict(entries) for t, entries in self.initial.items()}
        witnesses = []
        for pos, tx in enumerate(self.txs):
            overlay = {}
            reads = []
            for idx, ev in enumerate(tx["events"]):
                table = ev["table"]
                pk = pk_of(self.tables[table], ev["key"])
                entry = self._get(cur, overlay, table, pk)
                if ev["op"] == "insert":
                    assert entry[0] == ABSENT, f"tx {pos} ev {idx}: insert not absent"
                    reads.append(entry[1])
                    overlay[(table, pk)] = (ROW, ("TX", pos, idx), ev["after"])
                else:
                    assert entry[0] == ROW, f"tx {pos} ev {idx}: {ev['op']} not a row"
                    assert entry[2] == ev["before"], f"tx {pos} ev {idx}: before mismatch"
                    reads.append(entry[1])
                    if ev["op"] == "update":
                        overlay[(table, pk)] = (ROW, ("TX", pos, idx), ev["after"])
                    else:
                        overlay[(table, pk)] = (ABSENT, ("DEL", pos, idx), None)
            merged = {t: dict(entries) for t, entries in cur.items()}
            for (table, pk), entry in overlay.items():
                merged[table][pk] = entry
            problem = self._validate_state(merged)
            assert problem is None, f"tx {pos} commit invalid: {problem}"
            # Required-FK parent versions of surviving written rows.
            parents = []
            for (table, pk), entry in overlay.items():
                if entry[0] != ROW:
                    continue
                row = entry[2]
                for fk in self.tables[table]["fks"]:
                    if fk["nullable"]:
                        continue
                    vals = [row.get(c) for c in fk["columns"]]
                    if any(v is None for v in vals):
                        continue
                    for _pk2, pentry in merged[fk["ref_table"]].items():
                        if pentry[0] == ROW and [
                            pentry[2].get(c) for c in fk["ref_columns"]
                        ] == vals:
                            parents.append((table, pk, tuple(fk["columns"]), pentry[1]))
                            break
            witnesses.append({"reads": reads, "parents": parents})
            cur = merged
        self.final_state_full = self._final_rows(cur)
        return witnesses

    # -------------------------------------------------------------- replay
    def replay(self, keep):
        """Replay the selection (set of tx positions); return (committed, state)."""
        cur = {t: dict(entries) for t, entries in self.initial.items()}
        committed = []
        for pos, tx in enumerate(self.txs):
            if pos not in keep:
                continue
            witness = self.witnesses[pos]
            overlay = {}
            ok = True
            for idx, ev in enumerate(tx["events"]):
                table = ev["table"]
                pk = pk_of(self.tables[table], ev["key"])
                entry = self._get(cur, overlay, table, pk)
                want = witness["reads"][idx]
                if ev["op"] == "insert":
                    if entry[0] != ABSENT or entry[1] != want:
                        ok = False
                        break
                    overlay[(table, pk)] = (ROW, ("TX", pos, idx), ev["after"])
                else:
                    if entry[0] != ROW or entry[1] != want:
                        ok = False
                        break
                    if ev["op"] == "update":
                        overlay[(table, pk)] = (ROW, ("TX", pos, idx), ev["after"])
                    else:
                        overlay[(table, pk)] = (ABSENT, ("DEL", pos, idx), None)
            if ok:
                merged = {t: dict(entries) for t, entries in cur.items()}
                for (table, pk), entry in overlay.items():
                    merged[table][pk] = entry
                if self._validate_state(merged) is not None:
                    ok = False
            if ok:
                for (table, pk, _cols, want_parent) in witness["parents"]:
                    row = merged[table][pk][2]
                    vals = [row.get(c) for c in _cols]
                    found = None
                    for fk in self.tables[table]["fks"]:
                        if list(fk["columns"]) == list(_cols):
                            for _pk2, pentry in merged[fk["ref_table"]].items():
                                if pentry[0] == ROW and [
                                    pentry[2].get(c) for c in fk["ref_columns"]
                                ] == vals:
                                    found = pentry[1]
                                    break
                    if found != want_parent:
                        ok = False
                        break
            if ok:
                cur = merged
                committed.append(pos)
        return committed, self._final_rows(cur)

    def enumerate_all(self):
        """Every selection -> (committed positions, final state)."""
        n = len(self.txs)
        out = {}
        for mask in range(1 << n):
            keep = {i for i in range(n) if mask & (1 << i)}
            out[frozenset(keep)] = self.replay(keep)
        return out

    def per_commit_states(self):
        """Final state after each prefix of committed transactions."""
        states = []
        for i in range(len(self.txs)):
            _committed, state = self.replay(set(range(i + 1)))
            states.append(state)
        return states
