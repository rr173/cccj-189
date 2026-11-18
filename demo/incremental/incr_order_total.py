#!/usr/bin/env python3
"""Incremental order-amount aggregation over a CDC package.

A genuine incremental-processing command under test.  It reads a
self-contained CDC candidate directory:

    <candidate>/snapshot/orders.jsonl   fixed starting snapshot
    <candidate>/schema.json             table schema (primary key metadata)
    <candidate>/changes.jsonl           committed transactions to consume

and computes the total order amount per status two ways:

  * batch:        apply the whole log to the snapshot, then aggregate the
                  final table (the reference result);
  * incremental:  start from the snapshot aggregates and maintain them event
                  by event (the path taken by the production streaming job).

The incremental path keeps a per-key image cache to compute update deltas.
The demonstrated defect: a ``delete`` event subtracts the amount but does NOT
evict the cached image, so a later same-key rebuild (insert) is mistaken for
an overwrite of the stale image and the stale amount is subtracted a second
time.  Pure overwrite updates are handled correctly; the divergence appears
exactly when a delete is followed by a same-key rebuild.

Report contract (ddreduce oracle):
  PASS -> {"status": "PASS", ...}
  FAIL -> {"status": "FAIL", "signature": "E_INCR_DIVERGE:<status>[+<status>...]"}

The signature is derived from the *actual* set of status groups whose
incremental and batch totals diverge — never from transaction ids, candidate
lengths, or anything hardcoded.  Handled failures exit 0; genuine internal
errors (missing files, malformed log, contract violations) exit non-zero,
which ddreduce classifies as UNRESOLVED rather than FAIL.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

TABLE = "orders"
GROUP_COLUMN = "status"
AMOUNT_COLUMN = "amount"


class ContractError(ValueError):
    """The candidate package violates the expected CDC input contract."""


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"required file missing: {path}")
    rows = []
    with open(path, "r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ContractError(f"{path.name}:{line_no}: malformed JSON: {exc}")
    return rows


def load_pk(schema_path: Path) -> list[str]:
    with open(schema_path, "r", encoding="utf-8") as fh:
        schema = json.load(fh)
    for table in schema.get("tables", []):
        if table.get("name") == TABLE:
            pk = table.get("primary_key")
            if not pk:
                raise ContractError(f"table {TABLE!r} has no primary key in schema")
            return list(pk)
    raise ContractError(f"schema does not define table {TABLE!r}")


def key_of(pk: list[str], row: dict) -> tuple:
    try:
        return tuple(row[c] for c in pk)
    except KeyError as exc:
        raise ContractError(f"row is missing primary key column {exc}")


def load_transactions(path: Path) -> list[dict]:
    txs = []
    for obj in read_jsonl(path):
        if not isinstance(obj, dict) or "tx_id" not in obj or not isinstance(
            obj.get("events"), list
        ):
            raise ContractError("changes.jsonl: every line needs tx_id and events")
        txs.append(obj)
    return txs


def check_event(pk: list[str], ev: dict) -> None:
    if ev.get("table") != TABLE:
        raise ContractError(f"event for unknown table {ev.get('table')!r}")
    if ev.get("op") not in ("insert", "update", "delete"):
        raise ContractError(f"unknown op {ev.get('op')!r}")
    if not isinstance(ev.get("key"), dict):
        raise ContractError("event key must be an object")


def batch_final_state(snapshot_rows: list[dict], txs: list[dict], pk: list[str]) -> dict:
    """Apply the log to the snapshot; the reference (correct) final table."""
    state = {key_of(pk, row): row for row in snapshot_rows}
    for tx in txs:
        for ev in tx["events"]:
            check_event(pk, ev)
            key = key_of(pk, ev["key"])
            op = ev["op"]
            if op == "insert":
                if key in state:
                    raise ContractError(f"insert of existing key {key}")
                state[key] = ev["after"]
            elif op == "update":
                if key not in state or state[key] != ev["before"]:
                    raise ContractError(f"update before-image mismatch at {key}")
                state[key] = ev["after"]
            else:
                if key not in state or state[key] != ev["before"]:
                    raise ContractError(f"delete before-image mismatch at {key}")
                del state[key]
    return state


def group_totals(rows) -> dict:
    totals: dict[str, float] = {}
    for row in rows:
        totals[row[GROUP_COLUMN]] = totals.get(row[GROUP_COLUMN], 0.0) + row[AMOUNT_COLUMN]
    return totals


class IncrementalTotals:
    """The production streaming aggregation, including its stale-image defect."""

    def __init__(self, snapshot_rows: list[dict], pk: list[str]):
        self.pk = pk
        self.totals = group_totals(snapshot_rows)
        # Per-key image cache used to compute update deltas.
        self.image = {key_of(pk, row): row for row in snapshot_rows}

    def apply(self, ev: dict) -> None:
        check_event(self.pk, ev)
        key = key_of(self.pk, ev["key"])
        op = ev["op"]
        if op == "insert":
            stale = self.image.get(key)
            if stale is not None:
                # DEFECT: after a delete the stale image is still cached, so a
                # same-key rebuild is treated as an overwrite of the stale row
                # and the stale amount is subtracted a second time.
                self.totals[stale[GROUP_COLUMN]] -= stale[AMOUNT_COLUMN]
            after = ev["after"]
            self.totals[after[GROUP_COLUMN]] = (
                self.totals.get(after[GROUP_COLUMN], 0.0) + after[AMOUNT_COLUMN]
            )
            self.image[key] = after
        elif op == "update":
            current = self.image.get(key, ev["before"])
            self.totals[current[GROUP_COLUMN]] -= current[AMOUNT_COLUMN]
            after = ev["after"]
            self.totals[after[GROUP_COLUMN]] = (
                self.totals.get(after[GROUP_COLUMN], 0.0) + after[AMOUNT_COLUMN]
            )
            self.image[key] = after
        else:  # delete
            before = ev["before"]
            self.totals[before[GROUP_COLUMN]] -= before[AMOUNT_COLUMN]
            # DEFECT: the cached image is not evicted here.
            # Correct behavior would be: self.image.pop(key, None)


def run(candidate_dir: Path, report_path: Path) -> int:
    pk = load_pk(candidate_dir / "schema.json")
    snapshot_rows = read_jsonl(candidate_dir / "snapshot" / f"{TABLE}.jsonl")
    txs = load_transactions(candidate_dir / "changes.jsonl")

    final_state = batch_final_state(snapshot_rows, txs, pk)
    batch = group_totals(final_state.values())

    engine = IncrementalTotals(snapshot_rows, pk)
    for tx in txs:
        for ev in tx["events"]:
            engine.apply(ev)
    incremental = engine.totals

    groups = sorted(set(batch) | set(incremental))
    divergence = {
        g: {"incremental": incremental.get(g, 0.0), "batch": batch.get(g, 0.0)}
        for g in groups
        if incremental.get(g, 0.0) != batch.get(g, 0.0)
    }
    if divergence:
        # The signature is computed from the actual diverging groups only.
        signature = "E_INCR_DIVERGE:" + "+".join(sorted(divergence))
        report = {
            "status": "FAIL",
            "signature": signature,
            "divergence": divergence,
            "incremental_totals": incremental,
            "batch_totals": batch,
            "transactions_consumed": len(txs),
        }
    else:
        report = {
            "status": "PASS",
            "incremental_totals": incremental,
            "batch_totals": batch,
            "transactions_consumed": len(txs),
        }
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, sort_keys=True, indent=2)
        fh.write("\n")
    return 0


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: incr_order_total.py <candidate-dir> <report-file>", file=sys.stderr)
        return 2
    try:
        return run(Path(argv[1]), Path(argv[2]))
    except (FileNotFoundError, ContractError, KeyError, TypeError) as exc:
        # Internal failure: non-zero exit, no trusted report => UNRESOLVED.
        print(f"incremental aggregation internal error: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
