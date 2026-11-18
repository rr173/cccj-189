#!/usr/bin/env python3
"""Deterministic long CDC log generator for ddreduce demos and tests.

Usage:
    python3 demo/incremental/gen_log.py --out DIR [--tx 400] [--keys 120] [--seed 7]

Writes a self-contained package into DIR:
    DIR/schema.json
    DIR/snapshot/orders.jsonl
    DIR/changes.jsonl

The generated history embeds exactly one delete + same-key-rebuild pair (the
fault trigger for the stale-image defect in incr_order_total.py) among many
irrelevant overwrite updates, fresh-key inserts and final deletes.  The fault
key is never touched by any other transaction, so the pair's premises depend
only on the snapshot and on each other.  Everything is derived from the seed:
running the generator twice with the same arguments produces identical files.

Only this generator and the bounded hand-written sample under
demo/incremental/data/ are committed; generated outputs are runtime artifacts
and must not be committed.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path

SCHEMA = {
    "tables": [
        {
            "name": "orders",
            "columns": [
                {"name": "order_id", "type": "string", "nullable": False},
                {"name": "status", "type": "string", "nullable": False},
                {"name": "amount", "type": "float", "nullable": False},
            ],
            "primary_key": ["order_id"],
        }
    ]
}

STATUSES = ["open", "paid", "shipped"]


def row(order_id: str, status: str, amount: float) -> dict:
    return {"order_id": order_id, "status": status, "amount": float(amount)}


def generate(out: Path, n_tx: int, n_keys: int, seed: int) -> dict:
    rng = random.Random(seed)
    keys = [f"K{i:05d}" for i in range(n_keys)]
    fault_key = keys[rng.randrange(n_keys)]
    fault_amount = 10.0 * (1 + rng.randrange(40))
    rebuild_amount = fault_amount + 250.0

    snapshot_rows = [row(k, STATUSES[i % len(STATUSES)], 5.0 * (1 + (i % 20))) for i, k in enumerate(keys)]
    live = {k: dict(r) for k, r in zip(keys, snapshot_rows)}
    fault_row = dict(live[fault_key])

    # Positions of the fault pair: delete around 55%, rebuild around 75%.
    delete_pos = max(1, int(n_tx * 0.55))
    rebuild_pos = max(delete_pos + 1, int(n_tx * 0.75))
    if rebuild_pos >= n_tx:
        rebuild_pos = n_tx - 1
        delete_pos = max(1, rebuild_pos - 2)

    txs = []
    fresh_counter = 0
    for pos in range(n_tx):
        tx_id = f"g{pos:05d}"
        if pos == delete_pos:
            txs.append({
                "tx_id": tx_id,
                "events": [{
                    "op": "delete", "table": "orders",
                    "key": {"order_id": fault_key},
                    "before": dict(live[fault_key]),
                }],
            })
            del live[fault_key]
            continue
        if pos == rebuild_pos:
            after = row(fault_key, fault_row["status"], rebuild_amount)
            txs.append({
                "tx_id": tx_id,
                "events": [{
                    "op": "insert", "table": "orders",
                    "key": {"order_id": fault_key},
                    "after": after,
                }],
            })
            live[fault_key] = dict(after)
            continue
        # Irrelevant transactions, cycling deterministically through shapes.
        shape = rng.randrange(10)
        if shape < 6:
            # Overwrite update of a live key (never the fault key).
            candidates = [k for k in live if k != fault_key]
            k = candidates[rng.randrange(len(candidates))]
            before = dict(live[k])
            after = row(k, before["status"], before["amount"] + float(1 + rng.randrange(9)))
            txs.append({
                "tx_id": tx_id,
                "events": [{
                    "op": "update", "table": "orders",
                    "key": {"order_id": k}, "before": before, "after": after,
                }],
            })
            live[k] = dict(after)
        elif shape < 8:
            # Fresh-key insert.
            fresh_counter += 1
            k = f"N{seed:03d}-{fresh_counter:06d}"
            after = row(k, STATUSES[rng.randrange(len(STATUSES))], float(1 + rng.randrange(90)))
            txs.append({
                "tx_id": tx_id,
                "events": [{
                    "op": "insert", "table": "orders",
                    "key": {"order_id": k}, "after": after,
                }],
            })
            live[k] = dict(after)
        else:
            # Final delete of a fresh key if one exists, else an update.
            fresh_live = [k for k in live if k.startswith(f"N{seed:03d}-")]
            if fresh_live:
                k = fresh_live[rng.randrange(len(fresh_live))]
                txs.append({
                    "tx_id": tx_id,
                    "events": [{
                        "op": "delete", "table": "orders",
                        "key": {"order_id": k}, "before": dict(live[k]),
                    }],
                })
                del live[k]
            else:
                candidates = [k for k in live if k != fault_key]
                k = candidates[rng.randrange(len(candidates))]
                before = dict(live[k])
                after = row(k, before["status"], before["amount"] + 1.0)
                txs.append({
                    "tx_id": tx_id,
                    "events": [{
                        "op": "update", "table": "orders",
                        "key": {"order_id": k}, "before": before, "after": after,
                    }],
                })
                live[k] = dict(after)

    out.mkdir(parents=True, exist_ok=True)
    (out / "snapshot").mkdir(exist_ok=True)
    with open(out / "schema.json", "w", encoding="utf-8") as fh:
        json.dump(SCHEMA, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    with open(out / "snapshot" / "orders.jsonl", "w", encoding="utf-8") as fh:
        for r in snapshot_rows:
            fh.write(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n")
    with open(out / "changes.jsonl", "w", encoding="utf-8") as fh:
        for tx in txs:
            fh.write(json.dumps(tx, ensure_ascii=False, sort_keys=True) + "\n")
    return {
        "fault_delete_tx": f"g{delete_pos:05d}",
        "fault_rebuild_tx": f"g{rebuild_pos:05d}",
        "fault_key": fault_key,
        "tx_count": n_tx,
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    parser.add_argument("--tx", type=int, default=400)
    parser.add_argument("--keys", type=int, default=120)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args(argv)
    if args.tx < 4 or args.keys < 2:
        parser.error("--tx >= 4 and --keys >= 2 required")
    info = generate(Path(args.out), args.tx, args.keys, args.seed)
    print(json.dumps(info, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
