#!/usr/bin/env python3
"""Order-to-invoice ETL with an embedded data-quality gate.

This is a *real* multi-table transformation command used as the demo oracle.
It joins five tables, materializes an invoice JSONL output and emits the
PASS/FAIL report contract ddreduce expects:

  PASS -> {"status": "PASS", ...}
  FAIL -> {"status": "FAIL", "signature": "E_<CODE>", "violations": [...]}

The handled quality failures still exit 0 with a FAIL report.  Genuine internal
errors (missing input files, malformed JSON) exit non-zero, which ddreduce
classifies as UNRESOLVED rather than FAIL.

Known defect demonstrated in the demo: the zero-price gate fires on order
lines whose catalog product has price = 0, with a stable signature E_ZERO_PRICE
that depends only on the violating line, so the reducer can shrink the whole
database down to the minimal fault-reproducing closure.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"required table missing: {path}")
    rows = []
    with open(path, "r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path.name}:{line_no}: malformed JSON: {exc}") from exc
    return rows


def index(rows: list[dict], key: str) -> dict:
    return {r[key]: r for r in rows}


def run(candidate_dir: Path, report_path: Path) -> int:
    customers = read_jsonl(candidate_dir / "customers.jsonl")
    addresses = read_jsonl(candidate_dir / "addresses.jsonl")
    products = read_jsonl(candidate_dir / "products.jsonl")
    orders = read_jsonl(candidate_dir / "orders.jsonl")
    order_lines = read_jsonl(candidate_dir / "order_lines.jsonl")

    cust = index(customers, "customer_id")
    addr = index(addresses, "address_id")
    prod = index(products, "product_id")
    order = index(orders, "order_id")

    violations: list[dict] = []
    invoices: list[dict] = []

    for line in order_lines:
        lid = line["line_id"]
        product = prod.get(line["product_id"])
        if product is None:
            violations.append(
                {"code": "E_DANGLING_PRODUCT", "line_id": lid}
            )
            continue
        # ---- quality gate: the demonstrated defect -------------------------
        if product["price"] == 0:
            violations.append(
                {
                    "code": "E_ZERO_PRICE",
                    "line_id": lid,
                    "product_id": product["product_id"],
                }
            )
            continue
        if line["quantity"] <= 0:
            violations.append(
                {"code": "E_NON_POSITIVE_QTY", "line_id": lid}
            )
            continue

        o = order.get(line["order_id"])
        if o is None:
            violations.append({"code": "E_DANGLING_ORDER", "line_id": lid})
            continue
        customer = cust.get(o["customer_id"])
        if customer is None:
            violations.append(
                {"code": "E_DANGLING_CUSTOMER", "order_id": o["order_id"]}
            )
            continue
        # Nullable FK: a customer may have no address on file.
        address = addr.get(customer["address_id"]) if customer.get("address_id") else None

        invoices.append(
            {
                "invoice_id": f"INV-{lid}",
                "customer_id": customer["customer_id"],
                "region": address["region"] if address else "UNKNOWN",
                "product_id": product["product_id"],
                "amount": product["price"] * line["quantity"],
            }
        )

    # Deterministic, violation-code based signature: same fault => same
    # signature regardless of how many unrelated rows surround it.
    if violations:
        codes = sorted({v["code"] for v in violations})
        report = {
            "status": "FAIL",
            # Violation codes already carry the E_ prefix; join multiple
            # distinct codes deterministically so the signature is stable.
            "signature": "+".join(codes),
            "violation_count": len(violations),
            "violations": sorted(violations, key=lambda v: json.dumps(v, sort_keys=True)),
            "invoices_emitted": len(invoices),
        }
    else:
        report = {
            "status": "PASS",
            "invoices_emitted": len(invoices),
            "invoice_total": round(sum(i["amount"] for i in invoices), 2),
        }

    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, sort_keys=True, indent=2)
        fh.write("\n")
    # Output artifacts sit next to the report inside the candidate dir; the
    # runner discards the whole directory after the process exits.
    with open(candidate_dir / "invoices.jsonl", "w", encoding="utf-8") as fh:
        for inv in sorted(invoices, key=lambda i: i["invoice_id"]):
            fh.write(json.dumps(inv, sort_keys=True) + "\n")
    return 0


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: etl_quality_gate.py <candidate-dir> <report-file>", file=sys.stderr)
        return 2
    try:
        return run(Path(argv[1]), Path(argv[2]))
    except (FileNotFoundError, ValueError, KeyError) as exc:
        # Internal failure: non-zero exit, no trusted report => UNRESOLVED.
        print(f"etl internal error: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
