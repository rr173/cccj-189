"""End-to-end: an evolving archive driving the real incremental demo.

The demo's snapshot and final rows are re-expressed as two schema versions of
the ``orders`` table: version 1 calls the amount field ``total`` (typed
``int``); version 2 (effective mid-history) renames it to ``amount`` and
widens it to ``float``.  The archive interleaves transactions written under
both versions.  After conversion the package must:

1. be accepted by the existing ``log-replay`` machinery;
2. drive ``demo/incremental/incr_order_total.py`` to the same FAIL signature
   the full-image demo produces;
3. pass independent capture-v2 verification.
"""

from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

from ddreduce.capture.importer import run_import
from ddreduce.capture.verifier import VERDICT_MATCH, run_verify
from ddreduce.logmodel import load_changelog
from ddreduce.model import Database
from ddreduce.replay import validate_log_inputs
from ddreduce.schema import load_schema

from .capture_helpers import commit, field, v2_change, write_v2_archive
from .helpers import ROOT, TempDir

DEMO = ROOT / "demo" / "incremental"
SCHEMA_PATH = DEMO / "data" / "schema.json"
SNAPSHOT = DEMO / "data" / "snapshot"
CHANGES = DEMO / "data" / "changes.jsonl"

HISTORY = {
    "format": "capture-schema-history-v1",
    "versions": [
        {"version": 1, "effective_commit_seq": 0, "tables": {
            "orders": {"target_table": "orders", "primary_key": ["f-id"],
                       "columns": [
                           field("f-id", "order_id", "string", False),
                           field("f-status", "status", "string", False),
                           field("f-amount", "total", "int", False)],
                       "unique_keys": [], "foreign_keys": []}}},
        {"version": 2, "effective_commit_seq": 3, "tables": {
            "orders": {"target_table": "orders", "primary_key": ["f-id"],
                       "columns": [
                           field("f-id", "order_id", "string", False),
                           field("f-status", "status", "string", False),
                           field("f-amount", "amount", "float", False)],
                       "unique_keys": [], "foreign_keys": []}}},
    ],
}


def demo_schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text("utf-8"))


def demo_snapshot_rows() -> list[dict]:
    return [json.loads(line) for line in
            (SNAPSHOT / "orders.jsonl").read_text("utf-8").splitlines()]


def evolving_messages() -> tuple[list[dict], dict]:
    """Re-encode the demo log as two schema versions.

    The first two demo transactions are written at version 1 (``total`` is an
    int), the rest at version 2.  Values are integral so the int -> float
    widening is exact.
    """
    snapshot = {r["order_id"]: dict(r) for r in demo_snapshot_rows()}
    start_total = {k: int(r["amount"]) for k, r in snapshot.items()}
    live_total = dict(start_total)
    messages: list[dict] = []
    offset = 0
    commit_seq = 0
    for line_no, line in enumerate(CHANGES.read_text("utf-8").splitlines(), 1):
        if not line.strip():
            continue
        tx = json.loads(line)
        commit_seq += 1
        version = 1 if commit_seq <= 2 else 2
        for order, ev in enumerate(tx["events"], 1):
            key = ev["key"]
            if ev["op"] == "delete":
                messages.append(v2_change(
                    "s1", offset, tx["tx_id"], order, "orders", "delete", key,
                    before={"status": ev["before"]["status"],
                            "total" if version == 1 else "amount":
                                int(ev["before"]["amount"])},
                    schema_version=version,
                ))
                live_total.pop(key["order_id"], None)
            elif ev["op"] == "insert":
                live_total[key["order_id"]] = int(ev["after"]["amount"])
                messages.append(v2_change(
                    "s1", offset, tx["tx_id"], order, "orders", "insert", key,
                    after={"order_id": key["order_id"],
                           "status": ev["after"]["status"],
                           "total" if version == 1 else "amount":
                               int(ev["after"]["amount"])},
                    schema_version=version,
                ))
            else:
                before = ev["before"]
                after = ev["after"]
                col = "total" if version == 1 else "amount"
                before_img = {"status": before["status"],
                              col: int(before["amount"])}
                after_img = {}
                if after["status"] != before["status"]:
                    after_img["status"] = after["status"]
                if int(after["amount"]) != int(before["amount"]):
                    after_img[col] = int(after["amount"])
                live_total[key["order_id"]] = int(after["amount"])
                messages.append(v2_change(
                    "s1", offset, tx["tx_id"], order, "orders", "update", key,
                    before=before_img, after=after_img,
                    schema_version=version,
                ))
            offset += 1
        counts = {"orders": len(tx["events"])}
        messages.append(commit("s1", offset, tx["tx_id"], commit_seq,
                               len(tx["events"]), counts))
        offset += 1
    snapshot_rows = [
        {"order_id": r["order_id"], "status": r["status"],
         "total": int(r["amount"])}
        for r in demo_snapshot_rows()
    ]
    return messages, {
        "snapshot_rows": snapshot_rows,
        "sealed": commit_seq,
    }


class EvolvingIncrementalDemoTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()

    def tearDown(self):
        self.tmp.cleanup()

    def _build(self):
        base = self.tmp.path / "e2e"
        messages, extra = evolving_messages()
        archive, snap, spath, hpath = write_v2_archive(
            base, messages, history=HISTORY, schema=demo_schema(),
            snapshot_rows={"orders": extra["snapshot_rows"]},
        )
        return base, archive, snap, spath, hpath, extra["sealed"]

    def test_converted_package_replays_and_reproduces_demo_fault(self):
        base, archive, snap, spath, hpath, sealed = self._build()
        out = base / "pkg"
        result = run_import(archive, snap, spath, out, sealed,
                            history_path=hpath)
        self.assertEqual(result.report["counts"]["exported_transactions"],
                         len(CHANGES.read_text("utf-8").splitlines()))

        schema = load_schema(out / "schema.json")
        db = Database.load(schema, out / "snapshot")
        log = load_changelog(out / "changes.jsonl", schema)
        validate_log_inputs(
            schema, (out / "schema.json").read_text("utf-8"), db, log
        )

        report_path = base / "report.json"
        proc = subprocess.run(
            [sys.executable, "-I", str(DEMO / "incr_order_total.py"),
             str(out), str(report_path)],
            capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        report = json.loads(report_path.read_text("utf-8"))
        self.assertEqual(report["status"], "FAIL")
        self.assertTrue(report["signature"].startswith("E_INCR_DIVERGE:"))

        verify = run_verify(archive, snap, spath, sealed, out,
                            history_path=hpath)
        self.assertEqual(verify.verdict, VERDICT_MATCH, verify.failures)

    def test_converted_rows_use_the_unified_schema_only(self):
        base, archive, snap, spath, hpath, sealed = self._build()
        out = base / "pkg"
        run_import(archive, snap, spath, out, sealed, history_path=hpath)
        for line in (out / "changes.jsonl").read_text("utf-8").splitlines():
            tx = json.loads(line)
            for ev in tx["events"]:
                for image in ("before", "after"):
                    if image in ev:
                        self.assertIn("amount", ev[image])
                        self.assertNotIn("total", ev[image])
        snapshot_lines = (out / "snapshot" / "orders.jsonl").read_text(
            "utf-8"
        ).splitlines()
        for line in snapshot_lines:
            row = json.loads(line)
            self.assertIn("amount", row)
            self.assertNotIn("total", row)


if __name__ == "__main__":
    unittest.main()
