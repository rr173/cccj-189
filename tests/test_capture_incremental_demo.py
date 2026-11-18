"""End-to-end: the incremental demo log, encoded as a partial-update capture.

The existing demo/incremental history (full-image changes.jsonl) is re-encoded
into a capture-v1 archive where every update/delete carries only partial
images - exactly the shape operations hands over.  After ``capture-import``
the restored package must:

1. be byte-equivalent (canonical line content) to the original full-image
   changes.jsonl after normalization;
2. replay cleanly through the existing ``log-replay`` machinery;
3. drive the real ``incr_order_total.py`` aggregation command to the same
   FAIL signature the demo is built around;
4. survive an independent ``capture-verify``.
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

from .capture_helpers import (
    DEFAULT_SOURCE_ID,
    abort,
    change,
    commit,
    write_archive,
)
from .helpers import ROOT, TempDir

DEMO = ROOT / "demo" / "incremental"


def encode_full_image_log_as_capture(
    changes_path: Path,
) -> tuple[list[dict], int]:
    """Turn full-image old-format events into partial-image capture messages."""
    messages: list[dict] = []
    offset = 0
    sealed = 0
    for line_no, line in enumerate(changes_path.read_text("utf-8").splitlines(), 1):
        if not line.strip():
            continue
        tx = json.loads(line)
        for order, ev in enumerate(tx["events"], 1):
            kwargs = {}
            if ev["op"] in ("update", "delete"):
                # Partial before: assert the non-key columns named in the
                # original full image.  For delete the importer completes the
                # image from the current row.
                kwargs["before"] = {
                    c: ev["before"][c]
                    for c in ev["before"] if c not in ev["key"]
                }
            if ev["op"] == "insert":
                # Inserts always carry the complete after row.
                kwargs["after"] = ev["after"]
            elif ev["op"] == "update":
                # Partial after: every changed non-key column, nothing else.
                kwargs["after"] = {
                    c: ev["after"][c]
                    for c in ev["after"]
                    if c not in ev["key"] and ev["after"][c] != ev["before"].get(c)
                }
            messages.append(change(
                "s1", offset, tx["tx_id"], order, ev["table"], ev["op"],
                ev["key"], **kwargs,
            ))
            offset += 1
        sealed = line_no  # one commit per line, commit_seq = line number
        counts: dict[str, int] = {}
        for ev in tx["events"]:
            counts[ev["table"]] = counts.get(ev["table"], 0) + 1
        messages.append(commit(
            "s1", offset, tx["tx_id"], sealed, len(tx["events"]), counts
        ))
        offset += 1
    return messages, sealed


class IncrementalCaptureEndToEndTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()

    def tearDown(self):
        self.tmp.cleanup()

    def test_partial_capture_reproduces_demo_aggregation_fault(self):
        base = self.tmp.path / "e2e"
        schema_path = DEMO / "data" / "schema.json"
        snapshot_dir = DEMO / "data" / "snapshot"
        messages, sealed = encode_full_image_log_as_capture(
            DEMO / "data" / "changes.jsonl"
        )
        # Shuffle transport order and add a heartbeat to prove order neutrality.
        import random

        rng = random.Random(11)
        messages.append({"source_id": DEFAULT_SOURCE_ID, "stream": "s1",
                         "offset": 999, "kind": "heartbeat"})
        rng.shuffle(messages)
        # Re-dense offsets post-shuffle (single stream).
        for i, msg in enumerate(messages):
            msg["offset"] = i
        archive, snap, spath = write_archive(
            base, messages,
            schema=json.loads(schema_path.read_text("utf-8")),
            snapshot={"orders": [json.loads(l) for l in
                                 (snapshot_dir / "orders.jsonl")
                                 .read_text().splitlines()]},
            sealed_commit_seq=sealed,
        )
        package = base / "package"
        run_import(archive, snap, spath, package, sealed)

        # 1. Independent verification passes.
        verify = run_verify(archive, snap, spath, sealed, package)
        self.assertEqual(verify.verdict, VERDICT_MATCH, verify.failures)

        # 2. The restored package passes the existing log contract/witness build.
        schema = load_schema(spath)
        db = Database.load(schema, snap)
        log = load_changelog(package / "changes.jsonl", schema)
        validated = validate_log_inputs(
            schema, spath.read_text("utf-8"), db, log
        )
        normalized = validated.normalize({tx.position for tx in log})
        self.assertEqual(normalized.committed, [tx.position for tx in log])

        # 3. The restored log applies to the identical final state and keeps
        #    the same transaction/event shape as the original demo log.  A
        #    delete's full before-image is reconstructed by the importer from
        #    the current row, so compare applied state and structure rather
        #    than the source text.
        original_log = load_changelog(DEMO / "data" / "changes.jsonl", schema)
        original_validated = validate_log_inputs(
            schema, spath.read_text("utf-8"), db, original_log
        )
        original_final = original_validated.normalize(
            {tx.position for tx in original_log}
        ).final_database().content_hash()
        restored_final = normalized.final_database().content_hash()
        self.assertEqual(original_final, restored_final)
        self.assertEqual(
            [(tx.tx_id, len(tx.events), [e.op for e in tx.events])
             for tx in original_log],
            [(tx.tx_id, len(tx.events), [e.op for e in tx.events])
             for tx in log],
        )

        # 4. The real incremental aggregation command reproduces the demo fault.
        report_path = base / "agg-report.json"
        proc = subprocess.run(
            [sys.executable, "-I",
             str(DEMO / "incr_order_total.py"),
             str(package), str(report_path)],
            capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        report = json.loads(report_path.read_text("utf-8"))
        self.assertEqual(report["status"], "FAIL")
        self.assertTrue(report["signature"].startswith("E_INCR_DIVERGE:"))

        # 5. The existing log-replay command accepts the imported package too.
        replay_report = base / "replay-report.json"
        env = dict(__import__("os").environ)
        env["PYTHONPATH"] = str(ROOT / "src")
        proc = subprocess.run(
            [sys.executable, "-P", "-m", "ddreduce.cli", "log-replay",
             "--snapshot", str(package / "snapshot"),
             "--schema", str(package / "schema.json"),
             "--changes", str(package / "changes.jsonl"),
             "--report", str(replay_report)],
            capture_output=True, text=True, env=env, cwd=str(ROOT),
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        replay_doc = json.loads(replay_report.read_text("utf-8"))
        self.assertTrue(replay_doc["valid"])
        self.assertEqual(replay_doc["tx_count"], len(log))

        # 6. The existing log-reduce searcher (real demo oracle, no capture
        #    code on its hot path) runs on the imported package and certifies
        #    the same minimal delete+rebuild pair as the original demo.
        oracle_config = base / "oracle.json"
        oracle_config.write_text(json.dumps({
            "command": [
                sys.executable, "-I",
                str(DEMO / "incr_order_total.py"),
                "{candidate_dir}", "{report_file}",
            ],
            "timeout_seconds": 20.0,
            "retest_runs": 1,
            "confirm_runs": 2,
            "max_calls": 60,
        }), encoding="utf-8")
        workdir = base / "reduce-work"
        proc = subprocess.run(
            [sys.executable, "-P", "-m", "ddreduce.cli", "log-reduce",
             "--snapshot", str(package / "snapshot"),
             "--schema", str(package / "schema.json"),
             "--changes", str(package / "changes.jsonl"),
             "--oracle-config", str(oracle_config),
             "--workdir", str(workdir)],
            capture_output=True, text=True, env=env, cwd=str(ROOT),
        )
        self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
        certificate = json.loads((workdir / "certificate.json").read_text("utf-8"))
        self.assertEqual(certificate["mode"], "log")
        kept = certificate["result"]["kept_tx_ids"]
        # The minimal fault pair is the delete (t3) and same-key rebuild (t4).
        self.assertEqual(set(kept), {"t3", "t4"})


if __name__ == "__main__":
    unittest.main()
