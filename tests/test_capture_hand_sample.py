"""The hand-computed cross-partition capture sample, asserted exactly.

Scenario (accounts table, key/amount/note):

* snapshot: key ``k`` has amount=10, note="old"; snapshot_commit_seq=12;
* tx A (commit_seq 13): amount 10 -> 20, then note explicitly set to null;
* tx B (commit_seq 14): move key ``k`` to key ``z``;
* tx C: aborted change amount -> 999, must never appear;
* A's messages are retransmitted (physically and logically), shards are
  shuffled and B's commit arrives before B's change.

The restored output must be exactly one line for A with two full-image
updates and one line for B expanded into delete(k) + insert(z), with B's
complete before image equal to amount=20/note=null.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from ddreduce.capture.importer import run_import
from ddreduce.capture.verifier import VERDICT_MATCH, run_verify

from .helpers import ROOT, TempDir

SAMPLE = ROOT / "demo" / "capture" / "sample"


class HandSampleTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()

    def tearDown(self):
        self.tmp.cleanup()

    def test_hand_computed_restoration(self):
        out = self.tmp.path / "package"
        result = run_import(
            SAMPLE / "archive",
            SAMPLE / "snapshot",
            SAMPLE / "schema.json",
            out,
            through=14,
        )
        lines = [
            json.loads(line)
            for line in (out / "changes.jsonl").read_text("utf-8").splitlines()
        ]
        self.assertEqual([tx["tx_id"] for tx in lines], ["A", "B"])

        # A: one transaction line, two events, full images.
        tx_a = lines[0]
        self.assertEqual(len(tx_a["events"]), 2)
        ev0, ev1 = tx_a["events"]
        self.assertEqual(ev0["op"], "update")
        self.assertEqual(ev0["before"],
                         {"key": "k", "amount": 10, "note": "old"})
        self.assertEqual(ev0["after"],
                         {"key": "k", "amount": 20, "note": "old"})
        self.assertEqual(ev1["op"], "update")
        self.assertEqual(ev1["before"],
                         {"key": "k", "amount": 20, "note": "old"})
        # Explicit null is a written null, distinct from preservation.
        self.assertEqual(ev1["after"],
                         {"key": "k", "amount": 20, "note": None})

        # B: key move expanded at the same position into delete + insert.
        tx_b = lines[1]
        self.assertEqual([e["op"] for e in tx_b["events"]], ["delete", "insert"])
        delete_ev, insert_ev = tx_b["events"]
        # B's complete before must be the state left by A: 20 / null.
        self.assertEqual(delete_ev["before"],
                         {"key": "k", "amount": 20, "note": None})
        self.assertEqual(delete_ev["key"], {"key": "k"})
        self.assertEqual(insert_ev["key"], {"key": "z"})
        self.assertEqual(insert_ev["after"],
                         {"key": "z", "amount": 20, "note": None})

        # The aborted amount=999 change must appear nowhere.
        raw = (out / "changes.jsonl").read_text("utf-8")
        self.assertNotIn("999", raw)

        counts = result.report["counts"]
        self.assertEqual(counts["aborted_total"], 1)
        self.assertEqual(counts["aborted_events"], 1)
        self.assertEqual(counts["exported_transactions"], 2)
        self.assertEqual(counts["exported_events"], 4)
        self.assertGreaterEqual(counts["physical_duplicates"], 1)
        self.assertGreaterEqual(counts["logical_event_duplicates"], 1)
        self.assertEqual(counts["heartbeats"], 2)
        self.assertEqual(counts["key_move_expansions"], 2)

        # Every expanded B event traces back to the one source logical event.
        map_rows = [
            json.loads(line)
            for line in (out / "capture-map.jsonl").read_text("utf-8").splitlines()
        ]
        b_rows = [r for r in map_rows if r["tx_id"] == "B"]
        self.assertEqual(len(b_rows), 2)
        sources = {(r["source"]["tx_id"], r["source"]["tx_order"]) for r in b_rows}
        self.assertEqual(sources, {("B", 1)})
        for row in b_rows:
            self.assertTrue(row["source"]["expanded_from_key_move"])
            self.assertTrue(row["source"]["coordinates"])

    def test_independent_verification_matches_sample_package(self):
        out = self.tmp.path / "package"
        run_import(
            SAMPLE / "archive", SAMPLE / "snapshot", SAMPLE / "schema.json",
            out, through=14,
        )
        verify = run_verify(
            SAMPLE / "archive", SAMPLE / "snapshot", SAMPLE / "schema.json",
            14, out,
        )
        self.assertEqual(verify.verdict, VERDICT_MATCH)
        self.assertEqual(verify.failures, [])

    def test_window_through_13_excludes_b_but_keeps_archive_checks(self):
        out = self.tmp.path / "package13"
        result = run_import(
            SAMPLE / "archive", SAMPLE / "snapshot", SAMPLE / "schema.json",
            out, through=13,
        )
        lines = [
            json.loads(line)
            for line in (out / "changes.jsonl").read_text("utf-8").splitlines()
        ]
        self.assertEqual([tx["tx_id"] for tx in lines], ["A"])
        self.assertEqual(result.report["counts"]["out_of_window_commits"], 1)
        # B and the abort still participated in the archive closure checks.
        self.assertEqual(result.report["counts"]["transactions_total"], 3)

    def test_sample_is_deterministic_across_regeneration(self):
        # Regenerating with the same arguments must yield the identical
        # manifest and shard bytes (bounded reproducible sample).
        import subprocess
        import sys

        regen = self.tmp.path / "regen"
        proc = subprocess.run(
            [sys.executable, "-I",
             str(ROOT / "demo" / "capture" / "gen_capture.py"),
             "--out", str(regen), "--flavor", "hand",
             "--seed", "7", "--shards", "3"],
            capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)

        def tree_bytes(root: Path):
            return {
                p.relative_to(root).as_posix(): p.read_bytes()
                for p in sorted(root.rglob("*")) if p.is_file()
            }

        self.assertEqual(
            tree_bytes(regen / "archive"),
            tree_bytes(SAMPLE / "archive"),
        )


if __name__ == "__main__":
    unittest.main()
