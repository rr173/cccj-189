#!/usr/bin/env python3
"""Configurable mock oracle for ddreduce tests.

Standalone subprocess: it never imports the ddreduce package, so it mimics a
real external data-quality command.  It reads JSONL tables from the candidate
directory and writes a PASS/FAIL report.

Behavior is driven by a mock-config JSON file whose path is passed on argv:

  {
    "fail_rows":     ["P/p", ...],   # row_ids whose presence forces FAIL
    "fail_signature": "SIG-...",     # signature for those rows
    "extra_signatures": {"P/x": "SIG-OTHER"},
    "flaky":         {"P/p": {"pass_first": 1}},   # first N invocations PASS
    "timeout_rows":  ["P/t"],        # presence makes the command hang
    "crash_rows":    ["P/c"],        # presence => exit 3, no valid report
    "summary_field": "run_id",       # extra field varies while signature stays
    "spawn_children": 2,             # spawn N sleepers (pgroup-kill testing)
    "state_file": "/tmp/..."         # flaky invocation counter, persisted here
  }

Exit code is always 0 on handled PASS/FAIL; only crash_rows (or an internal
error) yields a non-zero exit, which ddreduce must classify UNRESOLVED.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path


def _typed(value) -> str:
    """Encode a scalar the same way ddreduce.values.canonical_scalar does."""
    if value is None:
        return "null:"
    if isinstance(value, bool):
        return f"bool:{'1' if value else '0'}"
    if isinstance(value, int):
        return f"int:{value}"
    if isinstance(value, float):
        return f"float:{value!r}"
    return "str:" + json.dumps(value, ensure_ascii=False, sort_keys=True)


def _row_id(table: str, row: dict, pk_columns: list[str]) -> str:
    # Mirror ddreduce's stable row id (table + typed PK parts) without
    # importing the package: the mock must remain an external program.
    parts = [table] + [_typed(row[c]) for c in sorted(pk_columns)]
    return "/".join(parts)


def _predicate_hit(rows, predicates: list[dict]) -> list[str]:
    """Match rows by {table, column, equals}; used for composite-key fixtures."""
    hits = []
    for table, row in rows:
        for pred in predicates:
            if (
                table == pred["table"]
                and pred["column"] in row
                and row[pred["column"]] == pred["equals"]
            ):
                hits.append(pred.get("label", f"{table}.{pred['column']}={pred['equals']}"))
    return sorted(set(hits))


def _read_tables(
    candidate_dir: Path, pk_map: dict[str, list[str]]
) -> list[tuple[str, dict, list[str]]]:
    rows = []
    for path in sorted(candidate_dir.glob("*.jsonl")):
        table = path.stem
        pk = pk_map.get(table, [])
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if not pk:  # ad-hoc fallback for standalone fixture runs
                    pk = ["id"] if "id" in obj else sorted(obj)[:1]
                rows.append((table, obj, pk))
    return rows


def _pk_key(key_obj: dict, pk: list[str]) -> str:
    return json.dumps([key_obj[c] for c in pk], sort_keys=True)


def _read_package(
    candidate_dir: Path, pk_map: dict[str, list[str]]
) -> tuple[list[tuple[str, dict, list[str]]], list]:
    """Read a candidate: static tables, or a CDC package (snapshot/ + log).

    Returns (rows, tx_ids); tx_ids is empty for static table packages.
    """
    changes = candidate_dir / "changes.jsonl"
    snapshot = candidate_dir / "snapshot"
    if not (changes.exists() and snapshot.is_dir()):
        return _read_tables(candidate_dir, pk_map), []

    # CDC package: apply the candidate log to the snapshot tables.
    state: dict[str, dict[str, dict]] = {}
    pks: dict[str, list[str]] = {}
    for path in sorted(snapshot.glob("*.jsonl")):
        table = path.stem
        pk = pk_map.get(table, [])
        pks[table] = pk
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                state.setdefault(table, {})[_pk_key(row, pk)] = row
    tx_ids: list = []
    with open(changes, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            tx = json.loads(line)
            tx_ids.append(tx["tx_id"])
            for ev in tx["events"]:
                table = ev["table"]
                pk = pks.get(table) or pk_map.get(table, [])
                key = _pk_key(ev["key"], pk)
                if ev["op"] == "delete":
                    state.get(table, {}).pop(key, None)
                else:
                    state.setdefault(table, {})[key] = ev["after"]
    rows = [
        (table, row, pks.get(table, []))
        for table, by_key in sorted(state.items())
        for row in by_key.values()
    ]
    return rows, tx_ids


def _spawn_sleepers(n: int) -> None:
    """Spawn long-lived children to verify whole-group kill on timeout.

    Each child execs an identifiable process name (argv0 ddreduce_mock_sleeper)
    so tests can prove the whole process group was reaped. The child closes
    its inherited standard streams so a pipe-reading parent (communicate())
    is not kept alive by the sleeper's duplicated descriptors.
    """
    for _ in range(n):
        pid = os.fork()
        if pid == 0:
            try:
                devnull = os.open(os.devnull, os.O_RDWR)
                os.dup2(devnull, 0)
                os.dup2(devnull, 1)
                os.dup2(devnull, 2)
                if devnull > 2:
                    os.close(devnull)
                os.execvp("sleep", ["ddreduce_mock_sleeper", "3600"])
            except Exception:
                os._exit(0)


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print("usage: mock_oracle.py <mock-config> <candidate-dir> <report-file>",
              file=sys.stderr)
        return 2
    with open(argv[1], "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    candidate_dir = Path(argv[2])
    report_path = Path(argv[3])
    pk_map: dict[str, list[str]] = {}
    if cfg.get("schema_path"):
        with open(cfg["schema_path"], "r", encoding="utf-8") as fh:
            schema_json = json.load(fh)
        for table_def in schema_json["tables"]:
            pk_map[table_def["name"]] = list(table_def["primary_key"])
    rows_raw, tx_ids = _read_package(candidate_dir, pk_map)
    rows = [(t, r) for t, r, _pk in rows_raw]
    present = {_row_id(t, r, pk) for t, r, pk in rows_raw}

    def _bare(rid: str) -> str:
        table, _, tail = rid.partition("/")
        # Single-column PKs in mock fixtures: one typed segment like
        # 'str:"c2"' or 'int:3'. Strip only its type tag and JSON quotes.
        raw = tail.split("/")[-1]
        _, _, value = raw.partition(":")
        if value.startswith('"'):
            value = json.loads(value)
        return f"{table}/{value}"

    # Fixtures may declare bare ids like "C/b"; match either the full typed
    # row id or its table/value shorthand.
    present |= {_bare(rid) for rid in list(present)}

    # Predicate-based triggers (composite-key / cycle fixtures).
    pred_timeout = _predicate_hit(rows, cfg.get("timeout_predicates", []))
    pred_crash = _predicate_hit(rows, cfg.get("crash_predicates", []))
    pred_fail = _predicate_hit(rows, cfg.get("fail_predicates", []))

    # Timeout: hang without writing a report. Two optional shape guards:
    #   timeout_requires_absent: hang only once every listed sentinel is gone;
    #   timeout_when_rows_below:  hang only once the package is this small
    # (so the full input confirms cleanly while a small neighbor goes unknown).
    # For CDC packages, timeout_when_tx_below does the same on transaction count.
    guard = cfg.get("timeout_requires_absent", [])
    sentinel_still_present = any(g in present for g in guard)
    below = cfg.get("timeout_when_rows_below")
    shape_allows = (below is None) or (len(rows) < int(below))
    tx_below = cfg.get("timeout_when_tx_below")
    if tx_below is not None:
        shape_allows = shape_allows and (len(tx_ids) < int(tx_below))
    timeout_trigger = (
        bool(present.intersection(cfg.get("timeout_rows", [])))
        or bool(pred_timeout)
        or bool(cfg.get("timeout_always"))
    )
    if timeout_trigger and not sentinel_still_present and shape_allows:
        # Spawn the long-lived children only on the actual hang path, so a
        # successful invocation never orphans descriptors or processes.
        if cfg.get("spawn_children"):
            _spawn_sleepers(int(cfg["spawn_children"]))
        while True:
            time.sleep(3600)

    # Crash: non-zero exit and no report.
    if present.intersection(cfg.get("crash_rows", [])) or pred_crash:
        print("mock crash", file=sys.stderr)
        return 3

    # Flaky counters are persisted per (config, candidate) so repeated runs
    # of the same candidate see the sequence defined in the config.
    state_file = cfg.get("state_file")
    state: dict[str, int] = {}
    if state_file and os.path.exists(state_file):
        with open(state_file, "r", encoding="utf-8") as fh:
            state = json.load(fh)

    def flaky_pass(row_id: str) -> bool:
        spec = (cfg.get("flaky") or {}).get(row_id)
        if not spec:
            return False
        key = f"{row_id}@{candidate_dir.name}"
        count = state.get(key, 0)
        state[key] = count + 1
        if state_file:
            with open(state_file, "w", encoding="utf-8") as fh:
                json.dump(state, fh)
        return count < int(spec.get("pass_first", 1))

    report: dict = {"status": "PASS"}
    summary_key = cfg.get("summary_field")
    if summary_key:
        report[summary_key] = state.get("__run__", 0)
        state["__run__"] = state.get("__run__", 0) + 1
        if state_file:
            with open(state_file, "w", encoding="utf-8") as fh:
                json.dump(state, fh)

    fail_rows = set(cfg.get("fail_rows", []))
    hit = sorted(present.intersection(fail_rows))
    if pred_fail:
        hit = sorted(set(hit) | set(pred_fail))
    if cfg.get("fail_always"):
        hit = sorted(set(hit) | {"__always__"})
    # CDC transaction-set triggers: fail when every tx_id of a set is present.
    tx_present = set(tx_ids)
    for tx_set in cfg.get("fail_tx_sets", []):
        if tx_set and all(t in tx_present for t in tx_set):
            hit = sorted(set(hit) | {f"tx:{t}" for t in tx_set})
            break
    if hit and not any(flaky_pass(r) for r in hit):
        # Extra per-row signature overrides (for "same target, other signature"
        # and "two alternative fault groups" scenarios).
        extras = cfg.get("extra_signatures", {})
        sig = cfg.get("fail_signature", "SIG-MOCK")
        for r in hit:
            if r in extras:
                sig = extras[r]
                break
        # Signature drift under shrinking: the SAME fault reports a different
        # signature once the candidate drops below a row-count threshold.
        shrink = cfg.get("shrink_signature")
        if shrink and len(rows) < int(shrink["row_count_below"]):
            sig = shrink["signature"]
        report = {
            "status": "FAIL",
            "signature": sig,
            "trigger_rows": hit,
        }
        if summary_key:
            report[summary_key] = state.get("__run__", 0)

    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, sort_keys=True)
        fh.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
