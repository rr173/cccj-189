"""Command line interface: reduce, resume, verify, log-replay, log-reduce,
log-verify, capture-import, capture-verify.

Exit codes:
  0  CERTIFIED (reduce/resume/log-reduce) or VERIFIED_MINIMAL (verify/log-verify);
     log-replay: the log is valid and the replay report was produced;
     capture-import: the package was built; capture-verify: package matched
  2  baseline rejected / certificate REJECTED / non-minimal or unknown neighbor /
     invalid change log / invalid or conflicting capture archive / restore
     failure / capture package mismatch
  3  call budget exhausted
  4  interrupted
  1  any other tool error (bad arguments, invalid schema/data, ...)
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
from pathlib import Path

from .engine import BaselineRejected, BudgetExhausted, Reducer, SearchInterrupted
from .logengine import LogReducer
from .logmodel import ChangeLog, LogContractError, load_changelog
from .logverify import LogVerifier, write_log_report
from .model import Database
from .oracle import OracleConfig, OracleConfigError
from .replay import validate_log_inputs
from .schema import Schema, load_schema
from .state import SearchState
from .values import DataError, SchemaError
from .verify import Verifier, write_report
from .capture import (
    CaptureError,
    run_import,
    run_verify,
)
from .capture.verifier import (
    VERDICT_ARCHIVE,
    VERDICT_MATCH,
    VERDICT_PACKAGE,
    VERDICT_RESTORE,
)


def _load_pieces(args: argparse.Namespace) -> tuple[Schema, Database, OracleConfig]:
    schema = load_schema(args.schema)
    overrides = {
        k: v
        for k, v in {
            "timeout_seconds": args.timeout,
            "retest_runs": args.retest_runs,
            "confirm_runs": args.confirm_runs,
            "max_calls": args.max_calls,
        }.items()
        if v is not None
    }
    config = OracleConfig.load(args.oracle_config, overrides)
    db = Database.load(schema, args.input)
    return schema, db, config


def _oracle_overrides(args: argparse.Namespace) -> dict:
    return {
        k: v
        for k, v in {
            "timeout_seconds": getattr(args, "timeout", None),
            "retest_runs": getattr(args, "retest_runs", None),
            "confirm_runs": getattr(args, "confirm_runs", None),
            "max_calls": getattr(args, "max_calls", None),
        }.items()
        if v is not None
    }


def _load_validated_log(args: argparse.Namespace):
    """Load schema/snapshot/log and validate the original log (no oracle yet)."""
    schema = load_schema(args.schema)
    schema_text = Path(args.schema).read_text(encoding="utf-8")
    snapshot = Database.load(schema, args.snapshot)
    log = load_changelog(args.changes, schema)
    return validate_log_inputs(schema, schema_text, snapshot, log)


def _install_signal_handlers(stop_event: threading.Event) -> None:
    def _handler(signum, frame):  # pragma: no cover - exercised via subprocess tests
        stop_event.set()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def cmd_reduce(args: argparse.Namespace) -> int:
    stop_event = threading.Event()
    _install_signal_handlers(stop_event)
    try:
        schema, db, config = _load_pieces(args)
        reducer = Reducer(
            schema=schema,
            original=db,
            config=config,
            work_dir=args.workdir,
            input_dir=args.input,
            schema_path=args.schema,
            config_path=args.oracle_config,
            stop_event=stop_event,
        )
        state = reducer.run()
    except (OracleConfigError, SchemaError, DataError, BaselineRejected) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except BudgetExhausted as exc:
        print(f"BUDGET EXHAUSTED: {exc}", file=sys.stderr)
        return 3
    except SearchInterrupted:
        print("INTERRUPTED: state saved; use 'resume' to continue", file=sys.stderr)
        return 4

    if state.status == "CERTIFIED":
        print(f"CERTIFIED 1-minimal package: {state.certificate_path}")
        print(f"rows kept: {len(state.result_ids)}; calls used: {state.calls_used}")
        return 0
    if state.status == "BUDGET_EXHAUSTED":
        print("BUDGET EXHAUSTED", file=sys.stderr)
        return 3
    if state.status == "INTERRUPTED":
        print("INTERRUPTED", file=sys.stderr)
        return 4
    print(f"NOT CERTIFIED: {state.status} {state.note}", file=sys.stderr)
    return 2


def cmd_resume(args: argparse.Namespace) -> int:
    stop_event = threading.Event()
    _install_signal_handlers(stop_event)
    try:
        store_path = Path(args.workdir) / "state.json"
        if not store_path.exists():
            raise DataError(f"no search state found in {args.workdir}")
        with open(store_path, "r", encoding="utf-8") as fh:
            state_raw = json.load(fh)
        mode = state_raw.get("mode", "tables")
        if mode == "log":
            return _resume_log(args, state_raw, stop_event)
        schema = load_schema(state_raw["schema_path"])
        config = OracleConfig.load(state_raw["oracle_config_path"])
        db = Database.load(schema, state_raw["input_dir"])
        reducer = Reducer(
            schema=schema,
            original=db,
            config=config,
            work_dir=args.workdir,
            input_dir=state_raw["input_dir"],
            schema_path=state_raw["schema_path"],
            config_path=state_raw["oracle_config_path"],
            stop_event=stop_event,
            budget_override=args.max_calls,
        )
        state = reducer.run()
    except (OracleConfigError, SchemaError, DataError, LogContractError, BaselineRejected) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except BudgetExhausted:
        print("BUDGET EXHAUSTED", file=sys.stderr)
        return 3
    except SearchInterrupted:
        print("INTERRUPTED: state saved; run resume again", file=sys.stderr)
        return 4

    if state.status == "CERTIFIED":
        print(f"RESUMED and CERTIFIED: {state.certificate_path}")
        print(f"rows kept: {len(state.result_ids)}; calls used: {state.calls_used}")
        return 0
    if state.status == "BUDGET_EXHAUSTED":
        print("BUDGET EXHAUSTED", file=sys.stderr)
        return 3
    if state.status == "INTERRUPTED":
        print("INTERRUPTED", file=sys.stderr)
        return 4
    print(f"NOT CERTIFIED: {state.status} {state.note}", file=sys.stderr)
    return 2


def _resume_log(
    args: argparse.Namespace, state_raw: dict, stop_event: threading.Event
) -> int:
    """Resume a log-mode search; the state file records every input path."""
    schema = load_schema(state_raw["schema_path"])
    schema_text = Path(state_raw["schema_path"]).read_text(encoding="utf-8")
    snapshot = Database.load(schema, state_raw["input_dir"])
    log = load_changelog(state_raw["changes_path"], schema)
    validated = validate_log_inputs(schema, schema_text, snapshot, log)
    config = OracleConfig.load(state_raw["oracle_config_path"])
    reducer = LogReducer(
        validated=validated,
        config=config,
        work_dir=args.workdir,
        snapshot_dir=state_raw["input_dir"],
        changes_path=state_raw["changes_path"],
        schema_path=state_raw["schema_path"],
        config_path=state_raw["oracle_config_path"],
        stop_event=stop_event,
        budget_override=args.max_calls,
    )
    state = reducer.run()
    if state.status == "CERTIFIED":
        print(f"RESUMED and CERTIFIED: {state.certificate_path}")
        print(f"transactions kept: {len(state.result_tx_ids)}; calls used: {state.calls_used}")
        return 0
    if state.status == "BUDGET_EXHAUSTED":
        print("BUDGET EXHAUSTED", file=sys.stderr)
        return 3
    if state.status == "BLOCKED":
        print(f"BLOCKED: {state.note}", file=sys.stderr)
        return 2
    if state.status == "INTERRUPTED":
        print("INTERRUPTED", file=sys.stderr)
        return 4
    print(f"NOT CERTIFIED: {state.status} {state.note}", file=sys.stderr)
    return 2


def cmd_verify(args: argparse.Namespace) -> int:
    try:
        cert_path = Path(args.workdir) / "certificate.json"
        if cert_path.exists():
            with open(cert_path, "r", encoding="utf-8") as fh:
                mode = json.load(fh).get("mode", "tables")
            if mode == "log":
                raise DataError(
                    "workdir holds a CDC log certificate; use 'log-verify' instead"
                )
        schema = load_schema(args.schema)
        config = OracleConfig.load(args.oracle_config)
        if args.max_calls is not None:
            config = OracleConfig.load(
                args.oracle_config, {"max_calls": args.max_calls}
            )
        target = args.signature
        result_dir = args.result_dir
        if target is None or result_dir is None:
            if not cert_path.exists():
                raise DataError(
                    "certificate.json not found; pass --signature and --result-dir explicitly"
                )
            with open(cert_path, "r", encoding="utf-8") as fh:
                cert = json.load(fh)
            target = target or cert["target_signature"]
            result_dir = result_dir or cert["result"]["directory"]
        verifier = Verifier(
            schema=schema,
            config=config,
            work_dir=Path(args.workdir) / "verify-runs",
            use_cache=not args.no_cache,
        )
        report = verifier.verify_package(result_dir, target, config.max_calls)
        out_path = Path(args.workdir) / "verify-report.json"
        write_report(report, out_path)
    except (OracleConfigError, SchemaError, DataError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print(f"{report.verdict}: {out_path}")
    print(
        f"neighbors tested: {len(report.neighbors)}; "
        f"calls: {report.calls_used}; cache hits: {report.cache_hits}"
    )
    if report.verdict == "VERIFIED_MINIMAL":
        return 0
    if report.verdict == "BUDGET_EXHAUSTED":
        return 3
    return 2


# =================================================================== log mode
def cmd_log_reduce(args: argparse.Namespace) -> int:
    stop_event = threading.Event()
    _install_signal_handlers(stop_event)
    try:
        validated = _load_validated_log(args)
        config = OracleConfig.load(args.oracle_config, _oracle_overrides(args))
        reducer = LogReducer(
            validated=validated,
            config=config,
            work_dir=args.workdir,
            snapshot_dir=args.snapshot,
            changes_path=args.changes,
            schema_path=args.schema,
            config_path=args.oracle_config,
            stop_event=stop_event,
        )
        state = reducer.run()
    except (OracleConfigError, SchemaError, DataError, LogContractError, BaselineRejected) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except BudgetExhausted as exc:
        print(f"BUDGET EXHAUSTED: {exc}", file=sys.stderr)
        return 3
    except SearchInterrupted:
        print("INTERRUPTED: state saved; use 'resume' to continue", file=sys.stderr)
        return 4

    if state.status == "CERTIFIED":
        print(f"CERTIFIED 1-minimal CDC package: {state.certificate_path}")
        print(
            f"transactions kept: {len(state.result_tx_ids)} "
            f"({state.result_tx_ids}); calls used: {state.calls_used}"
        )
        return 0
    if state.status == "BUDGET_EXHAUSTED":
        print("BUDGET EXHAUSTED", file=sys.stderr)
        return 3
    if state.status == "INTERRUPTED":
        print("INTERRUPTED", file=sys.stderr)
        return 4
    print(f"NOT CERTIFIED: {state.status} {state.note}", file=sys.stderr)
    return 2


def cmd_log_replay(args: argparse.Namespace) -> int:
    """Replay (and optionally normalize) a log without running any oracle."""
    try:
        validated = _load_validated_log(args)
        if args.selection:
            with open(args.selection, "r", encoding="utf-8") as fh:
                sel_doc = json.load(fh)
            selection = _parse_selection(sel_doc, validated.log)
        else:
            selection = {tx.position for tx in validated.log}
        normalized = validated.normalize(selection)
    except (SchemaError, DataError, LogContractError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        print(f"ERROR: invalid selection file: {exc}", file=sys.stderr)
        return 1

    candidate = validated.candidate(normalized.committed)
    report = {
        "valid": True,
        "tx_count": len(validated.log),
        "selection": sorted(selection),
        "committed_positions": list(normalized.committed),
        "committed_tx_ids": candidate.tx_ids(),
        "transactions": [d.to_dict() for d in normalized.dispositions],
        "final": {
            "table_counts": normalized.final_database().table_counts(),
            "state_hash": normalized.state_hash(),
        },
        "candidate_hash": candidate.content_hash(),
    }
    if args.out:
        candidate.materialize(args.out)
        report["candidate_dir"] = str(args.out)
    text = json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2)
    if args.report:
        Path(args.report).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


def _parse_selection(sel_doc: dict, log: ChangeLog) -> set[int]:
    if not isinstance(sel_doc, dict):
        raise ValueError("selection must be a JSON object")
    by_id = {json.dumps(tx.tx_id): tx.position for tx in log}

    def _positions(ids):
        out = set()
        for tx_id in ids:
            token = json.dumps(tx_id)
            if token not in by_id:
                raise ValueError(f"unknown tx_id in selection: {tx_id!r}")
            out.add(by_id[token])
        return out

    if "keep" in sel_doc:
        return _positions(sel_doc["keep"])
    if "drop" in sel_doc:
        return {tx.position for tx in log} - _positions(sel_doc["drop"])
    raise ValueError("selection file needs a 'keep' or 'drop' list of tx_ids")


def cmd_log_verify(args: argparse.Namespace) -> int:
    try:
        config = OracleConfig.load(args.oracle_config)
        if args.max_calls is not None:
            config = OracleConfig.load(args.oracle_config, {"max_calls": args.max_calls})
        verifier = LogVerifier(
            config=config,
            work_dir=Path(args.workdir) / "verify-runs",
            use_cache=not args.no_cache,
        )
        report = verifier.verify_certificate(args.workdir, config.max_calls)
        out_path = Path(args.workdir) / "log-verify-report.json"
        write_log_report(report, out_path)
    except (OracleConfigError, SchemaError, DataError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print(f"{report.verdict}: {out_path}")
    print(
        f"neighbors tested: {len(report.neighbors)}; "
        f"calls: {report.calls_used}; cache hits: {report.cache_hits}"
    )
    if report.verdict == "VERIFIED_MINIMAL":
        return 0
    if report.verdict == "BUDGET_EXHAUSTED":
        return 3
    return 2


# ============================================================== capture mode
def cmd_capture_import(args: argparse.Namespace) -> int:
    """Restore a capture-v1 archive into a full-image log package (no oracle)."""
    try:
        result = run_import(
            archive=args.archive,
            snapshot_dir=args.snapshot,
            schema_path=args.schema,
            output_dir=args.out,
            through=args.through,
            tmp_root=args.tmpdir,
        )
    except CaptureError as exc:
        print(f"CAPTURE IMPORT FAILED: {exc}", file=sys.stderr)
        return 2
    counts = result.report["counts"]
    print(f"CAPTURE PACKAGE: {result.output_dir}")
    print(
        "physical messages: {physical_messages_read} "
        "(duplicates {physical_duplicates}); heartbeats: {heartbeats}; "
        "transactions: {transactions_total} (committed {committed_total}, "
        "aborted {aborted_total}); exported tx: {exported_transactions}, "
        "exported events: {exported_events}; peak spill bytes: "
        "{peak}".format(
            peak=result.peak_spill_bytes, **counts,
        )
    )
    if args.report:
        Path(args.report).write_text(
            json.dumps(result.report, ensure_ascii=False, sort_keys=True, indent=2)
            + "\n",
            encoding="utf-8",
        )
        print(f"report: {args.report}")
    return 0


def cmd_capture_verify(args: argparse.Namespace) -> int:
    """Independently re-restore an archive and compare a package (no oracle)."""
    try:
        result = run_verify(
            archive=args.archive,
            snapshot_dir=args.snapshot,
            schema_path=args.schema,
            through=args.through,
            package_dir=args.package,
            tmp_root=args.tmpdir,
        )
    except CaptureError as exc:
        print(f"CAPTURE VERIFY FAILED: {exc}", file=sys.stderr)
        return 2
    text = json.dumps(result.report, ensure_ascii=False, sort_keys=True, indent=2)
    if args.report:
        Path(args.report).write_text(text + "\n", encoding="utf-8")
    print(text)
    if result.verdict == VERDICT_MATCH:
        print(f"{VERDICT_MATCH}: {args.package}", file=sys.stderr)
        return 0
    print(
        f"{result.verdict}: {len(result.failures)} discrepancy(ies); details above",
        file=sys.stderr,
    )
    return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ddreduce",
        description="Failure-input reducer for relational JSONL data and CDC change logs",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--input", required=True, help="directory of JSONL tables")
    common.add_argument("--schema", required=True, help="schema JSON file")
    common.add_argument("--oracle-config", required=True, help="oracle config JSON")
    common.add_argument("--workdir", required=True, help="tool state / cache / result dir")
    common.add_argument("--timeout", type=float, default=None)
    common.add_argument("--retest-runs", type=int, default=None)
    common.add_argument("--confirm-runs", type=int, default=None)
    common.add_argument("--max-calls", type=int, default=None)

    p_reduce = sub.add_parser("reduce", parents=[common], help="start (or continue) a reduction")
    p_reduce.set_defaults(func=cmd_reduce)

    p_resume = sub.add_parser(
        "resume", help="resume an interrupted search from its workdir (auto-detects mode)"
    )
    p_resume.add_argument("--workdir", required=True)
    p_resume.add_argument(
        "--max-calls",
        type=int,
        default=None,
        help="new total call budget (must be >= calls already used)",
    )
    p_resume.set_defaults(func=cmd_resume)

    p_verify = sub.add_parser("verify", parents=[common], help="independently verify a certificate")
    p_verify.add_argument("--signature", default=None)
    p_verify.add_argument("--result-dir", default=None)
    p_verify.add_argument("--no-cache", action="store_true")
    p_verify.set_defaults(func=cmd_verify)

    log_common = argparse.ArgumentParser(add_help=False)
    log_common.add_argument("--snapshot", required=True, help="fixed snapshot dir of JSONL tables")
    log_common.add_argument("--schema", required=True, help="schema JSON file")
    log_common.add_argument("--changes", required=True, help="original changes.jsonl")

    p_log_reduce = sub.add_parser(
        "log-reduce", parents=[log_common], help="reduce a CDC transaction history"
    )
    p_log_reduce.add_argument("--oracle-config", required=True, help="oracle config JSON")
    p_log_reduce.add_argument("--workdir", required=True, help="tool state / cache / result dir")
    p_log_reduce.add_argument("--timeout", type=float, default=None)
    p_log_reduce.add_argument("--retest-runs", type=int, default=None)
    p_log_reduce.add_argument("--confirm-runs", type=int, default=None)
    p_log_reduce.add_argument("--max-calls", type=int, default=None)
    p_log_reduce.set_defaults(func=cmd_log_reduce)

    p_log_replay = sub.add_parser(
        "log-replay",
        parents=[log_common],
        help="replay/normalize a change log without running the oracle",
    )
    p_log_replay.add_argument(
        "--selection",
        default=None,
        help="JSON file with {'keep': [tx_id,...]} or {'drop': [tx_id,...]}",
    )
    p_log_replay.add_argument("--out", default=None, help="write the normalized candidate package here")
    p_log_replay.add_argument("--report", default=None, help="also write the replay report JSON here")
    p_log_replay.set_defaults(func=cmd_log_replay)

    p_log_verify = sub.add_parser(
        "log-verify", help="independently verify a CDC history certificate"
    )
    p_log_verify.add_argument("--workdir", required=True)
    p_log_verify.add_argument("--oracle-config", required=True, help="oracle config JSON")
    p_log_verify.add_argument("--max-calls", type=int, default=None)
    p_log_verify.add_argument("--no-cache", action="store_true")
    p_log_verify.set_defaults(func=cmd_log_verify)

    # ------------------------------------------------------------- capture v1
    capture_common = argparse.ArgumentParser(add_help=False)
    capture_common.add_argument(
        "--archive", required=True, help="capture-v1 archive directory (with capture.json)"
    )
    capture_common.add_argument(
        "--snapshot", required=True, help="starting snapshot directory of JSONL tables"
    )
    capture_common.add_argument("--schema", required=True, help="target schema JSON file")
    capture_common.add_argument(
        "--through", required=True, type=int,
        help="export commits with snapshot_commit_seq < commit_seq <= THROUGH",
    )
    capture_common.add_argument(
        "--tmpdir", default=None,
        help="directory for spill working files (defaults to the system temp dir)",
    )

    p_capture_import = sub.add_parser(
        "capture-import",
        parents=[capture_common],
        help="restore a capture-v1 archive into a full-image changes package",
    )
    p_capture_import.add_argument(
        "--out", required=True,
        help="new output directory (must not exist; never overwritten)",
    )
    p_capture_import.add_argument(
        "--report", default=None, help="also copy the capture report JSON here"
    )
    p_capture_import.set_defaults(func=cmd_capture_import)

    p_capture_verify = sub.add_parser(
        "capture-verify",
        parents=[capture_common],
        help="independently re-restore an archive and verify a captured package",
    )
    p_capture_verify.add_argument(
        "--package", required=True, help="package directory produced by capture-import"
    )
    p_capture_verify.add_argument(
        "--report", default=None, help="write the verification report JSON here"
    )
    p_capture_verify.set_defaults(func=cmd_capture_verify)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
