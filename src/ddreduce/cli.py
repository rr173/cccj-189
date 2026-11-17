"""Command line interface: reduce, resume, verify.

Exit codes:
  0  CERTIFIED (reduce/resume) or VERIFIED_MINIMAL (verify)
  2  baseline rejected / certificate REJECTED / non-minimal neighbor found
  3  budget exhausted
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
from .oracle import OracleConfig, OracleConfigError
from .schema import Schema, load_schema
from .model import Database
from .values import DataError, SchemaError
from .verify import Verifier, write_report


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
    except (OracleConfigError, SchemaError, DataError, BaselineRejected) as exc:
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


def cmd_verify(args: argparse.Namespace) -> int:
    try:
        schema = load_schema(args.schema)
        config = OracleConfig.load(args.oracle_config)
        if args.max_calls is not None:
            config = OracleConfig.load(
                args.oracle_config, {"max_calls": args.max_calls}
            )
        target = args.signature
        result_dir = args.result_dir
        if target is None or result_dir is None:
            cert_path = Path(args.workdir) / "certificate.json"
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ddreduce",
        description="Failure-input reducer for relational JSONL data",
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
        "resume", help="resume an interrupted search from its workdir"
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
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
