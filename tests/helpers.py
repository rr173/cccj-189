"""Shared helpers for the ddreduce test suite."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
MOCK = Path(__file__).resolve().parent / "mock_oracle.py"


def write_json(path: Path, obj) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=2)
    return path


def write_jsonl(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            fh.write("\n")
    return path


def make_oracle_config(
    dest: Path,
    mock_config: Path,
    *,
    timeout: float = 10.0,
    retest_runs: int = 1,
    confirm_runs: int = 2,
    max_calls: int = 200,
) -> Path:
    cfg = {
        "command": [
            sys.executable,
            "-I",
            str(MOCK),
            str(mock_config),
            "{candidate_dir}",
            "{report_file}",
        ],
        "timeout_seconds": timeout,
        "retest_runs": retest_runs,
        "confirm_runs": confirm_runs,
        "max_calls": max_calls,
    }
    return write_json(dest, cfg)


def make_fixture(
    base: Path,
    schema: dict,
    tables: dict[str, list[dict]],
    mock: dict,
    *,
    timeout: float = 10.0,
    retest_runs: int = 1,
    confirm_runs: int = 2,
    max_calls: int = 200,
) -> tuple[Path, Path, Path]:
    """Create schema/tables/mock/oracle config under ``base``; return key paths."""
    schema_path = write_json(base / "schema.json", schema)
    tables_dir = base / "tables"
    for name, rows in tables.items():
        write_jsonl(tables_dir / f"{name}.jsonl", rows)
    mock = dict(mock)
    # The external mock oracle reads PK columns from the same schema file.
    mock["schema_path"] = str(schema_path)
    mock_path = write_json(base / "mock" / "mock.json", mock)
    oracle_path = make_oracle_config(
        base / "oracle.json",
        mock_path,
        timeout=timeout,
        retest_runs=retest_runs,
        confirm_runs=confirm_runs,
        max_calls=max_calls,
    )
    return schema_path, tables_dir, oracle_path


def run_cli(*args: str) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(SRC) + (os.pathsep + pythonpath if pythonpath else "")
    # -P keeps the interpreter from prepending the script/cwd directory to
    # sys.path (no planted json.py pickup), while still honoring PYTHONPATH so
    # the isolated test invocation can find the ddreduce package.  Full -I
    # would ignore PYTHONPATH entirely.
    return subprocess.run(
        [sys.executable, "-P", "-m", "ddreduce.cli", *args],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(ROOT),
    )


class TempDir:
    def __init__(self):
        self.path = Path(tempfile.mkdtemp(prefix="ddreduce-test-"))

    def cleanup(self):
        shutil.rmtree(self.path, ignore_errors=True)


# ------------------------------------------------------------------ CDC logs
def tx(tx_id, *events) -> dict:
    return {"tx_id": tx_id, "events": list(events)}


def ins(table, key, after) -> dict:
    return {"op": "insert", "table": table, "key": key, "after": after}


def upd(table, key, before, after) -> dict:
    return {"op": "update", "table": table, "key": key, "before": before, "after": after}


def dele(table, key, before) -> dict:
    return {"op": "delete", "table": table, "key": key, "before": before}


def make_log_fixture(
    base: Path,
    schema: dict,
    snapshot_tables: dict[str, list[dict]],
    changes: list[dict],
    mock: dict | None = None,
    *,
    timeout: float = 10.0,
    retest_runs: int = 1,
    confirm_runs: int = 2,
    max_calls: int = 200,
):
    """Create schema/snapshot/changes (+ optional mock oracle) under ``base``."""
    schema_path = write_json(base / "schema.json", schema)
    snapshot_dir = base / "snapshot"
    for name, rows in snapshot_tables.items():
        write_jsonl(snapshot_dir / f"{name}.jsonl", rows)
    changes_path = write_jsonl(base / "changes.jsonl", changes)
    oracle_path = None
    if mock is not None:
        mock = dict(mock)
        mock["schema_path"] = str(schema_path)
        mock_path = write_json(base / "mock" / "mock.json", mock)
        oracle_path = make_oracle_config(
            base / "oracle.json",
            mock_path,
            timeout=timeout,
            retest_runs=retest_runs,
            confirm_runs=confirm_runs,
            max_calls=max_calls,
        )
    return schema_path, snapshot_dir, changes_path, oracle_path
