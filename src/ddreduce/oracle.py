"""Controlled subprocess oracle.

Every oracle invocation gets:
  * a freshly created, unique candidate directory (never reused while a
    process may still be reading it),
  * its own process group / session so a timed-out process and *all* of its
    children can be killed together,
  * captured stdout/stderr (bounded) and an explicit exit status,
  * a verdict derived strictly from the report contract:

        PASS                         -> outcome PASS
        FAIL with signature          -> outcome FAIL (signature recorded)
        missing/malformed report     -> UNRESOLVED
        non-zero exit / timeout      -> UNRESOLVED

A timeout is never translated into FAIL: UNRESOLVED neither proves the fault
is retained nor that it disappeared.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .model import Database
from .values import canonical_json

REPORT_FILE = "report.json"
# Bounds on captured output so a chatty command cannot exhaust memory/disk.
MAX_OUTPUT_BYTES = 8 * 1024 * 1024
MAX_REPORT_BYTES = 1 * 1024 * 1024


class OracleConfigError(ValueError):
    pass


@dataclass(frozen=True)
class OracleConfig:
    command: list[str]  # argv with {candidate_dir} / {report_file} placeholders
    timeout_seconds: float
    retest_runs: int  # runs per candidate once the target is established
    confirm_runs: int  # repeated runs on the original input
    max_calls: int  # total subprocess budget (cache hits are free)
    report_file: str = REPORT_FILE

    @classmethod
    def load(cls, path: str | Path, overrides: dict[str, Any] | None = None) -> "OracleConfig":
        path = Path(path)
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        if overrides:
            raw = {**raw, **overrides}
        command = raw.get("command")
        if isinstance(command, str):
            command = [command]
        if not isinstance(command, list) or not command or not all(
            isinstance(p, str) and p for p in command
        ):
            raise OracleConfigError("'command' must be a non-empty string or argv list")
        # The oracle runs with its cwd set to the candidate directory, so any
        # relative script path is resolved once against the *launcher's* cwd
        # (where the CLI was started); absolute paths and bare executables
        # found on PATH are left untouched.
        resolved = []
        for idx, part in enumerate(command):
            candidate = Path(part)
            if (
                idx == 0
                or candidate.is_absolute()
                or "/" not in part
                or "{candidate_dir}" in part
                or "{report_file}" in part
            ):
                resolved.append(part)
            else:
                resolved.append(str(candidate.resolve()))
        command = resolved
        try:
            timeout = float(raw.get("timeout_seconds", 30.0))
            retest = int(raw.get("retest_runs", 1))
            confirm = int(raw.get("confirm_runs", 2))
            max_calls = int(raw.get("max_calls", 100))
        except (TypeError, ValueError) as exc:
            raise OracleConfigError(f"invalid numeric oracle setting: {exc}") from exc
        if timeout <= 0 or retest < 1 or confirm < 2 or max_calls < 1:
            raise OracleConfigError(
                "timeout_seconds>0, retest_runs>=1, confirm_runs>=2, max_calls>=1 required"
            )
        report_file = str(raw.get("report_file", REPORT_FILE))
        if not report_file or os.path.isabs(report_file) or ".." in Path(report_file).parts:
            raise OracleConfigError("report_file must be a relative safe path")
        return cls(
            command=list(command),
            timeout_seconds=timeout,
            retest_runs=retest,
            confirm_runs=confirm,
            max_calls=max_calls,
            report_file=report_file,
        )

    def canonical(self) -> dict:
        return {
            "command": self.command,
            "timeout_seconds": self.timeout_seconds,
            "retest_runs": self.retest_runs,
            "confirm_runs": self.confirm_runs,
            "max_calls": self.max_calls,
            "report_file": self.report_file,
        }


@dataclass(frozen=True)
class RunSignal:
    """The raw result of one subprocess invocation."""

    exit_code: int | None  # None when killed by timeout
    timed_out: bool
    report_status: str | None  # "PASS" / "FAIL" / None
    signature: str | None
    report_error: str | None
    stdout_tail: str
    stderr_tail: str
    duration_seconds: float

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class OracleOutcome:
    """Aggregated verdict over one or more runs of the same candidate."""

    verdict: str  # PASS / FAIL / UNRESOLVED
    signature: str | None
    runs: list[RunSignal] = field(default_factory=list)
    note: str = ""

    def to_dict(self) -> dict:
        return {
            "verdict": self.verdict,
            "signature": self.signature,
            "note": self.note,
            "runs": [r.to_dict() for r in self.runs],
        }


def expand_argv(template: list[str], candidate_dir: Path, report_path: Path) -> list[str]:
    args = []
    for part in template:
        args.append(
            part.replace("{candidate_dir}", str(candidate_dir)).replace(
                "{report_file}", str(report_path)
            )
        )
    return args


class OracleRunner:
    """Materializes candidates into unique dirs and runs the command in a pgroup."""

    def __init__(self, work_dir: Path, config: OracleConfig):
        self.work_dir = Path(work_dir)
        self.config = config
        self.candidates_root = self.work_dir / "candidates"
        self.candidates_root.mkdir(parents=True, exist_ok=True)
        self._cleanup_stale_candidates()
        self._seq = 0
        self._live_dirs: set[Path] = set()

    def _cleanup_stale_candidates(self) -> None:
        # Candidates left behind by an interrupted process are safe to remove:
        # this constructor only runs when no oracle of *this* process is live.
        if self.candidates_root.exists():
            for child in self.candidates_root.iterdir():
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)

    def materialize(self, db: Database) -> Path:
        """Create a brand-new candidate directory; never reused while live."""
        self._seq += 1
        stamp = f"c{self._seq:06d}_{db.content_hash()[:12]}"
        candidate_dir = self.candidates_root / stamp
        # Extremely unlikely hash+seq collision; refuse rather than overwrite.
        if candidate_dir.exists():
            raise RuntimeError(f"candidate directory already exists: {candidate_dir}")
        db.materialize(candidate_dir)
        self._live_dirs.add(candidate_dir)
        return candidate_dir

    def run_once(self, db: Database) -> tuple[RunSignal, Path]:
        candidate_dir = self.materialize(db)
        report_path = candidate_dir / self.config.report_file
        argv = expand_argv(self.config.command, candidate_dir, report_path)
        start = time.monotonic()
        proc = subprocess.Popen(
            argv,
            cwd=str(candidate_dir),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,  # own process group -> whole-tree kill
            text=True,
        )
        timed_out = False
        try:
            stdout, stderr = proc.communicate(timeout=self.config.timeout_seconds)
            exit_code = proc.returncode
        except subprocess.TimeoutExpired:
            timed_out = True
            self._kill_tree(proc)
            # Drain the pipes after the group is gone. Guard against a second
            # timeout so a descriptor-holding grandchild can never strand us:
            # _kill_tree already SIGKILLed the whole process group.
            try:
                stdout, stderr = proc.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                try:
                    stdout, stderr = proc.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    stdout, stderr = "", ""
            exit_code = proc.returncode
        duration = time.monotonic() - start

        report_status, signature, report_error = self._read_report(
            report_path, exit_code, timed_out
        )
        signal_obj = RunSignal(
            exit_code=exit_code,
            timed_out=timed_out,
            report_status=report_status,
            signature=signature,
            report_error=report_error,
            stdout_tail=_tail(stdout),
            stderr_tail=_tail(stderr),
            duration_seconds=round(duration, 6),
        )
        # The process has exited and its fds are drained: the directory is no
        # longer being read and may be removed.
        self._live_dirs.discard(candidate_dir)
        shutil.rmtree(candidate_dir, ignore_errors=True)
        return signal_obj, candidate_dir

    @staticmethod
    def _kill_tree(proc: subprocess.Popen) -> None:
        """Kill the whole process group and reap it.

        A timeout means the command is unresponsive, so we go straight to
        SIGKILL on the entire group (the runner created it via
        ``start_new_session``). Killing by pgid rather than by parent pid is
        essential: once the parent dies, forked grandchildren are reparented
        to PID 1 but stay in the same process group and are still reachable.
        """
        try:
            pgid = os.getpgid(proc.pid)
        except ProcessLookupError:
            return  # already gone; nothing of ours to kill

        for sig in (signal.SIGKILL,):
            try:
                os.killpg(pgid, sig)
            except ProcessLookupError:
                return
        # Reap the direct child. The group members are either reaped by the
        # kernel (if they share our parent relationship) or reparented and
        # destroyed by the group-wide SIGKILL regardless.
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(pgid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass

    @staticmethod
    def _read_report(
        report_path: Path, exit_code: int | None, timed_out: bool
    ) -> tuple[str | None, str | None, str | None]:
        if timed_out:
            return None, None, "timeout before report"
        if exit_code not in (0,):
            return None, None, f"non-zero exit {exit_code}; report not trusted"
        if not report_path.exists():
            return None, None, "report file missing after clean exit"
        try:
            if report_path.stat().st_size > MAX_REPORT_BYTES:
                return None, None, "report exceeds size limit"
            with open(report_path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            return None, None, f"unreadable report: {exc}"
        if not isinstance(raw, dict) or "status" not in raw:
            return None, None, "report missing 'status'"
        status = raw["status"]
        if status == "PASS":
            return "PASS", None, None
        if status == "FAIL":
            sig = raw.get("signature")
            if not isinstance(sig, str) or not sig:
                return None, None, "FAIL report lacks string signature"
            return "FAIL", sig, None
        return None, None, f"unknown report status {status!r}"

    def evaluate(
        self, db: Database, runs: int, target_signature: str | None = None
    ) -> OracleOutcome:
        """Run ``runs`` times and aggregate. Every run is charged separately."""
        signals: list[RunSignal] = []
        for _ in range(runs):
            sig, _dir = self.run_once(db)
            signals.append(sig)
        return aggregate_signals(signals, target_signature)


def aggregate_signals(
    signals: list[RunSignal], target_signature: str | None = None
) -> OracleOutcome:
    """Aggregate raw per-run signals into a verdict against a target."""
    if not signals:
        return OracleOutcome(
            verdict="UNRESOLVED", signature=None, runs=[], note="no runs recorded"
        )
    unresolved = [s for s in signals if s.report_status is None]
    if unresolved:
        return OracleOutcome(
            verdict="UNRESOLVED",
            signature=None,
            runs=list(signals),
            note=unresolved[0].report_error or "unresolved run",
        )
    statuses = {s.report_status for s in signals}
    signatures = {s.signature for s in signals}
    if len(statuses) != 1:
        return OracleOutcome(
            verdict="UNRESOLVED",
            signature=None,
            runs=list(signals),
            note="inconsistent PASS/FAIL across repeated runs (flaky)",
        )
    status = statuses.pop()
    if status == "PASS":
        return OracleOutcome(verdict="PASS", signature=None, runs=list(signals))
    if len(signatures) != 1:
        return OracleOutcome(
            verdict="UNRESOLVED",
            signature=None,
            runs=list(signals),
            note="FAIL signature changed across repeated runs (flaky)",
        )
    signature = signatures.pop()
    if target_signature is not None and signature != target_signature:
        return OracleOutcome(
            verdict="FAIL",
            signature=signature,
            runs=list(signals),
            note="different signature than target",
        )
    return OracleOutcome(
        verdict="FAIL", signature=signature, runs=list(signals)
    )


def _tail(text: str | None) -> str:
    if not text:
        return ""
    if len(text) <= MAX_OUTPUT_BYTES:
        return text
    return text[-MAX_OUTPUT_BYTES:]
