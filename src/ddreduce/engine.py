"""Reduction engine: batch ddmin followed by per-row deletion-closure checks.

Tie-breaking is fully deterministic: rows are always ordered lexicographically
by their stable ``row_id``; chunks follow that order; no randomness is used, so
the same inputs, state and budget always reproduce the same search.

Certificate semantics ("1-minimal"):
for every row r in the result D, let D' = D \\ deletion_closure({r}).  Either
D' == D (nothing extra removed) or the oracle returns PASS on D'.  Any smaller
candidate that is UNRESOLVED, untested (budget) or still a target FAIL blocks
the certificate.  Global row-count optimality is never claimed.
"""

from __future__ import annotations

import datetime as _dt
import json
import math
import threading
from pathlib import Path

from .cache import OracleCache, make_cache_key
from .model import Database
from .oracle import OracleConfig, OracleOutcome, OracleRunner, aggregate_signals
from .schema import Schema
from .state import SearchState, StateStore
from .values import canonical_json

TOOL_NAME = "ddreduce"
TOOL_VERSION = "1.0.0"


class BudgetExhausted(RuntimeError):
    pass


class SearchInterrupted(RuntimeError):
    pass


class BaselineRejected(RuntimeError):
    pass


class Reducer:
    def __init__(
        self,
        schema: Schema,
        original: Database,
        config: OracleConfig,
        work_dir: str | Path,
        input_dir: str | Path,
        schema_path: str | Path,
        config_path: str | Path,
        stop_event: threading.Event | None = None,
        budget_override: int | None = None,
    ):
        self.schema = schema
        self.original = original
        self.config = config
        self.budget_override = budget_override
        self.work_dir = Path(work_dir)
        self.input_dir = str(input_dir)
        self.schema_path = str(schema_path)
        self.config_path = str(config_path)
        self.stop_event = stop_event or threading.Event()
        self.store = StateStore(self.work_dir)
        self.cache = OracleCache(self.work_dir)
        self.runner = OracleRunner(self.work_dir, config)
        self._traj_seq = 0
        self.cache_hits = 0

    # ============================================================ entry point
    def run(self) -> SearchState:
        if self.store.exists():
            state = self.store.load()
            self._resume_checks(state)
            if state.status == "CERTIFIED":
                return state
        else:
            state = SearchState(
                input_dir=self.input_dir,
                schema_path=self.schema_path,
                oracle_config_path=self.config_path,
                input_hash=self.original.content_hash(),
                target_signature=None,
                max_calls=self.config.max_calls,
            )
            self.store.save(state)

        try:
            if state.phase == "baseline":
                self._baseline(state)
            if state.phase == "empty_probe":
                self._empty_probe(state)
            if state.phase == "batch":
                self._batch_phase(state)
            if state.phase == "row":
                self._row_phase(state)
            self._certify(state)
        except BudgetExhausted:
            state.status = "BUDGET_EXHAUSTED"
            state.note = "subprocess call budget exhausted before certificate issued"
            self.store.save(state)
        except SearchInterrupted:
            state.status = "INTERRUPTED"
            state.note = "stop requested; confirmed calls retained, in-flight call unknown"
            self.store.save(state)
            raise
        return state

    def _resume_checks(self, state: SearchState) -> None:
        if state.input_hash != self.original.content_hash():
            raise BaselineRejected(
                "input content changed since the search was started; refusing resume"
            )
        disk_config = OracleConfig.load(state.oracle_config_path)
        # max_calls is a search budget, not oracle behavior: it may grow on
        # resume. Everything else must match bit-for-bit.
        behavior_keys = ("command", "timeout_seconds", "retest_runs",
                         "confirm_runs", "report_file")
        current = {k: getattr(self.config, k) for k in behavior_keys}
        on_disk = {k: getattr(disk_config, k) for k in behavior_keys}
        if canonical_json(current) != canonical_json(on_disk):
            raise BaselineRejected(
                "oracle configuration changed since the search was started"
            )
        # Continue under the authoritative budget, unless the resume caller
        # explicitly granted a larger total call budget.
        effective_budget = (
            self.budget_override
            if self.budget_override is not None
            else state.max_calls
        )
        self.config = OracleConfig(
            command=self.config.command,
            timeout_seconds=self.config.timeout_seconds,
            retest_runs=self.config.retest_runs,
            confirm_runs=self.config.confirm_runs,
            max_calls=effective_budget,
            report_file=self.config.report_file,
        )
        state.max_calls = effective_budget
        # Trajectory sequence continues after existing entries.
        self._traj_seq = len(self.store.read_trajectory())

    # =============================================================== baseline
    def _baseline(self, state: SearchState) -> None:
        outcome = self._evaluate(self.original, self.config.confirm_runs, state)
        if outcome.verdict != "FAIL" or not outcome.signature:
            state.status = "BASELINE_REJECTED"
            state.note = (
                "original input does not reproducibly fail: "
                f"{outcome.verdict} {outcome.note}".strip()
            )
            self.store.save(state)
            raise BaselineRejected(state.note)
        state.target_signature = outcome.signature
        state.status = "BASELINE_CONFIRMED"
        state.phase = "empty_probe"
        state.current_ids = sorted(self.original.rows)
        state.k = 2
        state.row_index = 0
        self.store.save(state)

    def _empty_probe(self, state: SearchState) -> None:
        """The empty database is tested under exactly the same contract."""
        empty = Database(self.schema, [])
        outcome = self._evaluate(empty, self.config.retest_runs, state)
        if outcome.verdict == "FAIL" and outcome.signature == state.target_signature:
            state.current_ids = []
            state.phase = "row"  # nothing left to check; trivially 1-minimal
            state.row_index = 0
        else:
            state.phase = "batch"
        state.status = "RUNNING"
        self.store.save(state)

    # ============================================================ batch ddmin
    def _batch_phase(self, state: SearchState) -> None:
        while True:
            self._check_stop()
            ids = sorted(state.current_ids)
            n = len(ids)
            if n < 2:
                break
            k = min(state.k, n)
            chunk_size = math.ceil(n / k)
            chunks = [ids[i : i + chunk_size] for i in range(0, n, chunk_size)]
            progress = False
            for chunk in chunks:
                self._check_stop()
                current = self.original.subset(ids)
                removed = current.deletion_closure(chunk)
                candidate = current.apply_deletion(removed)
                outcome = self._evaluate(candidate, self.config.retest_runs, state)
                if (
                    outcome.verdict == "FAIL"
                    and outcome.signature == state.target_signature
                ):
                    state.current_ids = sorted(candidate.rows)
                    state.k = max(2, k - 1)
                    state.row_index = 0
                    state.status = "RUNNING"
                    self.store.save(state)
                    progress = True
                    break
            if progress:
                continue
            if k < n:
                state.k = min(n, k * 2)
                self.store.save(state)
                continue
            break
        state.phase = "row"
        state.row_index = 0
        state.k = 2
        self.store.save(state)

    # ============================================================ row final pass
    def _row_phase(self, state: SearchState) -> None:
        while True:
            ids = sorted(state.current_ids)
            if state.row_index >= len(ids):
                return  # every strict deletion neighbor resolved PASS or SAME
            self._check_stop()
            seed = ids[state.row_index]
            current = self.original.subset(ids)
            removed = current.deletion_closure([seed])
            candidate = current.apply_deletion(removed)
            if len(candidate) == len(current):
                # Deletion closure removes nothing beyond the seed itself;
                # the seed must be inside `removed`, so this only happens when
                # the seed vanished from the DB (defensive). Record as SAME.
                self._record(
                    state,
                    candidate,
                    OracleOutcome(verdict="SAME", signature=None, note="identical candidate"),
                    cache_hit=False,
                    key_digest="-",
                )
                state.row_index += 1
                self.store.save(state)
                continue
            outcome = self._evaluate(candidate, self.config.retest_runs, state)
            if (
                outcome.verdict == "FAIL"
                and outcome.signature == state.target_signature
            ):
                # A strictly smaller candidate still fails: shrink and restart.
                state.current_ids = sorted(candidate.rows)
                state.row_index = 0
                self.store.save(state)
                continue
            if outcome.verdict == "PASS":
                state.row_index += 1
                self.store.save(state)
                continue
            # UNRESOLVED, or a FAIL with a different signature: the certificate
            # cannot be issued while this neighbor is uncertain / changed.
            state.status = "BLOCKED"
            state.note = (
                f"strict neighbor of row {seed!r} is {outcome.verdict}: {outcome.note}"
            )
            self.store.save(state)
            raise BudgetExhausted(state.note) if outcome.verdict == "UNRESOLVED" else (
                BaselineRejected(state.note)
            )

    # ============================================================== evaluating
    def _evaluate(
        self, db: Database, runs: int, state: SearchState
    ) -> OracleOutcome:
        key = make_cache_key(db, self.schema, self.config)
        cached = self.cache.get(key)
        if cached is not None:
            outcome = aggregate_signals(cached, state.target_signature)
            self.cache_hits += 1
            self._record(state, db, outcome, cache_hit=True, key_digest=key.digest)
            return outcome

        if state.calls_used + runs > self.config.max_calls:
            raise BudgetExhausted(
                f"need {runs} call(s), {self.config.max_calls - state.calls_used} left"
            )
        signals = []
        for _ in range(runs):
            self._check_stop()
            # Mark in-flight before the subprocess: if we die here, resume must
            # not pretend an answer exists.
            state.note = f"in_flight:{key.digest}"
            self.store.save(state)
            signal_obj, _dir = self.runner.run_once(db)
            signals.append(signal_obj)
            state.calls_used += 1
            state.note = ""
            self.store.save(state)
        self.cache.put(key, signals)
        outcome = aggregate_signals(signals, state.target_signature)
        self._record(state, db, outcome, cache_hit=False, key_digest=key.digest)
        return outcome

    def _record(
        self,
        state: SearchState,
        db: Database,
        outcome: OracleOutcome,
        cache_hit: bool,
        key_digest: str,
    ) -> None:
        self._traj_seq += 1
        entry = {
            "seq": self._traj_seq,
            "candidate_hash": db.content_hash(),
            "row_count": len(db),
            "table_counts": db.table_counts(),
            "verdict": outcome.verdict,
            "signature": outcome.signature,
            "note": outcome.note,
            "cache_hit": cache_hit,
            "cache_key": key_digest,
            "subprocess_runs": 0 if cache_hit else len(outcome.runs),
            "calls_used": state.calls_used,
        }
        self.store.append_trajectory(entry)

    # ================================================================= certify
    def _certify(self, state: SearchState) -> None:
        result = self.original.subset(sorted(state.current_ids))
        # Re-validate constraints on the result package itself.
        result.validate()
        result_dir = self.work_dir / "result"
        if result_dir.exists():
            for old in result_dir.iterdir():
                old.unlink()
        result.materialize(result_dir)
        state.result_hash = result.content_hash()
        state.result_ids = sorted(result.rows)
        certificate = {
            "tool": TOOL_NAME,
            "version": TOOL_VERSION,
            "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            "input": {
                "directory": self.input_dir,
                "schema": self.schema_path,
                "content_hash": self.original.content_hash(),
                "row_count": len(self.original),
                "table_counts": self.original.table_counts(),
            },
            "target_signature": state.target_signature,
            "oracle": self.config.canonical(),
            "result": {
                "directory": str(result_dir),
                "content_hash": state.result_hash,
                "row_count": len(result),
                "table_counts": result.table_counts(),
                "row_ids": state.result_ids,
            },
            "calls_used": state.calls_used,
            "max_calls": self.config.max_calls,
            "cache_hits": self.cache_hits,
            "algorithm": "batch ddmin over legal deletion closures, then per-row closure final check",
            "tie_break": "rows ordered lexicographically by stable row_id; fixed chunk order",
            "minimality_claim": (
                "1-minimal: every single-row deletion-closure neighbor is either "
                "identical or oracle PASS; no claim of global row-count optimum"
            ),
            "trajectory_file": str(self.store.trajectory_path),
        }
        cert_path = self.work_dir / "certificate.json"
        with open(cert_path, "w", encoding="utf-8") as fh:
            json.dump(certificate, fh, ensure_ascii=False, sort_keys=True, indent=2)
        state.certificate_path = str(cert_path)
        state.phase = "done"
        state.status = "CERTIFIED"
        state.note = ""
        self.store.save(state)

    # ================================================================= helpers
    def _check_stop(self) -> None:
        if self.stop_event.is_set():
            raise SearchInterrupted()
