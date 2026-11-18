"""CDC history reduction engine: batch ddmin + per-transaction final pass.

The reduction object is the *transaction sequence*, never the final table:
every candidate is produced by ``ValidatedLog.normalize`` from a selection of
original transactions, so a candidate only ever contains a committed
subsequence of the original log replayed from the fixed snapshot.

Certificate semantics ("1-minimal by transaction deletion"):
for every transaction t kept in the result, the normalized candidate of
``kept \\ {t}`` must be oracle PASS.  A neighbor that still FAILs the target,
is UNRESOLVED, or is untested (budget) blocks the certificate.  No claim of a
global transaction-count optimum is made.  Deleting every transaction (the
empty log) is tested under the same contract.
"""

from __future__ import annotations

import datetime as _dt
import json
import math
import shutil
import threading
from pathlib import Path

from .cache import OracleCache, make_log_cache_key
from .engine import (
    TOOL_NAME,
    TOOL_VERSION,
    BaselineRejected,
    BudgetExhausted,
    SearchInterrupted,
    oracle_behavior,
)
from .oracle import OracleConfig, OracleOutcome, OracleRunner, aggregate_signals
from .replay import LogCandidate, NormalizeResult, ValidatedLog
from .state import SearchState, StateStore
from .values import canonical_json


class LogReducer:
    def __init__(
        self,
        validated: ValidatedLog,
        config: OracleConfig,
        work_dir: str | Path,
        snapshot_dir: str | Path,
        changes_path: str | Path,
        schema_path: str | Path,
        config_path: str | Path,
        stop_event: threading.Event | None = None,
        budget_override: int | None = None,
    ):
        self.validated = validated
        self.config = config
        self.budget_override = budget_override
        self.work_dir = Path(work_dir)
        self.snapshot_dir = str(snapshot_dir)
        self.changes_path = str(changes_path)
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
                input_dir=self.snapshot_dir,
                schema_path=self.schema_path,
                oracle_config_path=self.config_path,
                input_hash=self.validated.input_binding_hash(),
                target_signature=None,
                max_calls=self.config.max_calls,
                mode="log",
                changes_path=self.changes_path,
                snapshot_hash=self.validated.snapshot_hash,
                log_hash=self.validated.log_hash,
                schema_hash=self.validated.schema_hash,
                oracle_behavior=oracle_behavior(self.config),
            )
            self.store.save(state)

        try:
            if state.phase == "baseline":
                self._baseline(state)
            if state.phase == "empty_probe":
                self._empty_probe(state)
            if state.phase == "batch":
                self._batch_phase(state)
            if state.phase == "tx":
                self._tx_phase(state)
            # A blocked final pass (unknown/changed neighbor) must not certify.
            if state.status != "BLOCKED" and state.phase != "done":
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
        if state.mode != "log":
            raise BaselineRejected(
                "workdir belongs to a tables-mode search; use a fresh workdir "
                "for log-reduce (or 'reduce'/'resume' for tables mode)"
            )
        if state.snapshot_hash != self.validated.snapshot_hash:
            raise BaselineRejected(
                "snapshot content changed since the search was started; refusing resume"
            )
        if state.log_hash != self.validated.log_hash:
            raise BaselineRejected(
                "original change log changed since the search was started; "
                "refusing resume"
            )
        if state.schema_hash != self.validated.schema_hash:
            raise BaselineRejected(
                "schema changed since the search was started; refusing resume"
            )
        if state.input_hash != self.validated.input_binding_hash():
            raise BaselineRejected(
                "input binding changed since the search was started; refusing resume"
            )
        # The behavior configuration recorded when the search began is
        # authoritative: a changed command/timeout/count refuses the resume.
        if state.oracle_behavior and canonical_json(
            state.oracle_behavior
        ) != canonical_json(oracle_behavior(self.config)):
            raise BaselineRejected(
                "oracle configuration changed since the search was started"
            )
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
        self._traj_seq = len(self.store.read_trajectory())

    # =============================================================== baseline
    def _baseline(self, state: SearchState) -> None:
        all_positions = [tx.position for tx in self.validated.log]
        normalized = self.validated.normalize(all_positions)
        if normalized.committed != all_positions:  # pragma: no cover - defensive
            raise RuntimeError(
                "validated original log does not replay cleanly; this is a bug"
            )
        candidate = self.validated.candidate(normalized.committed)
        outcome = self._evaluate(candidate, self.config.confirm_runs, state, normalized)
        if outcome.verdict != "FAIL" or not outcome.signature:
            state.status = "BASELINE_REJECTED"
            state.note = (
                "original log does not reproducibly fail: "
                f"{outcome.verdict} {outcome.note}".strip()
            )
            self.store.save(state)
            raise BaselineRejected(state.note)
        state.target_signature = outcome.signature
        state.status = "BASELINE_CONFIRMED"
        state.phase = "empty_probe"
        state.current_keep = sorted(normalized.committed)
        state.k = 2
        state.tx_index = 0
        self.store.save(state)

    def _empty_probe(self, state: SearchState) -> None:
        """Deleting every transaction (empty log) is tested like any candidate."""
        normalized = self.validated.normalize([])
        candidate = self.validated.candidate(normalized.committed)
        outcome = self._evaluate(candidate, self.config.retest_runs, state, normalized)
        if outcome.verdict == "FAIL" and outcome.signature == state.target_signature:
            state.current_keep = []
            state.phase = "tx"
            state.tx_index = 0
        else:
            state.phase = "batch"
        state.status = "RUNNING"
        self.store.save(state)

    # ============================================================ batch ddmin
    def _batch_phase(self, state: SearchState) -> None:
        while True:
            self._check_stop()
            keep = sorted(state.current_keep)
            n = len(keep)
            if n < 2:
                break
            k = min(state.k, n)
            chunk_size = math.ceil(n / k)
            chunks = [keep[i : i + chunk_size] for i in range(0, n, chunk_size)]
            progress = False
            for chunk in chunks:
                self._check_stop()
                selection = set(keep) - set(chunk)
                normalized = self.validated.normalize(selection)
                candidate = self.validated.candidate(normalized.committed)
                outcome = self._evaluate(
                    candidate, self.config.retest_runs, state, normalized
                )
                if (
                    outcome.verdict == "FAIL"
                    and outcome.signature == state.target_signature
                ):
                    state.current_keep = sorted(normalized.committed)
                    state.k = max(2, k - 1)
                    state.tx_index = 0
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
        state.phase = "tx"
        state.tx_index = 0
        state.k = 2
        self.store.save(state)

    # ==================================================== per-transaction pass
    def _tx_phase(self, state: SearchState) -> None:
        while True:
            keep = sorted(state.current_keep)
            if state.tx_index >= len(keep):
                return  # every strict single-transaction neighbor resolved PASS
            self._check_stop()
            seed = keep[state.tx_index]
            selection = set(keep) - {seed}
            normalized = self.validated.normalize(selection)
            if sorted(normalized.committed) == keep:  # pragma: no cover - defensive
                self._record(
                    state,
                    self.validated.candidate(normalized.committed),
                    normalized,
                    OracleOutcome(verdict="SAME", signature=None, note="identical candidate"),
                    cache_hit=False,
                    key_digest="-",
                )
                state.tx_index += 1
                self.store.save(state)
                continue
            candidate = self.validated.candidate(normalized.committed)
            outcome = self._evaluate(candidate, self.config.retest_runs, state, normalized)
            if (
                outcome.verdict == "FAIL"
                and outcome.signature == state.target_signature
            ):
                # A strictly shorter history still fails: shrink and restart.
                state.current_keep = sorted(normalized.committed)
                state.tx_index = 0
                self.store.save(state)
                continue
            if outcome.verdict == "PASS":
                state.tx_index += 1
                self.store.save(state)
                continue
            # UNRESOLVED, or a FAIL with a different signature: distinct from
            # budget exhaustion, this blocks the certificate as *unknown*.
            state.status = "BLOCKED"
            state.note = (
                f"strict neighbor of tx {self.validated.log.transactions[seed].tx_id!r} "
                f"(position {seed}) is {outcome.verdict}: {outcome.note}"
            )
            self.store.save(state)
            return

    # ============================================================== evaluating
    def _evaluate(
        self,
        candidate: LogCandidate,
        runs: int,
        state: SearchState,
        normalized: NormalizeResult,
    ) -> OracleOutcome:
        key = make_log_cache_key(candidate, self.validated.schema, self.config)
        cached = self.cache.get(key)
        if cached is not None:
            outcome = aggregate_signals(cached, state.target_signature)
            self.cache_hits += 1
            self._record(state, candidate, normalized, outcome, cache_hit=True,
                         key_digest=key.digest)
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
            signal_obj, _dir = self.runner.run_once(candidate)
            signals.append(signal_obj)
            state.calls_used += 1
            state.note = ""
            self.store.save(state)
        self.cache.put(key, signals)
        outcome = aggregate_signals(signals, state.target_signature)
        self._record(state, candidate, normalized, outcome, cache_hit=False,
                     key_digest=key.digest)
        return outcome

    def _record(
        self,
        state: SearchState,
        candidate: LogCandidate,
        normalized: NormalizeResult,
        outcome: OracleOutcome,
        cache_hit: bool,
        key_digest: str,
    ) -> None:
        self._traj_seq += 1
        skipped = [
            {"tx_id": d.tx_id, "tx_pos": d.tx_pos, "reason": d.reason}
            for d in normalized.dispositions
            if d.status == "skipped"
        ]
        entry = {
            "seq": self._traj_seq,
            "candidate_hash": candidate.content_hash(),
            "tx_count": len(candidate.transactions),
            "committed_tx_ids": candidate.tx_ids(),
            "committed_positions": candidate.positions(),
            "skipped": skipped,
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
        keep = sorted(state.current_keep)
        normalized = self.validated.normalize(keep)
        if sorted(normalized.committed) != keep:  # pragma: no cover - defensive
            raise RuntimeError("kept set is not a normalization fixpoint; this is a bug")
        candidate = self.validated.candidate(keep)

        result_dir = self.work_dir / "result"
        if result_dir.exists():
            shutil.rmtree(result_dir)
        candidate.materialize(result_dir)

        # Evidence for independent causal verification: the untouched original
        # log and the exact original transaction selection.  The oracle never
        # reads these; only snapshot/, schema.json and changes.jsonl.
        evidence_dir = result_dir / "evidence"
        evidence_dir.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self.changes_path, evidence_dir / "original-changes.jsonl")
        selection_doc = {
            "selected_positions": keep,
            "selected_tx_ids": candidate.tx_ids(),
            "committed_positions": list(normalized.committed),
            "committed_tx_ids": candidate.tx_ids(),
            "dispositions": [d.to_dict() for d in normalized.dispositions],
            "snapshot_hash": self.validated.snapshot_hash,
            "log_hash": self.validated.log_hash,
            "schema_hash": self.validated.schema_hash,
        }
        with open(evidence_dir / "selection.json", "w", encoding="utf-8") as fh:
            json.dump(selection_doc, fh, ensure_ascii=False, sort_keys=True, indent=2)
            fh.write("\n")

        state.result_hash = candidate.content_hash()
        state.result_ids = []
        state.result_tx_ids = candidate.tx_ids()
        certificate = {
            "tool": TOOL_NAME,
            "version": TOOL_VERSION,
            "mode": "log",
            "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            "input": {
                "snapshot_dir": self.snapshot_dir,
                "schema": self.schema_path,
                "changes": self.changes_path,
                "snapshot_hash": self.validated.snapshot_hash,
                "log_hash": self.validated.log_hash,
                "schema_hash": self.validated.schema_hash,
                "tx_count": len(self.validated.log),
            },
            "target_signature": state.target_signature,
            "oracle": self.config.canonical(),
            "result": {
                "directory": str(result_dir),
                "package_hash": state.result_hash,
                "tx_count": len(candidate.transactions),
                "kept_tx_ids": candidate.tx_ids(),
                "kept_positions": candidate.positions(),
            },
            "evidence": {
                "original_changes": str(evidence_dir / "original-changes.jsonl"),
                "selection": str(evidence_dir / "selection.json"),
            },
            "calls_used": state.calls_used,
            "max_calls": self.config.max_calls,
            "cache_hits": self.cache_hits,
            "algorithm": (
                "batch ddmin over transaction deletions with witness-preserving "
                "normalization, then per-transaction final check"
            ),
            "tie_break": "transactions in original file order; fixed chunk order",
            "minimality_claim": (
                "1-minimal by transaction deletion: every single-transaction "
                "deletion neighbor (normalized) is oracle PASS; no claim of "
                "global transaction-count optimum"
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
