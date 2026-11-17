"""Independent certificate verification.

``verify`` never trusts the searcher's "minimal" flag.  It:
  1. reloads the result package and re-runs schema/unique/FK validation;
  2. re-runs the oracle on the result itself to confirm the target fault;
  3. enumerates *every* strict single-row deletion-closure neighbor itself and
     re-tests each one (including the empty-database case when reachable);
  4. reports one of three distinct outcomes:

       VERIFIED_MINIMAL    all neighbors PASS (or identical) and fault confirmed
       UNKNOWN_NEIGHBORS   one or more neighbors are UNRESOLVED / untested
       BUDGET_EXHAUSTED    the call budget ran out before every neighbor was tested
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from .cache import OracleCache, make_cache_key
from .model import Database
from .oracle import OracleConfig, OracleOutcome, OracleRunner, aggregate_signals
from .schema import Schema
from .values import DataError


class VerifyError(RuntimeError):
    pass


@dataclass
class NeighborResult:
    seed_row_id: str
    removed_row_ids: list[str]
    candidate_hash: str
    verdict: str
    signature: str | None
    note: str
    cache_hit: bool

    def to_dict(self) -> dict:
        return {
            "seed_row_id": self.seed_row_id,
            "removed_row_ids": self.removed_row_ids,
            "candidate_hash": self.candidate_hash,
            "verdict": self.verdict,
            "signature": self.signature,
            "note": self.note,
            "cache_hit": self.cache_hit,
        }


@dataclass
class VerifyReport:
    verdict: str  # VERIFIED_MINIMAL / UNKNOWN_NEIGHBORS / BUDGET_EXHAUSTED / REJECTED
    target_signature: str | None
    result_row_count: int
    result_fault_verdict: str
    neighbors: list[NeighborResult] = field(default_factory=list)
    calls_used: int = 0
    cache_hits: int = 0
    reason: str = ""

    def to_dict(self) -> dict:
        return {
            "verdict": self.verdict,
            "reason": self.reason,
            "target_signature": self.target_signature,
            "result_fault_verdict": self.result_fault_verdict,
            "result_row_count": self.result_row_count,
            "calls_used": self.calls_used,
            "cache_hits": self.cache_hits,
            "neighbors": [n.to_dict() for n in self.neighbors],
        }


class Verifier:
    def __init__(
        self,
        schema: Schema,
        config: OracleConfig,
        work_dir: str | Path,
        use_cache: bool = True,
    ):
        self.schema = schema
        self.config = config
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.runner = OracleRunner(self.work_dir / "verify", config)
        self.cache = OracleCache(self.work_dir)
        self.use_cache = use_cache
        self.calls_used = 0
        self.cache_hits = 0
        self._last_was_cache_hit = False

    def verify_package(
        self, result_dir: str | Path, target_signature: str, budget: int
    ) -> VerifyReport:
        result_dir = Path(result_dir)
        try:
            db = Database.load(self.schema, result_dir)
        except DataError as exc:
            return VerifyReport(
                verdict="REJECTED",
                target_signature=target_signature,
                result_row_count=-1,
                result_fault_verdict="UNRESOLVED",
                reason=f"result package fails constraint validation: {exc}",
            )

        # 1. Confirm the target fault on the result package itself.
        fault = self._evaluate(db, budget)
        if fault is None:
            return self._budget_report(db, target_signature, "fault confirmation")
        if not (fault.verdict == "FAIL" and fault.signature == target_signature):
            return VerifyReport(
                verdict="REJECTED",
                target_signature=target_signature,
                result_row_count=len(db),
                result_fault_verdict=fault.verdict,
                reason=(
                    f"result package does not reproduce target fault: "
                    f"{fault.verdict} {fault.signature or ''} {fault.note}".strip()
                ),
            )

        # 2. Enumerate every strict single-row deletion-closure neighbor.
        neighbors: list[NeighborResult] = []
        unknown = False
        for seed in sorted(db.rows):
            removed = db.deletion_closure([seed])
            candidate = db.apply_deletion(removed)
            if len(candidate) == len(db):
                neighbors.append(
                    NeighborResult(
                        seed_row_id=seed,
                        removed_row_ids=sorted(removed),
                        candidate_hash=db.content_hash(),
                        verdict="SAME",
                        signature=None,
                        note="deletion closure yields the identical package",
                        cache_hit=False,
                    )
                )
                continue
            outcome = self._evaluate(candidate, budget)
            if outcome is None:
                return self._budget_report(
                    db, target_signature, f"neighbor of {seed}", neighbors
                )
            cache_hit = self._last_was_cache_hit
            neighbors.append(
                NeighborResult(
                    seed_row_id=seed,
                    removed_row_ids=sorted(removed),
                    candidate_hash=candidate.content_hash(),
                    verdict=outcome.verdict,
                    signature=outcome.signature,
                    note=outcome.note,
                    cache_hit=cache_hit,
                )
            )
            if outcome.verdict != "PASS":
                # UNRESOLVED => cannot prove minimality; FAIL (any signature)
                # means a strict smaller FAILing package exists.
                unknown = True

        report = VerifyReport(
            verdict="UNKNOWN_NEIGHBORS" if unknown else "VERIFIED_MINIMAL",
            target_signature=target_signature,
            result_row_count=len(db),
            result_fault_verdict="FAIL",
            neighbors=neighbors,
            calls_used=self.calls_used,
            cache_hits=self.cache_hits,
            reason=(
                "at least one strict neighbor is UNRESOLVED or still FAIL"
                if unknown
                else "all strict deletion-closure neighbors are PASS or identical"
            ),
        )
        return report

    def _evaluate(self, db: Database, budget: int) -> OracleOutcome | None:
        """Return None when the budget cannot cover this evaluation."""
        key = make_cache_key(db, self.schema, self.config)
        if self.use_cache:
            cached = self.cache.get(key)
            if cached is not None:
                self.cache_hits += 1
                self._last_was_cache_hit = True
                return aggregate_signals(cached, None)
        self._last_was_cache_hit = False
        runs = self.config.retest_runs
        if self.calls_used + runs > budget:
            return None
        signals = []
        for _ in range(runs):
            sig, _dir = self.runner.run_once(db)
            signals.append(sig)
            self.calls_used += 1
        self.cache.put(key, signals)
        return aggregate_signals(signals, None)

    def _budget_report(
        self,
        db: Database,
        target_signature: str,
        where: str,
        neighbors: list[NeighborResult] | None = None,
    ) -> VerifyReport:
        return VerifyReport(
            verdict="BUDGET_EXHAUSTED",
            target_signature=target_signature,
            result_row_count=len(db),
            result_fault_verdict="UNKNOWN",
            neighbors=neighbors or [],
            calls_used=self.calls_used,
            cache_hits=self.cache_hits,
            reason=f"call budget exhausted while testing {where}",
        )


def write_report(report: VerifyReport, path: str | Path) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(report.to_dict(), fh, ensure_ascii=False, sort_keys=True, indent=2)
