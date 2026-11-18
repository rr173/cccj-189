"""Independent verification of CDC history certificates.

``log-verify`` never trusts the searcher's flags.  From the delivered package
it:

  1. reloads ``result/snapshot``, ``result/schema.json`` and
     ``result/evidence/original-changes.jsonl`` and re-validates the *original*
     log from scratch (any illegality or tampering rejects the package);
  2. re-derives every version/absence/parent-version witness itself and
     confirms that ``result/changes.jsonl`` is exactly the normalized result of
     the declared selection in ``result/evidence/selection.json``;
  3. checks the certificate's recorded hashes and references against the
     package it claims to describe (snapshot, original log, schema, result
     package, oracle behavior configuration);
  4. re-runs the fault command on the result package to confirm the target
     signature, then recomputes every strict single-transaction deletion
     neighbor itself and re-tests each one.

Nothing is read from the certificate's dependency edges, final-state hashes
or tested booleans: those are recomputed.  Outcomes:

    VERIFIED_MINIMAL    fault reproduced; every neighbor PASS
    UNKNOWN_NEIGHBORS   a neighbor is UNRESOLVED or still FAILs the target
    BUDGET_EXHAUSTED    the call budget ran out before all neighbors finished
    REJECTED            package/certificate inconsistent, tampered, or the
                        result does not reproduce the target fault
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from .cache import OracleCache, make_log_cache_key
from .logmodel import load_changelog
from .model import Database
from .oracle import OracleConfig, OracleOutcome, OracleRunner, aggregate_signals
from .replay import LogCandidate, ValidatedLog, validate_log_inputs
from .schema import load_schema
from .values import DataError, canonical_json


class LogVerifyError(RuntimeError):
    pass


@dataclass
class LogNeighborResult:
    tx_pos: int
    tx_id: object
    committed_tx_ids: list
    candidate_hash: str
    verdict: str
    signature: str | None
    note: str
    cache_hit: bool

    def to_dict(self) -> dict:
        return {
            "tx_pos": self.tx_pos,
            "tx_id": self.tx_id,
            "committed_tx_ids": self.committed_tx_ids,
            "candidate_hash": self.candidate_hash,
            "verdict": self.verdict,
            "signature": self.signature,
            "note": self.note,
            "cache_hit": self.cache_hit,
        }


@dataclass
class LogVerifyReport:
    verdict: str  # VERIFIED_MINIMAL / UNKNOWN_NEIGHBORS / BUDGET_EXHAUSTED / REJECTED
    target_signature: str | None
    result_tx_count: int
    result_fault_verdict: str
    neighbors: list[LogNeighborResult] = field(default_factory=list)
    calls_used: int = 0
    cache_hits: int = 0
    reason: str = ""

    def to_dict(self) -> dict:
        return {
            "verdict": self.verdict,
            "reason": self.reason,
            "target_signature": self.target_signature,
            "result_fault_verdict": self.result_fault_verdict,
            "result_tx_count": self.result_tx_count,
            "calls_used": self.calls_used,
            "cache_hits": self.cache_hits,
            "neighbors": [n.to_dict() for n in self.neighbors],
        }


class LogVerifier:
    def __init__(
        self,
        config: OracleConfig,
        work_dir: str | Path,
        use_cache: bool = True,
    ):
        self.config = config
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.runner = OracleRunner(self.work_dir / "verify", config)
        self.cache = OracleCache(self.work_dir)
        self.use_cache = use_cache
        self.calls_used = 0
        self.cache_hits = 0
        self._last_was_cache_hit = False

    # ------------------------------------------------------------- main flow
    def verify_certificate(self, workdir: str | Path, budget: int) -> LogVerifyReport:
        workdir = Path(workdir)
        cert_path = workdir / "certificate.json"
        if not cert_path.exists():
            return self._reject(None, f"certificate.json not found in {workdir}")
        try:
            with open(cert_path, "r", encoding="utf-8") as fh:
                cert = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            return self._reject(None, f"unreadable certificate: {exc}")
        if cert.get("mode") != "log":
            return self._reject(
                None,
                "certificate is not a CDC log certificate (mode != 'log'); "
                "use 'verify' for tables-mode certificates",
            )
        target = cert.get("target_signature")
        # The delivered package lives inside the workdir; always prefer it
        # over the absolute path recorded at certification time (the workdir
        # may have been relocated or copied for verification).
        result_dir = workdir / "result"
        if not result_dir.is_dir():
            result_dir = Path(cert["result"]["directory"])
        return self.verify_package(result_dir, target, budget, cert=cert)

    def verify_package(
        self,
        result_dir: str | Path,
        target_signature: str | None,
        budget: int,
        cert: dict | None = None,
    ) -> LogVerifyReport:
        result_dir = Path(result_dir)
        try:
            validated, selection, candidate = self._load_and_check_package(
                result_dir, cert
            )
        except LogVerifyError as exc:
            return self._reject(target_signature, str(exc))

        if cert is not None:
            mismatch = self._check_certificate_references(
                cert, validated, selection, candidate
            )
            if mismatch:
                return self._reject(target_signature, mismatch)
            behavior = {k: cert["oracle"][k] for k in
                        ("command", "timeout_seconds", "retest_runs",
                         "confirm_runs", "report_file")}
            current = {k: getattr(self.config, k) for k in behavior}
            if canonical_json(behavior) != canonical_json(current):
                return self._reject(
                    target_signature,
                    "oracle behavior configuration differs from the certificate",
                )

        # 1. Confirm the target fault on the result package itself.
        fault = self._evaluate(candidate, validated.schema, budget)
        if fault is None:
            return self._budget_report(candidate, target_signature, "fault confirmation")
        if not (fault.verdict == "FAIL" and fault.signature == target_signature):
            return LogVerifyReport(
                verdict="REJECTED",
                target_signature=target_signature,
                result_tx_count=len(candidate.transactions),
                result_fault_verdict=fault.verdict,
                reason=(
                    "result package does not reproduce target fault: "
                    f"{fault.verdict} {fault.signature or ''} {fault.note}".strip()
                ),
            )

        # 2. Recompute every strict single-transaction deletion neighbor.
        neighbors: list[LogNeighborResult] = []
        unknown = False
        keep = sorted(selection)
        for pos in keep:
            tx_id = validated.log.transactions[pos].tx_id
            normalized = validated.normalize(set(keep) - {pos})
            neighbor = validated.candidate(normalized.committed)
            outcome = self._evaluate(neighbor, validated.schema, budget)
            if outcome is None:
                return self._budget_report(
                    candidate, target_signature, f"neighbor of tx {tx_id!r}", neighbors
                )
            neighbors.append(
                LogNeighborResult(
                    tx_pos=pos,
                    tx_id=tx_id,
                    committed_tx_ids=neighbor.tx_ids(),
                    candidate_hash=neighbor.content_hash(),
                    verdict=outcome.verdict,
                    signature=outcome.signature,
                    note=outcome.note,
                    cache_hit=self._last_was_cache_hit,
                )
            )
            if outcome.verdict != "PASS":
                # UNRESOLVED => cannot prove minimality; FAIL (any signature)
                # means a strictly shorter failing history exists.
                unknown = True

        return LogVerifyReport(
            verdict="UNKNOWN_NEIGHBORS" if unknown else "VERIFIED_MINIMAL",
            target_signature=target_signature,
            result_tx_count=len(candidate.transactions),
            result_fault_verdict="FAIL",
            neighbors=neighbors,
            calls_used=self.calls_used,
            cache_hits=self.cache_hits,
            reason=(
                "at least one strict neighbor is UNRESOLVED or still FAIL"
                if unknown
                else "all strict single-transaction deletion neighbors are PASS"
            ),
        )

    # ------------------------------------------------------- package loading
    def _load_and_check_package(
        self, result_dir: Path, cert: dict | None
    ) -> tuple[ValidatedLog, list[int], LogCandidate]:
        if not result_dir.is_dir():
            raise LogVerifyError(f"result package not found: {result_dir}")
        schema_path = result_dir / "schema.json"
        snapshot_dir = result_dir / "snapshot"
        changes_path = result_dir / "changes.jsonl"
        evidence_log = result_dir / "evidence" / "original-changes.jsonl"
        evidence_sel = result_dir / "evidence" / "selection.json"
        for path, what in [
            (schema_path, "schema.json"),
            (snapshot_dir, "snapshot/"),
            (changes_path, "changes.jsonl"),
            (evidence_log, "evidence/original-changes.jsonl"),
            (evidence_sel, "evidence/selection.json"),
        ]:
            if not path.exists():
                raise LogVerifyError(f"result package is missing {what}: {path}")
        try:
            schema_text = schema_path.read_text(encoding="utf-8")
            schema = load_schema(schema_path)
            snapshot = Database.load(schema, snapshot_dir)
        except (DataError, OSError) as exc:
            raise LogVerifyError(f"package snapshot/schema invalid: {exc}") from exc

        # Re-derive every witness from the package's own original log; an
        # illegal or tampered original log is caught here, before any command.
        try:
            original_log = load_changelog(evidence_log, schema)
            validated = validate_log_inputs(schema, schema_text, snapshot, original_log)
        except Exception as exc:
            raise LogVerifyError(f"original log in evidence is invalid: {exc}") from exc

        try:
            with open(evidence_sel, "r", encoding="utf-8") as fh:
                selection_doc = json.load(fh)
            selection = [int(p) for p in selection_doc["selected_positions"]]
        except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            raise LogVerifyError(f"evidence selection is malformed: {exc}") from exc
        if sorted(selection) != selection or any(
            p < 0 or p >= len(validated.log) for p in selection
        ):
            raise LogVerifyError("evidence selection positions out of range")

        # The candidate must be exactly the normalized result of the declared
        # selection of the original log — recomputed here, not trusted.
        normalized = validated.normalize(selection)
        if sorted(normalized.committed) != sorted(selection):
            raise LogVerifyError(
                "declared selection is not a normalization fixpoint: committed "
                f"{normalized.committed}, declared {sorted(selection)}"
            )
        candidate = validated.candidate(normalized.committed)

        # The candidate file on disk must equal the recomputed candidate.
        try:
            on_disk = load_changelog(changes_path, schema)
        except Exception as exc:
            raise LogVerifyError(f"candidate changes.jsonl is invalid: {exc}") from exc
        if [tx.canonical() for tx in on_disk] != [
            tx.canonical() for tx in candidate.transactions
        ]:
            raise LogVerifyError(
                "candidate changes.jsonl does not match the normalized result of "
                "the declared selection (tampered log or transaction boundaries)"
            )
        return validated, sorted(selection), candidate

    def _check_certificate_references(
        self,
        cert: dict,
        validated: ValidatedLog,
        selection: list[int],
        candidate: LogCandidate,
    ) -> str | None:
        """Every hash/reference the certificate records must recompute exactly."""
        try:
            inp = cert["input"]
            if inp["snapshot_hash"] != validated.snapshot_hash:
                return "certificate snapshot_hash does not match the package snapshot"
            if inp["log_hash"] != validated.log_hash:
                return (
                    "certificate log_hash does not match "
                    "evidence/original-changes.jsonl (tampered original log)"
                )
            if inp["schema_hash"] != validated.schema_hash:
                return "certificate schema_hash does not match package schema.json"
            if inp["tx_count"] != len(validated.log):
                return "certificate tx_count does not match the original log"
            result = cert["result"]
            if result["package_hash"] != candidate.content_hash():
                return "certificate package_hash does not match the result package"
            if list(result["kept_positions"]) != list(selection):
                return "certificate kept_positions do not match the selection"
            if list(result["kept_tx_ids"]) != candidate.tx_ids():
                return "certificate kept_tx_ids do not match the result package"
            evidence = cert.get("evidence", {})
            if evidence.get("original_changes") and not Path(
                evidence["original_changes"]
            ).exists():
                return "certificate evidence original_changes path is missing"
            if evidence.get("selection") and not Path(evidence["selection"]).exists():
                return "certificate evidence selection path is missing"
        except (KeyError, TypeError) as exc:
            return f"certificate is missing required references: {exc}"
        return None

    # -------------------------------------------------------------- evaluating
    def _evaluate(
        self, candidate: LogCandidate, schema, budget: int
    ) -> OracleOutcome | None:
        """Return None when the budget cannot cover this evaluation."""
        key = make_log_cache_key(candidate, schema, self.config)
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
            sig, _dir = self.runner.run_once(candidate)
            signals.append(sig)
            self.calls_used += 1
        self.cache.put(key, signals)
        return aggregate_signals(signals, None)

    # ---------------------------------------------------------------- reports
    def _reject(self, target_signature: str | None, reason: str) -> LogVerifyReport:
        return LogVerifyReport(
            verdict="REJECTED",
            target_signature=target_signature,
            result_tx_count=-1,
            result_fault_verdict="UNRESOLVED",
            reason=reason,
        )

    def _budget_report(
        self,
        candidate: LogCandidate,
        target_signature: str | None,
        where: str,
        neighbors: list[LogNeighborResult] | None = None,
    ) -> LogVerifyReport:
        return LogVerifyReport(
            verdict="BUDGET_EXHAUSTED",
            target_signature=target_signature,
            result_tx_count=len(candidate.transactions),
            result_fault_verdict="UNKNOWN",
            neighbors=neighbors or [],
            calls_used=self.calls_used,
            cache_hits=self.cache_hits,
            reason=f"call budget exhausted while testing {where}",
        )


def write_log_report(report: LogVerifyReport, path: str | Path) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(report.to_dict(), fh, ensure_ascii=False, sort_keys=True, indent=2)
