"""Resumable search state and append-only trajectory log.

The state file is the authoritative, atomically replaced record of a search.
The trajectory log is a human-readable mirror appended after each accepted
or rejected oracle decision.  On resume the state file alone is trusted.

Budget accounting:
  * ``calls_used`` counts only actual subprocess invocations.
  * A cache hit is recorded in the trajectory but never charges the budget.
  * An interrupted in-flight call is marked ``in_flight``; on resume it is
    treated as unknown and retried (the process is gone, so no answer exists).
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

STATE_FILE = "state.json"
TRAJECTORY_FILE = "trajectory.jsonl"


@dataclass
class SearchState:
    input_dir: str
    schema_path: str
    oracle_config_path: str
    input_hash: str
    target_signature: str | None
    calls_used: int = 0
    max_calls: int = 0
    status: str = "INIT"  # INIT / BASELINE_CONFIRMED / RUNNING / INTERRUPTED /
    #                       BUDGET_EXHAUSTED / CERTIFIED / BASELINE_REJECTED
    phase: str = "baseline"  # baseline / batch / row / done
    # ddmin bookkeeping (deterministically reconstructable; stored for resume)
    current_ids: list[str] = field(default_factory=list)
    k: int = 2
    row_index: int = 0
    # Final result (when certified)
    result_hash: str | None = None
    result_ids: list[str] = field(default_factory=list)
    certificate_path: str | None = None
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "input_dir": self.input_dir,
            "schema_path": self.schema_path,
            "oracle_config_path": self.oracle_config_path,
            "input_hash": self.input_hash,
            "target_signature": self.target_signature,
            "calls_used": self.calls_used,
            "max_calls": self.max_calls,
            "status": self.status,
            "phase": self.phase,
            "k": self.k,
            "row_index": self.row_index,
            "current_ids": self.current_ids,
            "result_hash": self.result_hash,
            "result_ids": self.result_ids,
            "certificate_path": self.certificate_path,
            "note": self.note,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "SearchState":
        known = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in raw.items() if k in known})


class StateStore:
    def __init__(self, work_dir: str | Path):
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.work_dir / STATE_FILE
        self.trajectory_path = self.work_dir / TRAJECTORY_FILE

    def exists(self) -> bool:
        return self.state_path.exists()

    def load(self) -> SearchState:
        with open(self.state_path, "r", encoding="utf-8") as fh:
            return SearchState.from_dict(json.load(fh))

    def save(self, state: SearchState) -> None:
        fd, tmp = tempfile.mkstemp(dir=self.work_dir, prefix=".state-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(state.to_dict(), fh, ensure_ascii=False, sort_keys=True, indent=2)
            os.replace(tmp, self.state_path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def append_trajectory(self, entry: dict[str, Any]) -> None:
        with open(self.trajectory_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False, sort_keys=True))
            fh.write("\n")

    def read_trajectory(self) -> list[dict[str, Any]]:
        if not self.trajectory_path.exists():
            return []
        entries = []
        with open(self.trajectory_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    entries.append(json.loads(line))
        return entries
