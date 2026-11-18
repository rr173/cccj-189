"""Persistent cache bound to full candidate content + schema + command + config.

The cache stores *raw* per-run signals keyed by a hash of everything that can
change oracle behavior: the exact candidate rows, the schema, the command argv
and the oracle configuration (timeouts, retest counts, ...).  Verdict
classification against a target signature happens at read time, so one cache
entry can serve multiple reduction targets.

Only fully completed runs are cached.  An interrupted, in-flight invocation is
never written, so after a resume it is retried rather than disguised as an
answer.  Cache hits do not consume the subprocess call budget.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .model import Database
from .oracle import OracleConfig, RunSignal
from .schema import Schema
from .values import canonical_json

CACHE_VERSION = 1


@dataclass(frozen=True)
class CacheKey:
    digest: str

    def path(self, root: Path) -> Path:
        return root / self.digest[:2] / f"{self.digest}.json"


def make_cache_key(
    db: Database, schema: Schema, config: OracleConfig
) -> CacheKey:
    payload = {
        "version": CACHE_VERSION,
        "candidate_hash": db.content_hash(),
        "schema": schema.canonical(),
        "oracle": config.canonical(),
    }
    digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    return CacheKey(digest)


def make_log_cache_key(candidate, schema: Schema, config: OracleConfig) -> CacheKey:
    """Cache key for CDC candidates.

    The candidate hash covers the fixed snapshot *and* the ordered candidate
    log including transaction boundaries, so two histories that produce the
    same final table never share an entry.  Schema and the oracle behavior
    configuration are bound exactly as in tables mode.
    """
    payload = {
        "version": CACHE_VERSION,
        "mode": "log",
        "candidate_hash": candidate.content_hash(),
        "schema": schema.canonical(),
        "oracle": config.canonical(),
    }
    digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    return CacheKey(digest)


class OracleCache:
    def __init__(self, work_dir: Path):
        self.root = Path(work_dir) / "cache"
        self.root.mkdir(parents=True, exist_ok=True)

    def get(self, key: CacheKey) -> list[RunSignal] | None:
        path = key.path(self.root)
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            if raw.get("version") != CACHE_VERSION:
                return None
            return [RunSignal(**item) for item in raw["runs"]]
        except (OSError, json.JSONDecodeError, KeyError, TypeError):
            # Corrupt cache entry: treat as miss, never as an oracle answer.
            return None

    def put(self, key: CacheKey, runs: list[RunSignal]) -> None:
        path = key.path(self.root)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": CACHE_VERSION,
            "runs": [r.to_dict() for r in runs],
        }
        # Atomic replace so an interrupted write can never leave a partial entry.
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-", suffix=".json")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, sort_keys=True)
            os.replace(tmp, path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def stats(self) -> dict[str, int]:
        entries = 0
        for path in self.root.rglob("*.json"):
            entries += 1
        return {"entries": entries}
