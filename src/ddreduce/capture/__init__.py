"""Offline capture-v1 archive ingestion (``capture-import`` / ``capture-verify``).

This package restores partitioned CDC capture archives - sharded JSONL with
retransmissions, partial update images and transport-level reordering - into
the full-image ``changes.jsonl`` input shape already consumed by
``log-replay`` and ``log-reduce``.  It adds no reduction strategy and changes
no existing searcher: the two new entry points are purely offline
ingestion/verification tooling and never run an oracle.
"""

from .errors import (
    CaptureArchiveError,
    CaptureError,
    CapturePackageError,
    CaptureRestoreError,
)
from .importer import ImportResult, run_import
from .verifier import (
    VERDICT_ARCHIVE,
    VERDICT_MATCH,
    VERDICT_PACKAGE,
    VERDICT_RESTORE,
    VerificationResult,
    run_verify,
)

__all__ = [
    "CaptureArchiveError",
    "CaptureError",
    "CapturePackageError",
    "CaptureRestoreError",
    "ImportResult",
    "run_import",
    "run_verify",
    "VerificationResult",
    "VERDICT_ARCHIVE",
    "VERDICT_MATCH",
    "VERDICT_PACKAGE",
    "VERDICT_RESTORE",
]
