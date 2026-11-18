"""Offline capture ingestion (``capture-import`` / ``capture-verify``).

This package restores partitioned CDC capture archives - sharded JSONL with
retransmissions, partial update images and transport-level reordering - into
the full-image ``changes.jsonl`` input shape already consumed by
``log-replay`` and ``log-reduce``.  It adds no reduction strategy and changes
no existing searcher: the two new entry points are purely offline
ingestion/verification tooling and never run an oracle.

Two archive formats are accepted:

* **capture-v1** - one fixed schema plus table/column bindings (unchanged
  format and commands);
* **capture-v2** - an ordered schema history with stable field IDs and a
  declared write version on every change message.  Events written before and
  after a rename/extension are each interpreted with their own version and
  then mapped onto the unified target schema, backfilling columns that did
  not exist yet.
"""

from .errors import (
    CaptureArchiveError,
    CaptureError,
    CapturePackageError,
    CaptureRestoreError,
    CaptureSchemaHistoryError,
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
    "CaptureSchemaHistoryError",
    "ImportResult",
    "run_import",
    "run_verify",
    "VerificationResult",
    "VERDICT_ARCHIVE",
    "VERDICT_MATCH",
    "VERDICT_PACKAGE",
    "VERDICT_RESTORE",
]
