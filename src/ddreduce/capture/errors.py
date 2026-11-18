"""Error taxonomy for offline capture ingestion and verification.

Three distinct failure classes are kept separate end to end so that reports can
name what actually failed instead of collapsing everything into one verdict:

* :class:`CaptureArchiveError` - the archive itself is incomplete or in
  conflict (missing offsets, duplicate collisions, dangling transactions,
  unsupported versions, unsafe paths, a message naming a field its declared
  schema version does not have, ...).
* :class:`CaptureRestoreError` - the archive is internally consistent but the
  row history cannot be restored against the given snapshot/schema (stale
  before-image, key migration onto an existing key, commit constraint
  violation, ...).
* :class:`CapturePackageError` - the archive restores fine but the package
  under verification does not match the independent restoration (tampered
  changes, rewritten provenance, doctored counts, wrong bindings, ...).

Schema-evolving archives add one more input-side failure class:

* :class:`CaptureSchemaHistoryError` - the supplied schema history is
  malformed, internally inconsistent, violates the supported evolution rules,
  or is not the history the archive was written against.
"""

from __future__ import annotations


class CaptureError(ValueError):
    """Base class for every capture failure.

    ``locator`` optionally carries structured source coordinates (file/line,
    stream/offset, tx_id, tx_order, schema version, commit_seq) so callers can
    report *where* a rejected message lives without parsing the message text.
    """

    def __init__(self, message: str = "", **locator: object) -> None:
        super().__init__(message)
        self.locator: dict[str, object] = {
            k: v for k, v in locator.items() if v is not None
        }

    def with_locator(self, **extra: object) -> "CaptureError":
        merged = dict(self.locator)
        merged.update({k: v for k, v in extra.items() if v is not None})
        self.locator = merged
        return self


class CaptureArchiveError(CaptureError):
    """The capture archive is malformed, incomplete or internally conflicting."""


class CaptureSchemaHistoryError(CaptureError):
    """The schema history (or its binding to the archive) is invalid.

    Covers malformed history files, unsupported evolution steps, and an
    archive/message whose declared schema version disagrees with the history.
    """


class CaptureRestoreError(CaptureError):
    """Row restoration against snapshot/schema failed for a valid archive."""


class CapturePackageError(CaptureError):
    """The package under verification does not match the source archive."""
