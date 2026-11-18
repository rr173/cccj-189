"""Error taxonomy for offline capture-v1 ingestion and verification.

Three distinct failure classes are kept separate end to end so that reports can
name what actually failed instead of collapsing everything into one verdict:

* :class:`CaptureArchiveError` - the archive itself is incomplete or in
  conflict (missing offsets, duplicate collisions, dangling transactions,
  unsupported versions, unsafe paths, ...).
* :class:`CaptureRestoreError` - the archive is internally consistent but the
  row history cannot be restored against the given snapshot/schema (stale
  before-image, key migration onto an existing key, commit constraint
  violation, ...).
* :class:`CapturePackageError` - the archive restores fine but the package
  under verification does not match the independent restoration (tampered
  changes, rewritten provenance, doctored counts, wrong bindings, ...).
"""

from __future__ import annotations


class CaptureError(ValueError):
    """Base class for every capture-v1 failure."""


class CaptureArchiveError(CaptureError):
    """The capture archive is malformed, incomplete or internally conflicting."""


class CaptureRestoreError(CaptureError):
    """Row restoration against snapshot/schema failed for a valid archive."""


class CapturePackageError(CaptureError):
    """The package under verification does not match the source archive."""
