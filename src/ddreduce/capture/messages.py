"""capture-v1 message validation and canonicalization.

Every physical shard line is one message.  All messages share the envelope::

    {"source_id": <string>, "stream": <string>, "offset": <non-neg int>,
     "kind": "change" | "commit" | "abort" | "heartbeat", ...}

Full field tables
-----------------
All kinds:

    field       required  type / rule
    source_id   yes       non-empty string; must equal the manifest source_id
    stream      yes       non-empty string; must be a declared stream
    offset      yes       non-negative integer; booleans are rejected
    kind        yes       one of change / commit / abort / heartbeat

``change`` adds:

    field       required  type / rule
    tx_id       yes       non-empty string
    tx_order    yes       integer >= 1 (booleans rejected)
    table       yes       non-empty string; must be a bound source table
    op          yes       insert / update / delete
    key         yes       object with exactly the source primary-key columns
    before      conditional  object; required for update/delete (delete allows
                          the empty object), forbidden on insert; may be partial
    after       conditional  object; required for insert (complete row) and
                          update (may be partial, empty object still an update),
                          forbidden on delete
    new_key     optional   only on update; object with the full source key

``change`` in a **capture-v2** (schema-evolving) archive additionally carries
``schema_version`` - the schema version the writer used for this row image.
It is mandatory there and forbidden in capture-v1.  Its value must be one of
the versions declared by the archive's schema history.

``commit`` adds:

    field        required  type / rule
    tx_id        yes       non-empty string
    commit_seq   yes       non-negative integer (booleans rejected)
    event_count  yes       non-negative integer
    table_counts yes       object: source table -> positive integer; the sum of
                          its positive entries equals event_count; ``{}`` for a
                          zero-event transaction

``abort`` adds the same ``tx_id`` / ``event_count`` / ``table_counts`` fields
but must NOT carry ``commit_seq``.  ``heartbeat`` carries only the envelope.

Unknown fields, missing fields and booleans impersonating integers are always
rejected.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..values import canonical_json
from .errors import CaptureArchiveError
from .manifest import Manifest

if TYPE_CHECKING:
    from ..schema import Schema

KINDS = ("change", "commit", "abort", "heartbeat")
CHANGE_OPS = ("insert", "update", "delete")
ENVELOPE_FIELDS = ("source_id", "stream", "offset", "kind")


@dataclass(frozen=True)
class Coordinate:
    stream: str
    offset: int

    def as_tuple(self) -> tuple[str, int]:
        return (self.stream, self.offset)


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_nonneg_int(value: object) -> bool:
    return _is_int(value) and value >= 0


def _is_pos_int(value: object) -> bool:
    return _is_int(value) and value >= 1


def canonical_message(message: dict[str, Any]) -> str:
    """Canonical serialization ignoring JSON formatting/key-order differences.

    Two physical messages are the same message exactly when their canonical
    encodings (transport coordinates included) are equal.
    """
    return canonical_json(message)


def business_payload(message: dict[str, Any]) -> dict[str, Any]:
    """The message with its transport coordinates (source_id/stream/offset).

    The remaining object is the *business content* used for logical
    de-duplication of the same ``(tx_id, tx_order)`` event or the same
    transaction end decision delivered at several positions.
    """
    return {k: v for k, v in message.items() if k not in ("source_id", "stream", "offset")}


def canonical_business(message: dict[str, Any]) -> str:
    return canonical_json(business_payload(message))


def validate_envelope(
    obj: Any, manifest: Manifest, file_name: str, line_no: int
) -> tuple[str, str, int, str]:
    """Validate the shared envelope; return (source_id, stream, offset, kind)."""
    where = f"{file_name}:{line_no}"
    if not isinstance(obj, dict):
        raise CaptureArchiveError(f"{where}: message must be a JSON object")
    missing = [f for f in ENVELOPE_FIELDS if f not in obj]
    if missing:
        raise CaptureArchiveError(f"{where}: missing envelope fields {sorted(missing)}")
    source_id = obj["source_id"]
    if not isinstance(source_id, str) or not source_id:
        raise CaptureArchiveError(f"{where}: source_id must be a non-empty string")
    if source_id != manifest.source_id:
        raise CaptureArchiveError(
            f"{where}: source_id {source_id!r} does not match manifest "
            f"source_id {manifest.source_id!r}"
        )
    stream = obj["stream"]
    if not isinstance(stream, str) or not stream:
        raise CaptureArchiveError(f"{where}: stream must be a non-empty string")
    spec = manifest.stream(stream)
    if spec is None:
        raise CaptureArchiveError(f"{where}: undeclared stream {stream!r}")
    offset = obj["offset"]
    if not _is_nonneg_int(offset):
        raise CaptureArchiveError(
            f"{where}: offset must be a non-negative integer (booleans rejected)"
        )
    if offset < spec.first_offset or offset > spec.last_offset:
        raise CaptureArchiveError(
            f"{where}: offset {offset} outside declared range "
            f"[{spec.first_offset}, {spec.last_offset}] of stream {stream!r}"
        )
    kind = obj["kind"]
    if kind not in KINDS:
        raise CaptureArchiveError(
            f"{where}: kind must be one of {list(KINDS)}, got {kind!r}"
        )
    return source_id, stream, offset, kind


def _reject_unknown(obj: dict, allowed: set[str], where: str) -> None:
    unknown = set(obj) - allowed
    if unknown:
        raise CaptureArchiveError(f"{where}: unknown fields {sorted(unknown)}")


def _check_tx_id(obj: dict, where: str) -> str:
    tx_id = obj.get("tx_id")
    if not isinstance(tx_id, str) or not tx_id:
        raise CaptureArchiveError(f"{where}: tx_id must be a non-empty string")
    return tx_id


def _check_table_counts(obj: dict, where: str, manifest: Manifest) -> dict[str, int]:
    if "table_counts" not in obj:
        raise CaptureArchiveError(f"{where}: missing table_counts")
    counts = obj["table_counts"]
    if not isinstance(counts, dict):
        raise CaptureArchiveError(f"{where}: table_counts must be an object")
    total = 0
    for table, count in counts.items():
        tw = f"{where} table_counts[{table!r}]"
        if not isinstance(table, str) or table not in manifest.bindings.tables:
            raise CaptureArchiveError(f"{tw}: unknown/unbound source table {table!r}")
        if not _is_pos_int(count):
            raise CaptureArchiveError(
                f"{tw}: count must be a positive integer (zero entries are omitted; "
                "booleans are rejected)"
            )
        total += count
    event_count = obj.get("event_count")
    if event_count is not None and total != event_count:
        raise CaptureArchiveError(
            f"{where}: sum of positive table_counts ({total}) != event_count "
            f"({event_count})"
        )
    if event_count == 0 and counts:
        raise CaptureArchiveError(
            f"{where}: zero-event transaction must carry an empty table_counts object"
        )
    return counts


def _check_key(obj: dict, field_name: str, source_pk: tuple[str, ...], where: str) -> dict:
    if field_name not in obj:
        raise CaptureArchiveError(f"{where}: missing {field_name}")
    key = obj[field_name]
    if not isinstance(key, dict):
        raise CaptureArchiveError(f"{where}: {field_name} must be an object")
    if set(key) != set(source_pk):
        raise CaptureArchiveError(
            f"{where}: {field_name} must contain exactly the source primary-key "
            f"columns {sorted(source_pk)}, got {sorted(key)}"
        )
    for col in source_pk:
        if key[col] is None:
            raise CaptureArchiveError(f"{where}: {field_name} column {col!r} is null")
    return key


def _source_pk(
    manifest: Manifest, source_table: str, schema: "Schema"
) -> tuple[str, ...]:
    target_table = manifest.bindings.target_table(source_table)
    # Import locally to avoid a circular import at module load.
    from ..capture.restore import source_primary_key

    return source_primary_key(manifest, source_table, target_table, schema)


def validate_message(
    obj: Any, manifest: Manifest, schema: "Schema", file_name: str, line_no: int
) -> dict:
    """Full structural validation of one shard message.

    Envelope, kind-specific fields and value shapes are checked here.  Semantic
    cross-message checks (de-duplication, coverage, transaction closure, row
    restoration) happen in later stages.
    """
    _source_id, stream, offset, kind = validate_envelope(
        obj, manifest, file_name, line_no
    )
    where = f"{file_name}:{line_no} ({stream}@{offset})"

    if kind == "heartbeat":
        _reject_unknown(obj, set(ENVELOPE_FIELDS), where)
        return obj

    if kind == "change":
        allowed = set(ENVELOPE_FIELDS) | {
            "tx_id", "tx_order", "table", "op", "key", "before", "after", "new_key"
        }
        if manifest.evolving:
            allowed.add("schema_version")
        _reject_unknown(obj, allowed, where)
        if manifest.evolving:
            if "schema_version" not in obj:
                raise CaptureArchiveError(
                    f"{where}: change message missing schema_version; "
                    "capture-v2 archives must declare the write schema version "
                    "of every row image"
                )
            sv = obj["schema_version"]
            if not _is_pos_int(sv):
                raise CaptureArchiveError(
                    f"{where}: schema_version must be an integer >= 1 "
                    "(booleans rejected)"
                )
        elif "schema_version" in obj:
            raise CaptureArchiveError(
                f"{where}: schema_version is only allowed in capture-v2 "
                "archives with a schema history"
            )
        for field_name in ("tx_id", "tx_order", "table", "op", "key"):
            if field_name not in obj:
                raise CaptureArchiveError(f"{where}: change missing field {field_name!r}")
        _check_tx_id(obj, where)
        if not _is_pos_int(obj["tx_order"]):
            raise CaptureArchiveError(
                f"{where}: tx_order must be an integer >= 1 (booleans rejected)"
            )
        source_table = obj["table"]
        if not isinstance(source_table, str) or source_table not in manifest.bindings.tables:
            raise CaptureArchiveError(f"{where}: unknown/unbound source table {source_table!r}")
        op = obj["op"]
        if op not in CHANGE_OPS:
            raise CaptureArchiveError(
                f"{where}: op must be one of {list(CHANGE_OPS)}, got {op!r}"
            )
        if manifest.evolving:
            from .evolve import layout_for

            if manifest.history is None:  # pragma: no cover - manifest always sets it
                raise CaptureArchiveError(
                    f"{where}: archive declares a schema history but none is loaded"
                )
            _check_declared_version(manifest, obj["schema_version"], where)
            from .errors import CaptureRestoreError

            try:
                layout = layout_for(
                    manifest.history, manifest, schema, source_table,
                    obj["schema_version"],
                )
            except CaptureRestoreError as exc:
                raise CaptureArchiveError(f"{where}: {exc}") from exc
            pk = layout.source_pk
        else:
            pk = _source_pk(manifest, source_table, schema)
        key = _check_key(obj, "key", pk, where)

        has_before = "before" in obj
        has_after = "after" in obj
        if op == "insert":
            if has_before:
                raise CaptureArchiveError(f"{where}: insert must not carry before")
            if not has_after:
                raise CaptureArchiveError(f"{where}: insert requires after")
        elif op == "delete":
            if has_after:
                raise CaptureArchiveError(f"{where}: delete must not carry after")
            if not has_before:
                raise CaptureArchiveError(
                    f"{where}: delete requires before (an empty object is allowed)"
                )
        else:  # update
            if not has_before:
                raise CaptureArchiveError(
                    f"{where}: update requires before (an empty object is allowed)"
                )
            if not has_after:
                raise CaptureArchiveError(
                    f"{where}: update requires after (an empty object is still an update)"
                )
        for image_field in ("before", "after"):
            if image_field in obj:
                image = obj[image_field]
                if not isinstance(image, dict):
                    raise CaptureArchiveError(f"{where}: {image_field} must be an object")
        if manifest.evolving:
            _check_images_named_by_version(
                obj, layout.source_columns, obj["schema_version"], where
            )
        if op == "insert":
            if manifest.evolving:
                _check_complete_after_versioned(
                    obj["after"], obj["schema_version"], manifest, schema,
                    source_table, where,
                )
            else:
                _check_complete_after(obj["after"], manifest, source_table, where)
        if "new_key" in obj:
            if op != "update":
                raise CaptureArchiveError(f"{where}: new_key is only allowed on update")
            _check_key(obj, "new_key", pk, where)
        return obj

    # commit / abort share the transaction-end shape.
    allowed = set(ENVELOPE_FIELDS) | {"tx_id", "event_count", "table_counts"}
    if kind == "commit":
        allowed.add("commit_seq")
    elif "commit_seq" in obj:
        raise CaptureArchiveError(f"{where}: abort must not carry commit_seq")
    _reject_unknown(obj, allowed, where)
    _check_tx_id(obj, where)
    if "event_count" not in obj:
        raise CaptureArchiveError(f"{where}: missing event_count")
    if not _is_nonneg_int(obj["event_count"]):
        raise CaptureArchiveError(
            f"{where}: event_count must be a non-negative integer (booleans rejected)"
        )
    _check_table_counts(obj, where, manifest)
    if kind == "commit":
        if "commit_seq" not in obj:
            raise CaptureArchiveError(f"{where}: commit missing commit_seq")
        if not _is_nonneg_int(obj["commit_seq"]):
            raise CaptureArchiveError(
                f"{where}: commit_seq must be a non-negative integer (booleans rejected)"
            )
    elif "commit_seq" in obj:
        raise CaptureArchiveError(f"{where}: abort must not carry commit_seq")
    return obj


def _check_images_named_by_version(
    obj: dict, source_columns, schema_version: int, where: str
) -> None:
    """Every column named in an image must exist in the declared version.

    A message that references a column its version never had (for example an
    event written before a rename that names the *new* column) means the
    archive and its schema history disagree; the archive is rejected instead
    of silently treating the column as missing.
    """
    known = set(source_columns)
    for image_field in ("before", "after"):
        if image_field not in obj:
            continue
        unknown = set(obj[image_field]) - known
        if unknown:
            raise CaptureArchiveError(
                f"{where}: {image_field} names columns {sorted(unknown)} that "
                f"schema version {schema_version} does not define "
                f"(known: {sorted(known)})"
            )


def _check_declared_version(
    manifest: Manifest, schema_version: int, where: str
) -> None:
    """The declared version must exist in this archive's ordered history."""
    numbers = manifest.history.version_numbers() if manifest.history else []
    if schema_version not in numbers:
        raise CaptureArchiveError(
            f"{where}: schema_version {schema_version} is not declared by the "
            f"archive's schema history (known versions: {numbers})"
        )


def _check_complete_after_versioned(
    after: dict, schema_version: int, manifest: Manifest, schema: "Schema",
    source_table: str, where: str,
) -> None:
    """capture-v2 insert images are complete *in their declared version*.

    Columns added by a later schema version are not expected here: they are
    supplied from the declared backfill during conversion.
    """
    from .evolve import layout_for

    from .errors import CaptureRestoreError

    try:
        layout = layout_for(
            manifest.history, manifest, schema, source_table, schema_version
        )
    except CaptureRestoreError as exc:
        raise CaptureArchiveError(f"{where}: {exc}") from exc
    unknown = set(after) - set(layout.source_columns)
    if unknown:
        raise CaptureArchiveError(f"{where}: after has unknown columns {sorted(unknown)}")
    missing = set(layout.source_columns) - set(after)
    if missing:
        raise CaptureArchiveError(
            f"{where}: insert after must be complete for schema version "
            f"{schema_version}; missing columns {sorted(missing)}"
        )


def _check_complete_after(
    after: dict, manifest: Manifest, source_table: str, where: str
) -> None:
    """Insert ``after`` must name every source column (explicit null allowed)."""
    binding = manifest.bindings.tables[source_table]
    source_columns = set(binding.columns)
    unknown = set(after) - source_columns
    if unknown:
        raise CaptureArchiveError(f"{where}: after has unknown columns {sorted(unknown)}")
    missing = source_columns - set(after)
    if missing:
        raise CaptureArchiveError(
            f"{where}: insert after must be complete; missing columns {sorted(missing)}"
        )
