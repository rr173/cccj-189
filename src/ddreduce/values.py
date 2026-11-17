"""Typed scalar values, canonicalization and stable row identifiers.

Primary keys may be composite and carry explicit scalar types.  A stable
identifier of a row is therefore built from the table name and the ordered
(type, value) pairs of its primary key columns, serialized canonically so
that the same logical row always gets the same ``row_id`` regardless of which
candidate directory it is materialized into.
"""

from __future__ import annotations

import json
from typing import Any

SCALAR_TYPES = ("int", "float", "string", "bool", "null")

# Hard ceiling on individual scalar string values keeps hashing / materializing
# bounded for maliciously large inputs.
MAX_SCALAR_CHARS = 1_000_000


class SchemaError(ValueError):
    """Raised when a schema definition itself is malformed."""


class DataError(ValueError):
    """Raised when table data violates the schema or declared constraints."""


def normalize_scalar(value: Any, declared: str) -> Any:
    """Validate ``value`` against declared scalar type and return a normalized form.

    ``int`` only accepts Python ints (booleans are rejected); ``float`` accepts
    ints and floats and normalizes both to float; ``string`` requires str;
    ``bool`` requires bool; ``null`` requires None.
    """
    if declared == "int":
        if isinstance(value, bool) or not isinstance(value, int):
            raise DataError(f"expected int, got {type(value).__name__}: {value!r}")
        return value
    if declared == "float":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise DataError(f"expected float, got {type(value).__name__}: {value!r}")
        return float(value)
    if declared == "string":
        if not isinstance(value, str):
            raise DataError(f"expected string, got {type(value).__name__}: {value!r}")
        if len(value) > MAX_SCALAR_CHARS:
            raise DataError("string scalar exceeds maximum length")
        return value
    if declared == "bool":
        if not isinstance(value, bool):
            raise DataError(f"expected bool, got {type(value).__name__}: {value!r}")
        return value
    if declared == "null":
        if value is not None:
            raise DataError(f"expected null, got {value!r}")
        return None
    raise SchemaError(f"unknown scalar type: {declared!r}")


def canonical_scalar(value: Any, declared: str) -> str:
    """Stable textual encoding of a typed scalar (used inside row_id / hashes)."""
    normalized = normalize_scalar(value, declared)
    if normalized is None:
        return "null:"
    if declared == "int":
        return f"int:{normalized}"
    if declared == "float":
        # repr(float) round-trips in Python; normalize ints promoted to float.
        return f"float:{normalized!r}"
    if declared == "bool":
        return f"bool:{'1' if normalized else '0'}"
    return "str:" + json.dumps(normalized, ensure_ascii=False, sort_keys=True)


def canonical_json(obj: Any) -> str:
    """Deterministic JSON encoding used for cache keys and content hashes."""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def make_row_id(table: str, pk_columns: list[tuple[str, str]], row: dict[str, Any]) -> str:
    """Build the stable row id: ``table/<canonical PK>/...``."""
    parts = [table]
    for col, typ in pk_columns:
        if col not in row:
            raise DataError(f"table {table!r}: missing primary key column {col!r}")
        parts.append(canonical_scalar(row[col], typ))
    return "/".join(parts)


def encode_key_values(values: list[Any], types: list[str]) -> str:
    """Encode an ordered list of typed values (FK lookups, unique keys)."""
    return "|".join(canonical_scalar(v, t) for v, t in zip(values, types))
