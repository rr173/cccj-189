"""Independent offline ``capture-verify``.

The verifier never trusts the package's own "import succeeded" marker.  It
re-opens the raw archive, re-runs the whole scan/de-duplication/assembly and
re-restores the requested window itself, then compares every artifact of the
package against its own independent result:

* ``schema.json`` and ``snapshot/`` are compared against the schema and
  snapshot paths given on the command line;
* ``changes.jsonl`` is compared line-for-line against the independently
  restored full-image transactions;
* ``capture-map.jsonl`` is rebuilt from the spill database (all transport
  coordinates included) and compared entry-for-entry;
* ``capture-report.json`` bindings, window, hashes and counts are recomputed.

Three failure classes stay distinct in the report:

* ``ARCHIVE_INVALID``  - the source archive itself is incomplete/conflicting;
* ``RESTORE_FAILED``   - a valid archive cannot be restored against the
  snapshot/schema (stale image, key collision, commit constraint, ...);
* ``PACKAGE_MISMATCH`` - restoration succeeds but the package differs.

The verdict strings are deliberately not oracle-style PASS/FAIL: this tool
never runs an oracle.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..model import Database
from ..schema import load_schema
from ..values import canonical_json
from .errors import CaptureArchiveError, CaptureError, CapturePackageError, CaptureRestoreError
from .importer import (
    MAP_FORMAT,
    PACKAGE_FILES,
    REPORT_FORMAT,
    archive_hash,
    hash_bytes,
    hash_file,
)
from .manifest import load_manifest
from .restore import Restorer
from .store import CaptureStore, Coordinate

VERDICT_MATCH = "CAPTURE_PACKAGE_MATCH"
VERDICT_ARCHIVE = "ARCHIVE_INVALID"
VERDICT_RESTORE = "RESTORE_FAILED"
VERDICT_PACKAGE = "PACKAGE_MISMATCH"


@dataclass
class VerificationResult:
    verdict: str
    failures: list[dict[str, str]] = field(default_factory=list)
    report: dict[str, Any] | None = None

    def add(self, category: str, detail: str, **locator: Any) -> None:
        entry = {"category": category, "detail": detail}
        entry.update(locator)
        self.failures.append(entry)


def _read_jsonl(path: Path) -> list[Any]:
    if not path.is_file():
        raise CapturePackageError(f"required package file missing: {path}")
    rows = []
    with open(path, "r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise CapturePackageError(
                    f"{path.name}:{line_no}: invalid JSON ({exc})"
                ) from exc
    return rows


def _canonical_line(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


def _coords(coordinates: tuple[Coordinate, ...]) -> list[dict[str, Any]]:
    return [c.to_dict() for c in coordinates]


def _rebuild_map_and_changes(
    manifest, schema, snapshot, store, through: int
) -> tuple[list[str], list[str], dict[str, int]]:
    """Independent restoration; returns (changes lines, map lines, counters)."""
    restorer = Restorer(manifest, schema, list(snapshot.rows.values()))
    changes_lines: list[str] = []
    map_lines: list[str] = []
    counters = {
        "out_of_window_commits": 0,
        "in_window_zero_event_commits": 0,
        "exported_transactions": 0,
        "exported_events": 0,
        "key_move_expansions": 0,
    }
    output_position = -1
    for end in store.committed_transactions():
        seq = end.commit_seq
        in_window = manifest.snapshot_commit_seq < seq <= through
        events = store.logical_events(end.tx_id)
        if not in_window:
            counters["out_of_window_commits"] += 1
            continue
        if not events:
            counters["in_window_zero_event_commits"] += 1
            map_lines.append(_canonical_line({
                "format": MAP_FORMAT,
                "tx_id": end.tx_id,
                "commit_seq": seq,
                "zero_event_commit": True,
                "end_coordinates": _coords(end.coordinates),
            }))
            continue

        output_events = restorer.apply_transaction(end.tx_id, events)
        output_position += 1
        clean_events = []
        for ev in output_events:
            obj = {"op": ev.op, "table": ev.table, "key": ev.key}
            if ev.before is not None:
                obj["before"] = ev.before
            if ev.after is not None:
                obj["after"] = ev.after
            clean_events.append(obj)
        changes_lines.append(
            _canonical_line({"tx_id": end.tx_id, "events": clean_events})
        )
        counters["exported_transactions"] += 1
        counters["exported_events"] += len(output_events)
        for ev_idx, output_event in enumerate(output_events):
            src = output_event.source
            map_lines.append(_canonical_line({
                "format": MAP_FORMAT,
                "output_tx_position": output_position,
                "tx_id": end.tx_id,
                "commit_seq": seq,
                "output_event_index": ev_idx,
                "output_op": output_event.op,
                "output_table": output_event.table,
                "output_key": output_event.key,
                "source": {
                    "tx_id": src.tx_id,
                    "tx_order": src.tx_order,
                    "source_table": src.source_table,
                    "source_op": src.op,
                    "expanded_from_key_move": src.expanded_from_key_move,
                    "coordinates": _coords(src.coordinates),
                },
                "end_coordinates": _coords(end.coordinates),
            }))
            if src.expanded_from_key_move:
                counters["key_move_expansions"] += 1
    return changes_lines, map_lines, counters


def _expected_report(
    manifest, schema, snapshot, store, archive: Path, snapshot_dir: Path,
    schema_path: Path, package_dir: Path, through: int,
    changes_lines: list[str], map_lines: list[str], counters: dict,
) -> dict[str, Any]:
    return {
        "format": REPORT_FORMAT,
        "archive": {
            "path": str(Path(archive)),
            "source_id": manifest.source_id,
            "version": manifest.version,
            "archive_hash": archive_hash(manifest, archive),
            "manifest_hash": hash_bytes(manifest.canonical_bytes()),
            "files": list(manifest.files),
            "streams": [
                {"name": s.name, "first_offset": s.first_offset,
                 "last_offset": s.last_offset}
                for s in manifest.streams
            ],
        },
        "inputs": {
            "snapshot_dir": str(Path(snapshot_dir)),
            "snapshot_hash": snapshot.content_hash(),
            "schema_path": str(Path(schema_path)),
            "schema_hash": hash_bytes(
                canonical_json(schema.canonical()).encode("utf-8")
            ),
        },
        "bindings": manifest.raw["bindings"],
        "window": {
            "snapshot_commit_seq": manifest.snapshot_commit_seq,
            "sealed_commit_seq": manifest.sealed_commit_seq,
            "through": through,
            "export_rule": "snapshot_commit_seq < commit_seq <= through",
        },
        "counts": {
            "files_read": store.metrics.files_read,
            "physical_messages_read": store.metrics.physical_messages_read,
            "physical_duplicates": store.metrics.physical_duplicates,
            "logical_event_duplicates": store.metrics.logical_event_duplicates,
            "end_duplicates": store.metrics.end_duplicates,
            "heartbeats": store.metrics.heartbeats,
            "transactions_total": store.assembly.transactions,
            "committed_total": store.assembly.committed,
            "aborted_total": store.assembly.aborted,
            "aborted_events": store.assembly.aborted_events,
            "zero_event_commits_total": store.assembly.zero_event_commits,
            "out_of_window_commits": counters["out_of_window_commits"],
            "in_window_zero_event_commits":
                counters["in_window_zero_event_commits"],
            "exported_transactions": counters["exported_transactions"],
            "exported_events": counters["exported_events"],
            "key_move_expansions": counters["key_move_expansions"],
        },
        "output": {
            "directory": str(Path(package_dir)),
            "changes_hash": hash_bytes(
                ("".join(line + "\n" for line in changes_lines)).encode("utf-8")
            ),
            "capture_map_hash": hash_bytes(
                ("".join(line + "\n" for line in map_lines)).encode("utf-8")
            ),
        },
    }


def _normalize_expected_report(expected: dict, actual: dict) -> dict:
    """Drop/replace fields that legitimately differ across machines.

    Paths are compared by resolved realpath; resource measurements are
    recomputed structurally (spill sizes/timings are machine-specific and only
    checked for presence/shape).
    """
    out = json.loads(json.dumps(expected, ensure_ascii=False))
    for section in ("archive", "inputs"):
        for key in list(out.get(section, {})):
            if key.endswith("_path") or key in ("path", "snapshot_dir"):
                out[section][key] = _resolve(actual.get(section, {}).get(key))
    out["output"]["directory"] = _resolve(actual.get("output", {}).get("directory"))
    out.pop("resources", None)
    return out


def _resolve(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return str(Path(value).resolve())
    except OSError:
        return value


def _resolve_report_paths(report: dict) -> dict:
    out = json.loads(json.dumps(report, ensure_ascii=False))
    for section in ("archive", "inputs"):
        for key in list(out.get(section, {})):
            if key.endswith("_path") or key in ("path", "snapshot_dir"):
                out[section][key] = _resolve(out[section][key])
    if "output" in out and "directory" in out["output"]:
        out["output"]["directory"] = _resolve(out["output"]["directory"])
    out.pop("resources", None)
    return out


def _base_report(
    result: VerificationResult,
    archive: Path, snapshot_dir: Path, schema_path: Path,
    package_dir: Path, through: int,
) -> dict[str, Any]:
    return {
        "verdict": result.verdict,
        "archive": str(archive),
        "snapshot_dir": str(snapshot_dir),
        "schema_path": str(schema_path),
        "package_dir": str(package_dir),
        "through": through,
        "failures": result.failures,
    }


def run_verify(
    archive: str | Path,
    snapshot_dir: str | Path,
    schema_path: str | Path,
    through: int,
    package_dir: str | Path,
    *,
    tmp_root: str | Path | None = None,
) -> VerificationResult:
    archive = Path(archive)
    snapshot_dir = Path(snapshot_dir)
    schema_path = Path(schema_path)
    package_dir = Path(package_dir)
    result = VerificationResult(VERDICT_MATCH)

    # Record structural package-layout problems but do NOT return early: the
    # source archive must still be scanned and restored so the report can
    # distinguish ARCHIVE_INVALID / RESTORE_FAILED from a mere package
    # mismatch.  Artifact comparison is skipped when the layout is broken.
    layout_broken = False
    if not package_dir.is_dir():
        result.add(VERDICT_PACKAGE,
                   f"package directory not found: {package_dir}")
        layout_broken = True
    else:
        present = {p.name for p in package_dir.iterdir()}
        extra = present - set(PACKAGE_FILES)
        missing = set(PACKAGE_FILES) - present
        if extra:
            result.add(VERDICT_PACKAGE,
                       f"unexpected package files {sorted(extra)}")
        if missing:
            result.add(VERDICT_PACKAGE,
                       f"missing package files {sorted(missing)}")
        layout_broken = bool(extra or missing)

    try:
        schema = load_schema(schema_path)
        manifest = load_manifest(archive, schema)
        if isinstance(through, bool) or not isinstance(through, int) or through < 0:
            raise CaptureError("--through must be a non-negative integer")
        if not (manifest.snapshot_commit_seq <= through <= manifest.sealed_commit_seq):
            raise CaptureError(
                f"--through {through} outside the archive window "
                f"[{manifest.snapshot_commit_seq}, {manifest.sealed_commit_seq}]"
            )
        snapshot = Database.load(schema, snapshot_dir)
        with CaptureStore(manifest, schema, archive, tmp_root=tmp_root) as store:
            store.scan()
            store.assemble()
            changes_lines, map_lines, counters = _rebuild_map_and_changes(
                manifest, schema, snapshot, store, through
            )
            expected = _expected_report(
                manifest, schema, snapshot, store, archive, snapshot_dir,
                schema_path, package_dir, through, changes_lines, map_lines,
                counters,
            )
    except CaptureArchiveError as exc:
        result.verdict = VERDICT_ARCHIVE
        result.add(VERDICT_ARCHIVE, str(exc))
        result.report = _base_report(
            result, archive, snapshot_dir, schema_path, package_dir, through
        )
        return result
    except CaptureRestoreError as exc:
        result.verdict = VERDICT_RESTORE
        result.add(VERDICT_RESTORE, str(exc))
        result.report = _base_report(
            result, archive, snapshot_dir, schema_path, package_dir, through
        )
        return result
    except CaptureError as exc:
        result.add(VERDICT_PACKAGE, str(exc))
        result.verdict = VERDICT_PACKAGE
        result.report = _base_report(
            result, archive, snapshot_dir, schema_path, package_dir, through
        )
        return result

    # The archive restores cleanly.  Compare the package artifacts only when
    # its five-part layout is actually present.
    if not layout_broken:
        _compare_static_artifacts(result, schema_path, snapshot_dir, package_dir)
        _compare_jsonl(result, package_dir / "changes.jsonl", changes_lines,
                       "changes.jsonl", kind="changes")
        _compare_jsonl(result, package_dir / "capture-map.jsonl", map_lines,
                       "capture-map.jsonl", kind="map")
        _compare_report(result, package_dir / "capture-report.json", expected)

    if result.failures:
        kinds = {f["category"] for f in result.failures}
        if VERDICT_ARCHIVE in kinds:
            result.verdict = VERDICT_ARCHIVE
        elif VERDICT_RESTORE in kinds:
            result.verdict = VERDICT_RESTORE
        else:
            result.verdict = VERDICT_PACKAGE
    result.report = {
        "verdict": result.verdict,
        "archive": str(archive),
        "snapshot_dir": str(snapshot_dir),
        "schema_path": str(schema_path),
        "package_dir": str(package_dir),
        "through": through,
        "failures": result.failures,
        "independent_counts": expected["counts"],
    }
    return result


def _compare_static_artifacts(
    result: VerificationResult, schema_path: Path, snapshot_dir: Path,
    package_dir: Path,
) -> None:
    # The importer copies the schema text verbatim, only guaranteeing a
    # trailing newline; compare parsed JSON content rather than raw bytes.
    pkg_schema = package_dir / "schema.json"
    try:
        want_schema = json.loads(schema_path.read_text(encoding="utf-8"))
        got_schema = json.loads(pkg_schema.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        result.add(VERDICT_PACKAGE, f"schema.json unreadable: {exc}",
                   file="schema.json")
        return
    if got_schema != want_schema:
        result.add(
            VERDICT_PACKAGE,
            "schema.json content differs from the schema given to capture-verify",
            file="schema.json",
        )
    pkg_snapshot = package_dir / "snapshot"
    if not pkg_snapshot.is_dir():
        result.add(VERDICT_PACKAGE, "package snapshot/ is not a directory")
        return
    pkg_files = {p.name for p in pkg_snapshot.iterdir() if p.is_file()}
    src_files = {p.name for p in snapshot_dir.iterdir() if p.is_file()}
    if pkg_files != src_files:
        result.add(
            VERDICT_PACKAGE,
            f"snapshot file set differs: package {sorted(pkg_files)} vs "
            f"source {sorted(src_files)}",
            file="snapshot/",
        )
        return
    for name in sorted(pkg_files):
        if hash_file(pkg_snapshot / name) != hash_file(snapshot_dir / name):
            result.add(
                VERDICT_PACKAGE,
                f"snapshot table {name!r} content differs from the source snapshot",
                file=f"snapshot/{name}",
            )


def _compare_jsonl(
    result: VerificationResult, path: Path, expected_lines: list[str],
    label: str, *, kind: str,
) -> None:
    try:
        actual = _read_jsonl(path)
    except CapturePackageError as exc:
        result.add(VERDICT_PACKAGE, str(exc), file=label)
        return
    actual_lines = [_canonical_line(obj) for obj in actual]
    if len(actual_lines) != len(expected_lines):
        result.add(
            VERDICT_PACKAGE,
            f"{label}: line count {len(actual_lines)} differs from the independent "
            f"restoration {len(expected_lines)}",
            file=label,
        )
        return
    for idx, (got, want) in enumerate(zip(actual_lines, expected_lines)):
        if got != want:
            result.add(
                VERDICT_PACKAGE,
                f"{label}: line {idx} differs from the independent restoration",
                file=label, line=idx,
            )


def _compare_report(
    result: VerificationResult, path: Path, expected: dict
) -> None:
    try:
        actual = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        result.add(VERDICT_PACKAGE, f"capture-report.json unreadable: {exc}")
        return
    if not isinstance(actual, dict):
        result.add(VERDICT_PACKAGE, "capture-report.json must be an object")
        return
    if actual.get("format") != REPORT_FORMAT:
        result.add(
            VERDICT_PACKAGE,
            f"report format {actual.get('format')!r} != {REPORT_FORMAT!r}",
        )
    want = _normalize_expected_report(expected, actual)
    got = _resolve_report_paths(actual)
    # Compare section by section so tampering is located precisely.
    for section in ("archive", "inputs", "bindings", "window", "counts", "output"):
        if got.get(section) != want.get(section):
            result.add(
                VERDICT_PACKAGE,
                f"capture-report.json section {section!r} was modified or is wrong",
                file="capture-report.json", section=section,
                expected=want.get(section), actual=got.get(section),
            )
    resources = actual.get("resources")
    if resources is not None and (
        not isinstance(resources, dict)
        or "peak_spill_bytes" not in resources
        or "elapsed_seconds" not in resources
    ):
        result.add(
            VERDICT_PACKAGE,
            "capture-report.json resources section has an unexpected shape",
            file="capture-report.json",
        )
