# ddreduce — relational failure-input reducer

A failure-input minimizer for **relational JSONL data** and for **CDC
transaction histories**. Given a directory of JSONL tables, a schema, and an
external "oracle" command that reports `PASS` or a signed `FAIL`, `ddreduce`
repeatedly tries **smaller but still referentially valid** relational datasets
until it finds a small package that still triggers the *same* fault signature.

In **tables mode** the reducer deletes rows. In **log mode** (`log-reduce`)
the input is a fixed starting snapshot plus a `changes.jsonl` change log of
committed transactions, and the reducer deletes *transactions* from the
history — some incremental-warehouse faults only appear while a row is
overwritten, deleted and rebuilt under the same key, even though the final
table looks perfectly normal.

The target fault is determined entirely by **actually running the command
under test** — the tool never assumes which rows or transactions are relevant.

---

## 1. Concepts and contract

### Inputs

| Input | Contents |
| --- | --- |
| `--input DIR` | One `<table>.jsonl` file per schema table (one JSON object per line). |
| `--schema FILE` | Typed columns, composite typed primary keys, unique keys, foreign keys. |
| `--oracle-config FILE` | Command argv, per-call timeout, retest/confirm counts, total call budget. |
| `--workdir DIR` | All tool state: cache, resumable progress, trajectory, result, certificate. |

Rows carry stable `table/row_id` identifiers. Primary keys may be **composite**
and use typed scalars (`int`, `float`, `string`, `bool`). Foreign keys may form
**cycles**; FK nullability is explicit. Inputs must first pass unique-key and
foreign-key validation or the tool refuses to run.

### The oracle report contract

The command receives a fresh candidate directory and must write a report JSON:

```json
{ "status": "PASS" }
{ "status": "FAIL", "signature": "E_ZERO_PRICE", "...": "..." }
```

Verdict classification is strict:

- clean exit + `PASS` report → **PASS**
- clean exit + `FAIL` report with a string `signature` → **FAIL** (signature kept)
- non-zero exit, timeout, missing/malformed report, or PASS/FAIL or signature
  disagreement across repeated runs → **UNRESOLVED**

A **timeout is never treated as a FAIL**. `UNRESOLVED` neither proves the fault
is retained nor that it disappeared, so it blocks any minimality certificate.

### Legal deletion operation

A legal deletion removes chosen seed rows, then **cascades** every child that
loses a *required* parent (a non-nullable foreign key), iterating to a
fixpoint:

- nullable foreign keys create **no** hard dependency (the child may stay with
  a dangling reference — the tool only deletes rows, it never edits columns);
- cyclic foreign keys are removed as a **whole closure** (reverse-edge
  reachability), so deleting any cycle member removes every dependent member;
- only rows are deleted; fields and constraints are never modified.

### What "1-minimal" means

A certified package `D` is **1-minimal** when, for every row `r` in `D`,
letting `D' = D \ deletion_closure({r})`, either:

1. `D'` is identical to `D`, or
2. the oracle returns **PASS** on `D'`.

If any strictly smaller neighbor is `UNRESOLVED`, untested (budget), or still a
target `FAIL`, **no certificate is issued**. This makes no claim of a global
row-count optimum. The empty database is tested under the same contract and, if
it reproduces the target fault, is itself 1-minimal.

### Controlled subprocess execution

Every oracle invocation:

- materializes a **brand-new, unique candidate directory** that is never
  reused while a process may still read it, and is removed after the process
  exits and its pipes are drained;
- runs in its **own process group/session**; on timeout the entire group is
  `SIGKILL`ed (including forked grandchildren) and reaped, so nothing leaks
  into the next call;
- captures bounded `stdout`, `stderr`, exit status, duration, and the report.

### Cache, budget and resumability

- The cache key binds **full candidate content + schema + command + oracle
  config**; changing any of them changes the key. Cache entries store only raw
  run signals (not candidate copies), are written atomically, and a corrupt
  entry degrades to a miss — never a fabricated answer.
- `calls_used` counts only real subprocess invocations; **cache hits are
  free** and are never re-charged.
- Progress (`state.json`) is atomically replaced; `trajectory.jsonl` appends
  every accepted/rejected decision with call counts and cache-hit flags.
- On `SIGINT`/`SIGTERM` the search stops at a safe boundary. An in-flight call
  is recorded as unknown (never faked). `resume` continues the search and the
  remaining budget, reusing confirmed calls; it refuses if the input or oracle
  behavior changed since the search began.
- Tie-breaking is fully deterministic: rows are ordered lexicographically by
  stable `row_id`; chunks follow that fixed order; no randomness is used.

---

## 2. Commands

```bash
# Start a reduction (or continue one already present in --workdir)
ddreduce reduce \
  --input demo/data/tables \
  --schema demo/data/schema.json \
  --oracle-config demo/config/oracle.json \
  --workdir runs/demo

# Resume an interrupted / budget-exhausted search; optionally raise the budget
ddreduce resume --workdir runs/demo [--max-calls 500]

# Independently verify a certificate (does not trust the searcher's flags)
ddreduce verify \
  --input demo/data/tables \
  --schema demo/data/schema.json \
  --oracle-config demo/config/oracle.json \
  --workdir runs/demo
```

Per-run limits may be overridden on the command line:
`--timeout`, `--retest-runs`, `--confirm-runs`, `--max-calls`.

### Exit codes

| Code | Meaning |
| --- | --- |
| 0 | `CERTIFIED` (reduce/resume) or `VERIFIED_MINIMAL` (verify) |
| 2 | baseline rejected, certificate rejected, or a non-minimal/changed neighbor |
| 3 | call budget exhausted |
| 4 | interrupted (state saved; run `resume`) |
| 1 | bad arguments, invalid schema/data, or other tool error |

### `verify` outcomes (three distinct results)

- `VERIFIED_MINIMAL` — package constraints valid, target fault reproduced, and
  **every** strict single-row deletion-closure neighbor independently re-tested
  as PASS or identical;
- `UNKNOWN_NEIGHBORS` — at least one neighbor is `UNRESOLVED` or still FAILs;
- `BUDGET_EXHAUSTED` — the budget ran out before all neighbors were tested.

`verify` re-derives every neighbor itself and never reads a "minimal" boolean
from the searcher.

---

## 3. Oracle configuration

```json
{
  "command": [
    "python3", "-I",
    "demo/transform/etl_quality_gate.py",
    "{candidate_dir}",
    "{report_file}"
  ],
  "timeout_seconds": 20.0,
  "retest_runs": 1,
  "confirm_runs": 2,
  "max_calls": 100
}
```

- `{candidate_dir}` and `{report_file}` are substituted per invocation.
- Relative script paths in `command` are resolved against the launcher's
  working directory (the oracle itself runs with cwd = candidate dir).
- `confirm_runs` (≥ 2) repetitions must all yield the same FAIL signature on
  the original input, or the run is rejected before any shrinking starts.
- `retest_runs` repetitions are used for each candidate during reduction.

---

## 4. Included demo — a real multi-table transformation defect

`demo/transform/etl_quality_gate.py` is a genuine five-table order-to-invoice
ETL with an embedded data-quality gate. It joins
`addresses → customers → orders → order_lines ← products`, emits invoice
JSONL, and reports quality violations with stable code-based signatures.

The demonstrated defect: the zero-price gate (`E_ZERO_PRICE`) fires on an order
line whose catalog product has price `0`. The demo database contains unrelated
customers, products and orders; the reducer shrinks all 15 rows down to the
minimal fault-reproducing closure of 4 rows:

```
products/P2  (the zero-price product)
order_lines/L2 (the line referencing it)
orders/O2, customers/C2 (the required-FK chain)
```

The nullable-FK parent `addresses/A2` is correctly **not** retained, because a
nullable foreign key creates no hard dependency.

Run it:

```bash
PYTHONPATH=src python3 -P -m ddreduce.cli reduce \
  --input demo/data/tables --schema demo/data/schema.json \
  --oracle-config demo/config/oracle.json --workdir /tmp/demo-run
PYTHONPATH=src python3 -P -m ddreduce.cli verify \
  --input demo/data/tables --schema demo/data/schema.json \
  --oracle-config demo/config/oracle.json --workdir /tmp/demo-run
```

The reduced package in `/tmp/demo-run/result` is a standalone valid input that
still fails with signature `E_ZERO_PRICE`.

---

## 5. CDC transaction-history reduction (log mode)

Some incremental-warehouse faults only exist in the *history*: a row is
overwritten, deleted, and rebuilt under the same key, and the final table
looks completely normal. Log mode reduces that history itself — it never
compresses the log into the final table, and the starting snapshot is never
shrunk.

### The change-log contract (`changes.jsonl`)

Each line is one **committed transaction**; file order is business order
(transactions are never re-sorted by `tx_id`):

```json
{"tx_id": "t3", "events": [{"op": "delete", "table": "orders", "key": {"order_id": "O4"}, "before": {"order_id": "O4", "status": "open", "amount": 75.0}}]}
```

- `tx_id` is unique; `events` is non-empty and executes in array order.
- Every event has `op`, `table`, `key`. `insert` carries a complete `after`
  row; `delete` a complete `before` row; `update` both, and never changes the
  primary key. `key` must agree with the row image.
- Columns, types, nullability and key constraints follow the same schema as
  tables mode.
- Per event, only row structure and primary-key presence are checked.
  Cross-row uniqueness and foreign keys are checked **when the transaction
  commits** — so one transaction may swap two rows' unique values, insert a
  child before its parent, or form an FK cycle. Any commit-state violation
  fails the whole transaction; no partial event is ever committed.
- If the *original* log is illegal, the tool names the transaction and event
  position and refuses to start the oracle command at all.

### Versions, absences and witnesses

Every write in the original log creates a new row **version** — writing back
an identical value still creates a new version that cannot impersonate the
old one. Every delete creates a new **absence source** for its key; the
initial absence of a key is a different source. Identities are fixed by the
baseline or by the original transaction/event position and are never
renumbered per candidate.

From the full original log the tool derives, per transaction: the exact
version (update/delete) or absence source (insert) each event read, and — for
each inserted/updated row surviving at commit — the version of every
*required* (non-nullable) foreign-key parent at commit time. Nullable FKs
keep the tables-mode semantics: no hard dependency.

### Normalized replay of a deletion selection

A deletion request selects original transactions. Replaying the rest **in
original order from the fixed snapshot**: a transaction is kept only if every
event reads the identical version/absence as witnessed, every required parent
version at commit matches the witness, and all event and commit constraints
hold — otherwise the whole transaction is skipped and its private writes stay
invisible to later transactions. Missing premises are never rebound to
same-key or same-value records; events are never inserted, rewritten, split,
or reordered. The result is the unique normalized candidate for that
selection: replaying the same selection twice yields the same transaction
subsequence and data state, including the empty log when every transaction is
deleted.

### Log-mode commands

```bash
# Inspect the replay (and skip reasons) without running the fault command
ddreduce log-replay --snapshot DIR --schema FILE --changes FILE \
  [--selection keep-or-drop.json] [--out DIR] [--report FILE]

# Reduce the history
ddreduce log-reduce --snapshot DIR --schema FILE --changes FILE \
  --oracle-config FILE --workdir DIR

# Independently verify a history certificate
ddreduce log-verify --workdir DIR --oracle-config FILE [--max-calls N]

# resume auto-detects tables vs log mode from the workdir state
ddreduce resume --workdir DIR [--max-calls N]
```

A log candidate handed to the oracle is a self-contained directory:

```
<candidate>/snapshot/<table>.jsonl   the fixed starting snapshot
<candidate>/schema.json              the schema (copied verbatim)
<candidate>/changes.jsonl            the normalized candidate log
```

The incremental command under test must re-consume the candidate log from the
snapshot on every call. The delivered result package additionally keeps
`evidence/original-changes.jsonl` (the untouched original log) and
`evidence/selection.json` (the exact original transaction selection) for
causal verification; the oracle never reads the evidence files.

### Log-mode reduction and certification

The search first tries **batch transaction deletion** (ddmin chunks), then
tries deleting each remaining transaction individually, normalizing and
re-testing every time. A result is certified **1-minimal by transaction
deletion** only when the result itself stably triggers the target fault and
every strictly shorter single-transaction-deletion neighbor reports PASS —
neighbors that still FAIL, are UNRESOLVED, or are untested block the
certificate. `log-reduce` reports budget exhaustion (exit 3), blocked/unknown
results (exit 2) and certified results (exit 0) as distinct outcomes.

`log-verify` independently re-derives every version witness from the
package's own original log, confirms the candidate is exactly the normalized
result of the declared selection, recomputes every deletion neighbor and
re-runs the fault command. It trusts nothing saved in the certificate
(dependency edges, final-state hashes, tested booleans are all recomputed)
and rejects tampered logs, transaction boundaries, or certificate references
with `REJECTED`. Its outcomes are `VERIFIED_MINIMAL`, `UNKNOWN_NEIGHBORS`,
`BUDGET_EXHAUSTED`, `REJECTED`.

### History-bound cache, progress and certificates

History is part of the oracle input: two logs that produce the same final
table never share a cache entry. Cache keys, progress state and certificates
bind the fixed snapshot, the original log's ordered content and transaction
boundaries, the schema, and the oracle behavior configuration. Raising the
total budget on `resume` continues the search; changing the original log, the
schema, the snapshot, or the behavior configuration while paused refuses to
reuse old conclusions. The trajectory records real subprocess calls, cache
hits, and accept/skip reasons; an interrupted in-flight call is never
converted into a fabricated result.

Tables-mode certificates keep their existing meaning (`"mode": "tables"`);
log-mode certificates carry `"mode": "log"` and are verified only by
`log-verify`.

### Included CDC demo — a real incremental aggregation defect

`demo/incremental/incr_order_total.py` consumes a CDC package and computes
order-amount totals per status two ways: batch (apply the log, aggregate the
final table) and incremental (maintain aggregates event by event). The
incremental path keeps a per-key image cache that is **not invalidated on
delete**, so a same-key rebuild after a delete subtracts the stale amount a
second time and the incremental total diverges from the batch reference. The
FAIL signature (`E_INCR_DIVERGE:<status>`) is derived from the actual
diverging groups — never from transaction ids or candidate lengths.

The demo history (`demo/incremental/data/changes.jsonl`) contains overwrite
updates, a delete, a same-key rebuild and unrelated inserts; the reducer
shrinks it to the minimal delete + rebuild pair `[t3, t4]`:

```bash
PYTHONPATH=src python3 -P -m ddreduce.cli log-reduce \
  --snapshot demo/incremental/data/snapshot \
  --schema demo/incremental/data/schema.json \
  --changes demo/incremental/data/changes.jsonl \
  --oracle-config demo/incremental/config/oracle.json \
  --workdir /tmp/demo-log
PYTHONPATH=src python3 -P -m ddreduce.cli log-verify \
  --workdir /tmp/demo-log \
  --oracle-config demo/incremental/config/oracle.json
# The package runs standalone, without the original input paths:
python3 -I demo/incremental/incr_order_total.py /tmp/demo-log/result /tmp/report.json
```

`demo/incremental/gen_log.py` deterministically generates longer logs for
experiments (`--out DIR --tx 400 --keys 120 --seed 7`); generated data is a
runtime artifact and is not committed.

---

## 6. Offline capture ingestion (`capture-import` / `capture-verify`)

Operations sometimes receives a **partitioned capture archive** rather than a
ready-made full-image log: one transaction's row changes are scattered across
several JSONL shards, retransmitted records appear more than once, and update
messages usually name only the columns that changed.  Hand-assembling the
`changes.jsonl` the log tools consume is exactly how fault history gets
silently rewritten.

The two offline commands below restore such an archive into the existing
full-image input package without adding any reduction strategy and without
touching the existing searchers. **Neither command ever runs an oracle**; the
result is handed to the normal `log-replay` / `log-reduce` afterwards.

Two archive formats are accepted:

* **capture-v1** - one fixed schema plus table/column bindings (the existing
  format; its inputs, commands and package format are unchanged);
* **capture-v2** - an *ordered schema history* with stable field IDs, for
  archives that contain events written before and after an upstream rename or
  column addition.

### The capture-v1 format

An archive is a plain directory holding one `capture.json` manifest and the
regular JSONL shard files it declares (no symlinks, no escaping paths, no
undeclared files):

```json
{
  "version": 1,
  "source_id": "src-hand-1",
  "snapshot_commit_seq": 12,
  "sealed_commit_seq": 14,
  "streams": [{"name": "s1", "first_offset": 0, "last_offset": 11}],
  "files": ["shards/s1-00.jsonl", "shards/s1-01.jsonl"],
  "bindings": {
    "tables": {
      "accounts": {
        "target": "accounts",
        "columns": {"key": "key", "amount": "amount", "note": "note"}
      }
    }
  }
}
```

* `version` must be `1`; any other value is refused (version `2` selects the
  schema-history format described below).
* `snapshot_commit_seq` is the last commit point already contained in the
  starting snapshot; `sealed_commit_seq` is the high-water mark the archive
  promises to have sealed.  Gaps between commit sequence numbers are allowed.
* Every `streams` entry names a closed integer interval
  `[first_offset, last_offset]`; **every** integer position in it must contain
  exactly one unique physical message, or the archive is rejected.
* `bindings` maps source table/column names onto the target schema.  Tables
  and columns are one-to-one and must cover **every** target table and
  column - no filtering, no implicit column drops.  Source names may differ
  completely from target names.

Every shard line is one message with the shared envelope
`source_id`, `stream`, `offset`, `kind`.  `source_id` must equal the
manifest; `offset` is a non-negative integer (booleans are never accepted as
numbers); `kind` is one of `change`, `commit`, `abort`, `heartbeat`.

| kind | extra fields |
| --- | --- |
| `change` | non-empty string `tx_id`, integer `tx_order` ≥ 1, source `table`, `op` (insert/update/delete), full primary-key `key`, and the op-specific `before`/`after`; updates may additionally carry a full `new_key` |
| `commit` | `tx_id`, non-negative `commit_seq`, non-negative `event_count`, `table_counts` |
| `abort` | `tx_id`, `event_count`, `table_counts` (no `commit_seq`) |
| `heartbeat` | envelope only; never part of a transaction |

`table_counts` maps source table to a positive logical row-change count; the
sum of its positive entries equals `event_count`, and a zero-event
transaction uses an empty object `{}`.  Missing fields, unknown fields and
booleans impersonating integers are all rejected.

Image rules on `change` messages:

* `insert` carries a **complete** `after` (every source column present,
  explicit null allowed) and no `before`; the key must currently be absent.
* `delete` carries a partial `before` (the empty object is allowed); the
  importer completes the output before-image from the current full row.
* `update` carries partial `before` and partial `after`.  A column named in
  `before` is asserted against the current row; columns named in `after`
  overwrite it.  A **missing column means preserve**; an explicit JSON
  `null` means *write null* - the two are never confused.  An empty `after`
  is still an update and is never dropped.
* An update normally keeps its primary key.  An update may instead carry a
  complete `new_key`; when the key actually changes the importer expands the
  event, at its original position, into an adjacent `delete` of the old row
  and `insert` of the new row, keeping both full images.  A new key that
  already exists is refused (no overwrite); `new_key` on any other op is
  refused.

### De-duplication and closure semantics

File order and in-file message order are transport order with no business
meaning.

* The same `(stream, offset)` repeated with identical canonical content
  (formatting/key-order differences ignored) merges as a physical
  retransmission; different content at one coordinate is a hard conflict.
* The same `(tx_id, tx_order)` may be delivered at several coordinates: it is
  one logical change when the business content (coordinates stripped) is
  identical, and **all** coordinates are retained for provenance; different
  content is a conflict - one variant is never picked arbitrarily.
* Transaction end decisions follow the same rule; a commit and an abort for
  the same transaction, or a retransmitted commit whose content changed, are
  conflicts.
* The archive is closed: every transaction with changes has exactly one
  consistent end decision, `tx_order` runs exactly `1..event_count`, and
  `table_counts` match the logical events.  Commit sequence numbers are
  unique across committed transactions (gaps allowed).  Aborted events pass
  all structural, completeness and de-duplication checks but are never
  applied to row state.
* Coordinate conflicts and coverage gaps are rejected even when the affected
  messages fall outside the requested export window.

### Restoration window and output

`capture-import --through C` exports only committed transactions with
`snapshot_commit_seq < commit_seq <= C`, where
`snapshot_commit_seq <= C <= sealed_commit_seq`.  Earlier and later
transactions still participate in every archive integrity check, but their
row images never touch the restored state of the export window.  Transactions
are ordered by `commit_seq` - never by offset, file name or `tx_id`.  A
non-empty transaction, even when its events span partitions, produces exactly
one `changes.jsonl` line.  Zero-event commits are checked and counted, keep
their end message and coordinates in the map, but produce no empty events log
line (the old log format rejects those).

Cross-row unique and foreign-key constraints may temporarily fail between a
transaction's own events; they are judged only at commit, reusing the
existing schema semantics (so an in-transaction unique-value swap or
child-before-parent insert is legal).  Any event or commit error fails the
whole import - bad transactions are never skipped to produce a
seemingly-complete package.

The output directory is brand new (an existing path is never overwritten) and
is assembled in a hidden staging directory that is renamed into place only on
success; failures and interruptions remove all staging and spill state.  The
package contains exactly:

```
<out>/snapshot/<table>.jsonl   starting snapshot in the target schema
<out>/schema.json              the unified target schema
<out>/changes.jsonl            full-image committed transactions (old log format)
<out>/capture-map.jsonl        every output tx/event -> source logical event
                               and ALL transport coordinates
<out>/capture-report.json      counts, bindings, input/window and hashes
```

The map traces each output event (including both halves of a key-move
expansion) back to its source `(tx_id, tx_order)` and every coordinate it was
seen at.  `tx_id` values are preserved verbatim.  For capture-v2 the map also
records each event's declared `schema_version` and any `backfilled_columns`.

### capture-v2: ordered schema history

An upstream table can be renamed and extended while one archive still holds
records written on both sides of the change.  Reading old records with the
newest schema silently turns old columns into missing values; reading new
records with the old schema silently drops the new information.  A
capture-v2 archive therefore declares its schema history explicitly:

```json
{
  "version": 2,
  "source_id": "src-evolving-1",
  "snapshot_commit_seq": 0,
  "sealed_commit_seq": 2,
  "streams": [{"name": "s1", "first_offset": 0, "last_offset": 3}],
  "files": ["shards/s1-00.jsonl", "shards/s1-01.jsonl"],
  "schema_history": "schema-history.json",
  "history_hash": "c8cd37a0…8c89"
}
```

`schema-history.json` (`format: "capture-schema-history-v1"`) holds an
**ordered, non-empty** list of versions:

```json
{
  "format": "capture-schema-history-v1",
  "versions": [
    {"version": 1, "effective_commit_seq": 0, "tables": {
      "accounts": {"target_table": "accounts", "primary_key": ["f-key"],
        "columns": [
          {"field_id": "f-key",    "name": "key",   "type": "string", "nullable": false},
          {"field_id": "f-amount", "name": "gross", "type": "int",    "nullable": false}],
        "unique_keys": [], "foreign_keys": []}}},
    {"version": 2, "effective_commit_seq": 2, "tables": {
      "accounts": {"target_table": "accounts", "primary_key": ["f-key"],
        "columns": [
          {"field_id": "f-key",      "name": "key",      "type": "string", "nullable": false},
          {"field_id": "f-amount",   "name": "amount",   "type": "float",  "nullable": false},
          {"field_id": "f-currency", "name": "currency", "type": "string", "nullable": false,
           "backfill": "USD"}],
        "unique_keys": [], "foreign_keys": []}}}
  ]
}
```

Each version binds a **strictly increasing** version number, the commit point
at which it becomes the write schema, and the table/column definitions
including a **stable field ID** per column.  Constraints name field IDs too.
`version` numbers must be unique, `effective_commit_seq` must strictly
increase, version 1 must be effective at commit `0`, and every later switch
point must lie within the sealed range.

Every `change` message in a capture-v2 archive carries an extra
`schema_version` naming the version its row image was written with (and a
capture-v1 message may not carry one).  Import validates, before touching any
row state:

* the version is declared by this archive's history;
* every column named in `before`/`after` exists **in that version** - a
  message naming a column of another version is rejected with its
  source coordinate;
* the transaction's events all declare one version, and that version is the
  one in force at the transaction's `commit_seq`.  A schema switch happens at
  a **transaction commit boundary**: no transaction is ever split across two
  versions, and a version is never applied early to events that merely
  arrived first in transport order.

The manifest's `history_hash` binds the exact history content (SHA-256 over
canonical JSON).  `capture-import`/`capture-verify` take the same history with
`--schema-history` and refuse a history whose hash differs - an archive is
never reinterpreted against a different history.

#### Supported evolution

Stable field IDs carry the identity of a business field; the *name* is just
the current label.  Between consecutive versions only these changes are
allowed:

| change | allowed |
| --- | --- |
| rename a column (same field ID, new name) | yes |
| add a column with an explicit constant `backfill` | yes (also for NOT NULL) |
| add a nullable column without a backfill | yes (older rows get `null`) |
| `int` -> `float` | yes (lossless widening) |
| delete a column | **no** |
| replace a field ID (same name, different ID is a different field) | **no** |
| narrow a type, or retype anything but `int` -> `float` | **no** |
| change an existing column's nullable rule | **no** |
| add a new NOT NULL column without a backfill | **no** |
| change primary key, unique keys or foreign keys | **no** |

Names are not identities: two different field IDs are never merged because
their names coincide, and a rename never creates a second target column.

#### Conversion to the unified target schema

Each message is interpreted first with its declared version (types validated
as that version declared them, primary key as that version defined it), then
mapped onto the final target schema by field ID:

* a renamed field writes the same target column in both versions;
* a target column whose field did not exist yet is written from that field's
  constant backfill (for complete images: snapshot rows and insert after
  images); partial images still mean *preserve*, never *backfill*;
* `int` values widen to `float` exactly (no value changes);
* missing and explicit null stay distinct throughout.

The starting snapshot is the **initial** version's data, written under that
version's historical column names; it is converted the same way (renamed
columns moved, later columns backfilled) and then validated with the normal
target-schema unique/FK rules.

The report's `schema_history` section records the history hash, every version
and its switch point; `target_schema` records the unified target schema
content hash.  Both are compared by the verifier.

### Independent verification

`capture-verify` takes the original archive, snapshot, schema (and for
capture-v2 the schema history) plus a package, and re-runs the whole scan,
de-duplication, assembly, per-version interpretation, commit-boundary check
and restoration itself.  It then compares the complete transaction images,
window boundary, provenance map, converted snapshot and report
bindings/counts/hashes against the package.  It never reads an "import
succeeded" flag and never compares only final table state.

Verdicts are deliberately not oracle-style PASS/FAIL:

* `CAPTURE_PACKAGE_MATCH` - the independent restoration equals the package;
* `ARCHIVE_INVALID` - the source archive or its schema history is incomplete,
  conflicting or violates the supported evolution rules;
* `RESTORE_FAILED` - a valid archive cannot be restored against the given
  snapshot/schema (stale before-image, key collision, commit constraint, ...);
* `PACKAGE_MISMATCH` - restoration succeeds but the package differs.

The schema-history checks specifically catch:

* the pre-rename column mapped onto a target column that merely *shares its
  name* but belongs to a different field ID;
* a backfilled value replaced by `null` (or any other value);
* events converted with the wrong version relative to the commit switch
  (including a doctored `schema_version` in the provenance map);
* a modified schema-history hash or switch point in the report;
* a package snapshot that lost a backfill or kept a historical column name.

Failures name the source stream/offset, transaction, logical order, schema
version and commit sequence.  The failure classes stay distinct in the
report.

### Commands

```bash
# capture-v1: restore an archive into a full-image package (no oracle).
PYTHONPATH=src python3 -P -m ddreduce.cli capture-import \
  --archive demo/capture/sample/archive \
  --snapshot demo/capture/sample/snapshot \
  --schema demo/capture/sample/schema.json \
  --through 14 --out /tmp/capture-package

# independently re-restore and compare the package.
PYTHONPATH=src python3 -P -m ddreduce.cli capture-verify \
  --archive demo/capture/sample/archive \
  --snapshot demo/capture/sample/snapshot \
  --schema demo/capture/sample/schema.json \
  --through 14 --package /tmp/capture-package

# capture-v2: the same commands plus --schema-history.
PYTHONPATH=src python3 -P -m ddreduce.cli capture-import \
  --archive demo/capture/evolving-sample/archive \
  --snapshot demo/capture/evolving-sample/snapshot \
  --schema demo/capture/evolving-sample/schema.json \
  --schema-history demo/capture/evolving-sample/archive/schema-history.json \
  --through 2 --out /tmp/evolving-package

# The result then feeds the unchanged existing tools.
PYTHONPATH=src python3 -P -m ddreduce.cli log-replay \
  --snapshot /tmp/capture-package/snapshot \
  --schema /tmp/capture-package/schema.json \
  --changes /tmp/capture-package/changes.jsonl
```

`capture-import`/`capture-verify` exit `0` on match/success and `2` on any
archive, history, restore or package failure.

### Bounded samples and reproducible generators

`demo/capture/sample/` is a small, committed cross-partition capture-v1
archive built by `demo/capture/gen_capture.py`.  Its hand-computed scenario:
snapshot key `k` has `amount=10, note="old"`; transaction A (commit point 13)
first sets `amount=20` then explicitly sets `note` to null; transaction B
(commit point 14) moves key `k` to `z`; an aborted transaction that would set
`amount=999` is present but never applied.  A's messages are retransmitted
(physically and logically), the shards are shuffled and B's commit arrives
before B's change.  The correct restoration is one line for A with two
full-image updates and one line for B as adjacent delete/insert.

`demo/capture/evolving-sample/` is the committed capture-v2 hand sample built
by `demo/capture/gen_evolving.py --flavor hand`:

* version 1 has field `f-amount` named `gross`, typed `int`;
* version 2 (effective at commit 2) renames it to `amount`, widens it to
  `float`, and adds `currency` with the constant backfill `"USD"`;
* commit 1 (version 1) updates the row; commit 2 (version 2) updates it again.

After import both events write the target column `amount` (10.0 -> 20.0 ->
30.0), the old row and the updated row both carry `currency="USD"`, and the
independent verifier matches.  `--flavor nameclash` builds an archive where a
new field reuses an old column *name*: the legacy field keeps its own target
column and the new field is backfilled, proving names alone never merge
fields.

```bash
# Bounded samples (byte-identical for fixed arguments).
python3 -I demo/capture/gen_capture.py  --out /tmp/sample  --flavor hand --seed 7 --shards 3
python3 -I demo/capture/gen_evolving.py --out /tmp/evolving --flavor hand --seed 7 --shards 2
python3 -I demo/capture/gen_evolving.py --out /tmp/clash   --flavor nameclash --seed 3 --shards 1
# Larger archives are generated at runtime for the resource tests only.
python3 -I demo/capture/gen_capture.py  --out /tmp/big --flavor large --tx 2000 --keys 200
python3 -I demo/capture/gen_evolving.py --out /tmp/ebig --flavor large --tx 600 --keys 60
```

The scan/de-duplication/assembly stage spills to a temporary SQLite database
(messages, transaction grouping and per-event provenance are never held in
memory as the archive grows); only the snapshot and current row state stay in
memory, and output provenance is streamed one record at a time.  Spill files
are removed on both success and failure; the large-scale tests measure peak
heap and spill bytes against archive growth (capture-v1 and capture-v2) and
check the cleanup.

---

## 7. Test suite

The suite uses an external, deliberately configurable mock oracle
(`tests/mock_oracle.py`) plus the real demo ETL and the real incremental
aggregation command. It covers:

- the **hand-computed small example** (P={p,q}, C={b→p, g→q}), with exhaustive
  enumeration of every legal deletion set and its certificate;
- composite typed primary keys and whole-closure handling of **cyclic FKs**;
- orphan bad rows, **empty-database failure**, two alternative fault groups;
- same output but a **changed command signature** under shrinking;
- timeouts (never FAIL), non-zero exits, missing reports, **flaky** baselines;
- process-group kill with grandchild cleanup (no leaked processes);
- nullable FK semantics (dangling references stay legal, no cascade);
- typed-scalar and unique-key validation (bool ≠ int, duplicate PK/UK rejected);
- SIGTERM interruption + `resume`, budget exhaustion, no double charging;
- cache key binding (content/schema/command/config) and corrupt-entry handling;
- a large generated dataset (3,600 rows) checking bounded disk usage;
- the end-to-end real ETL defect and standalone reproduction of the result;
- the **CDC log contract**: per-event and commit-time validation, unique-value
  swaps, child-before-parent, cyclic FK commits, atomic commit failure;
- **hand-computed causality scenarios**: overwrite chains (x=1→2→1→9) that
  must not rebind to same-value baseline rows, and tombstone chains
  (insert/delete/rebuild) that must not rebind to the initial absence;
- **parent-version witnesses**: a child must not rebind to a same-value
  baseline parent;
- end-to-end CDC reduction on the real incremental aggregation defect,
  standalone package reproduction, and tamper detection (candidate log,
  evidence log, selection, certificate references, transaction boundaries);
- **same final table, different history**: distinct cache keys, distinct
  verdicts, no shared results;
- an **independent naive replayer** cross-checking every transaction
  selection, per-commit state, and certificate neighbors on small logs;
- log-mode budget exhaustion, SIGTERM + resume, input/schema/oracle-behavior
  change refusal, unknown neighbors, and `log-replay` reporting;
- a generated 160-transaction log checking bounded staging/cache disk usage;
- **capture-v1 ingestion**: manifest/message field tables and rejection rules
  (unsupported version, escaping/duplicate/symlink paths, undeclared streams,
  out-of-range offsets, booleans impersonating integers, binding bijection and
  full-coverage rules);
- capture stream coverage gaps (with first missing offset), physical
  same-coordinate conflicts, logical `(tx_id, tx_order)` conflicts,
  commit/abort and changed-commit conflicts, end count/table-count mismatch,
  missing end decisions, duplicate `commit_seq`, and conflicts hidden inside
  out-of-window transactions;
- partial-image restoration (preserve vs explicit null, empty-after updates,
  stale before-images, aborted events never applied), key-move expansion into
  delete/insert with full provenance, key-move target collision, in-transaction
  unique-value swaps and child-before-parent commits;
- the exact hand-computed cross-partition sample, window truncation,
  zero-event commit bookkeeping, deterministic regeneration and full
  provenance retention under reordering/retransmission;
- randomized multi-transaction archives (shard reordering, physical and
  logical retransmissions, aborts, heartbeats, window cuts) cross-checked
  against an independent naive reference replayer that shares no product
  imaging/merge code, plus "same final row, different history" archives that
  must never compare equal;
- end-to-end re-encoding of the incremental demo's full-image log into a
  partial-image capture archive, restoring it and reproducing the same
  aggregation divergence through the real demo command, with the package also
  accepted by the existing `log-replay`;
- independent `capture-verify` tamper detection: dropped overwritten updates,
  folded delete/rebuilds, rewritten transport provenance, doctored counts and
  extra package files, with the three failure classes kept distinct;
- source-to-target table/column renaming bindings and post-rename type rules;
- a runtime-generated large archive checking bounded heap growth, linear spill
  growth and spill/staging cleanup on both success and failure;
- **capture-v2 schema evolution**: ordered histories with stable field IDs and
  explicit backfills; refusal of duplicate version numbers, non-increasing
  commit points, out-of-range switch points and a history that differs from
  the one sealed in the manifest;
- the exact hand-computed evolving sample (int `gross` -> float `amount`
  rename, `currency="USD"` backfill, one commit on each side of the switch):
  both events reach the target column, old and new rows keep the backfill,
  and independent verification matches;
- same-name/different-field-ID archives proving fields are never merged by
  name, plus verifier detection of a renamed column poured into a same-named
  target column, a backfill replaced by null, events converted with the wrong
  version, and a doctored schema-history hash or switch point in the report;
- supported-evolution refusals: deleted columns, replaced field IDs, narrowed
  types, changed nullability, new NOT NULL columns without a backfill and
  changed key/constraint definitions, plus message/history disagreements
  (missing or unknown `schema_version`, images naming another version's
  columns, transactions straddling a switch);
- a naive independent capture-v2 reference converter cross-checking
  enumerated version/commit/window combinations and three-version histories;
- the real incremental demo re-encoded as a two-version archive, converted,
  replayed through `log-replay` and driving the demo to its FAIL signature;
- a runtime-generated three-version archive checking bounded heap, linear
  spill growth and cleanup on success and failure.

Run the suite:

```bash
PYTHONPATH=src python3 -m unittest discover -t . -s tests -v
```

The large dataset and all temporary fixtures are generated at test time into
per-test temp directories; only small, bounded fixtures are committed under
`fixtures/` and `demo/`.

## Project layout

```
src/ddreduce/
  values.py    typed scalars, canonical encoding, stable row ids
  schema.py    schema model and schema-level validation
  model.py     JSONL loading, unique/FK validation, deletion closures, materialization
  oracle.py    subprocess runner, report contract, timeout kill, verdict aggregation
  cache.py     content/schema/command/config-bound persistent cache
  state.py     atomic resumable state and append-only trajectory
  engine.py    batch ddmin + per-row closure final pass, budget accounting
  verify.py    independent certificate verification
  logmodel.py  CDC changes.jsonl contract: parsing and structural validation
  replay.py    version/absence witnesses, normalized replay, log candidates
  logengine.py batch ddmin over transaction deletions + per-transaction pass
  logverify.py independent CDC certificate verification
  capture/     offline capture ingestion (no oracle, no new reducer)
    manifest.py   capture.json parsing, path safety, binding bijections
    messages.py   envelope/kind field tables and structural message validation
    store.py      streaming SQLite-backed scan, de-duplication and assembly
    history.py    capture-v2 ordered schema history, field IDs, evolution rules
    evolve.py     per-version interpretation, field-ID mapping, backfill, widening
    restore.py    partial-image row restoration, key moves, commit constraints
    importer.py   capture-import: windowed restore into a full-image package
    verifier.py   capture-verify: independent re-restore and package comparison
  cli.py       reduce / resume / verify / log-replay / log-reduce / log-verify
               / capture-import / capture-verify
demo/
  transform/   real multi-table ETL command, schema, data, oracle config
  incremental/ real CDC incremental aggregation command, data, log generator
  capture/     capture-v1 + capture-v2 hand samples and their generators
fixtures/      small committed hand-computed fixture
tests/         mock oracle, naive replayers, and the full scenario suite
```
