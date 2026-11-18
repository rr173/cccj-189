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

## 6. Test suite

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
- a generated 160-transaction log checking bounded staging/cache disk usage.

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
  cli.py       reduce / resume / verify / log-replay / log-reduce / log-verify
demo/
  transform/   real multi-table ETL command, schema, data, oracle config
  incremental/ real CDC incremental aggregation command, data, log generator
fixtures/      small committed hand-computed fixture
tests/         mock oracle, naive replayer, and the full scenario suite
```
