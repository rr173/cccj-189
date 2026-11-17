# ddreduce — relational failure-input reducer

A failure-input minimizer for **relational JSONL data**. Given a directory of
JSONL tables, a schema, and an external "oracle" command that reports `PASS`
or a signed `FAIL`, `ddreduce` repeatedly tries **smaller but still
referentially valid** relational datasets until it finds a small package that
still triggers the *same* fault signature.

The target fault is determined entirely by **actually running the command
under test** — the tool never assumes which rows are relevant.

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

## 5. Test suite

The suite uses an external, deliberately configurable mock oracle
(`tests/mock_oracle.py`) plus the real demo ETL. It covers:

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
- the end-to-end real ETL defect and standalone reproduction of the result.

Run the suite:

```bash
PYTHONPATH=src python3 -m unittest discover -t . -s tests -v
```

The large dataset and all temporary fixtures are generated at test time into
per-test temp directories; only small, bounded fixtures are committed under
`fixtures/`.

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
  cli.py       reduce / resume / verify entry points
demo/          real multi-table ETL command, schema, data, oracle config
fixtures/      small committed hand-computed fixture
tests/         mock oracle and the full scenario suite
```
