# capture-v1 samples and generator

This directory contains the reproducible capture-v1 tooling for the offline
`capture-import` / `capture-verify` commands.  Nothing here runs an oracle and
nothing here adds a reduction strategy: the output is a full-image package the
existing `log-replay` / `log-reduce` commands already understand.

## Committed bounded sample

`sample/` is the small cross-partition hand sample (a handful of JSONL shards,
safe to commit).  Its scenario:

* snapshot: key `k` has `amount=10`, `note="old"`; `snapshot_commit_seq=12`;
* transaction **A** (commit_seq 13): `amount 10 -> 20`, then `note` explicitly
  set to `null`;
* transaction **B** (commit_seq 14): move key `k` to key `z`;
* transaction **C**: aborted change `amount -> 999`, never applied;
* A's messages are retransmitted (one physical duplicate at the same
  stream/offset and logical redeliveries on a second stream), the shards are
  shuffled, and B's commit physically arrives before B's change.

The expected restoration is one `changes.jsonl` line for A with two full-image
updates, then one line for B expanded into `delete(k)` + `insert(z)`, where
B's complete before image is exactly `{key: "k", amount: 20, note: null}`.

```bash
# Regenerate byte-identically (deterministic for fixed arguments).
python3 -I gen_capture.py --out /tmp/sample --flavor hand --seed 7 --shards 3

# Restore and independently verify.
PYTHONPATH=../../src python3 -P -m ddreduce.cli capture-import \
  --archive sample/archive --snapshot sample/snapshot \
  --schema sample/schema.json --through 14 --out /tmp/cap-package
PYTHONPATH=../../src python3 -P -m ddreduce.cli capture-verify \
  --archive sample/archive --snapshot sample/snapshot \
  --schema sample/schema.json --through 14 --package /tmp/cap-package
```

## Same final row, different histories

`--flavor direct` and `--flavor overwrite` build two archives that end at the
same row but carry different histories (one direct update vs an update
followed by an overwrite).  The verifier treats them as distinct histories:
a package restored from one never matches the other archive.

## Large runtime archives

Large archives are generated at test time into temporary directories and are
never committed:

```bash
python3 -I gen_capture.py --out /tmp/big --flavor large --tx 2000 --keys 200
```

The test suite (`tests/test_capture_large.py`) measures peak Python heap and
SQLite spill bytes as the archive grows, asserts bounded/non-quadratic
resource use, and checks that every spill and staging file is removed on both
success and failure.
