"""Cache key binding and disk-resource behavior on large generated data."""

from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

from ddreduce.cache import OracleCache, make_cache_key
from ddreduce.model import Database
from ddreduce.oracle import OracleConfig, OracleRunner, RunSignal
from ddreduce.schema import load_schema

from .helpers import TempDir, make_fixture, run_cli

SCHEMA = {
    "tables": [
        {
            "name": "P",
            "columns": [{"name": "id", "type": "string", "nullable": False}],
            "primary_key": ["id"],
        },
        {
            "name": "C",
            "columns": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "p_id", "type": "string", "nullable": False},
            ],
            "primary_key": ["id"],
            "foreign_keys": [
                {"columns": ["p_id"], "ref_table": "P", "ref_columns": ["id"]}
            ],
        },
    ]
}


class CacheBindingTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TempDir()
        mock = {"fail_rows": ["C/c1"], "fail_signature": "SIG-TARGET"}
        tables = {
            "P": [{"id": "p1"}, {"id": "p2"}],
            "C": [{"id": "c1", "p_id": "p1"}, {"id": "c2", "p_id": "p2"}],
        }
        self.schema_path, self.tables_dir, self.oracle_path = make_fixture(
            self.tmp.path, SCHEMA, tables, mock
        )
        self.workdir = self.tmp.path / "work"

    def tearDown(self):
        self.tmp.cleanup()

    def _key(self, db, config):
        schema = load_schema(self.schema_path)
        return make_cache_key(db, schema, config)

    def test_cache_key_binds_content_schema_command_config(self):
        schema = load_schema(self.schema_path)
        db1 = Database.load(schema, self.tables_dir)
        cfg = OracleConfig.load(self.oracle_path)
        key1 = make_cache_key(db1, schema, cfg)

        # Same content, same everything => identical key.
        db2 = Database.load(schema, self.tables_dir)
        self.assertEqual(make_cache_key(db2, schema, cfg).digest, key1.digest)

        # Different candidate content => different key.
        smaller = db1.apply_deletion(db1.deletion_closure(['P/str:"p2"']))
        self.assertNotEqual(
            make_cache_key(smaller, schema, cfg).digest, key1.digest
        )

        # Different command / timeout / retest count => different key.
        cfg2 = OracleConfig(
            command=cfg.command + ["--extra"],
            timeout_seconds=cfg.timeout_seconds,
            retest_runs=cfg.retest_runs,
            confirm_runs=cfg.confirm_runs,
            max_calls=cfg.max_calls,
        )
        self.assertNotEqual(
            make_cache_key(db1, schema, cfg2).digest, key1.digest
        )
        cfg3 = OracleConfig(
            command=cfg.command,
            timeout_seconds=cfg.timeout_seconds + 1,
            retest_runs=cfg.retest_runs,
            confirm_runs=cfg.confirm_runs,
            max_calls=cfg.max_calls,
        )
        self.assertNotEqual(
            make_cache_key(db1, schema, cfg3).digest, key1.digest
        )

    def test_cache_roundtrip_and_corrupt_entry_is_miss(self):
        schema = load_schema(self.schema_path)
        db = Database.load(schema, self.tables_dir)
        cfg = OracleConfig.load(self.oracle_path)
        cache = OracleCache(self.workdir)
        key = make_cache_key(db, schema, cfg)
        self.assertIsNone(cache.get(key))
        signal = RunSignal(
            exit_code=0,
            timed_out=False,
            report_status="FAIL",
            signature="SIG-TARGET",
            report_error=None,
            stdout_tail="",
            stderr_tail="",
            duration_seconds=0.01,
        )
        cache.put(key, [signal])
        got = cache.get(key)
        self.assertIsNotNone(got)
        self.assertEqual(got[0].signature, "SIG-TARGET")
        # Corrupt the entry: must degrade to a miss, never a fake answer.
        path = key.path(self.workdir / "cache")
        path.write_text("{not json", encoding="utf-8")
        self.assertIsNone(cache.get(key))

    def test_candidate_directories_never_reused_and_cleaned(self):
        schema = load_schema(self.schema_path)
        db = Database.load(schema, self.tables_dir)
        cfg = OracleConfig.load(self.oracle_path)
        runner = OracleRunner(self.workdir, cfg)
        seen = set()
        for _ in range(3):
            sig, candidate = runner.run_once(db)
            self.assertNotIn(candidate.name, seen)
            seen.add(candidate.name)
            self.assertFalse(candidate.exists())  # removed after process exits
        self.assertEqual(list((self.workdir / "candidates").glob("*")), [])


class LargeDataResourceTest(unittest.TestCase):
    """Large generated fixture: closure/cache stay bounded in disk usage."""

    N_PARENTS = 400
    N_CHILDREN_PER = 8  # 3600 rows total; generated at test time, not committed

    def setUp(self):
        self.tmp = TempDir()
        tables = {
            "P": [{"id": f"p{i:05d}"} for i in range(self.N_PARENTS)],
            "C": [
                {"id": f"c{i:05d}-{j}", "p_id": f"p{i:05d}"}
                for i in range(self.N_PARENTS)
                for j in range(self.N_CHILDREN_PER)
            ],
        }
        mock = {"fail_rows": ["C/c00399-7"], "fail_signature": "SIG-TARGET"}
        self.schema_path, self.tables_dir, self.oracle_path = make_fixture(
            self.tmp.path, SCHEMA, tables, mock, max_calls=300
        )
        self.workdir = self.tmp.path / "work"

    def tearDown(self):
        self.tmp.cleanup()

    def test_large_reduce_keeps_disk_bounded(self):
        total_rows = self.N_PARENTS * (1 + self.N_CHILDREN_PER)
        result = run_cli(
            "reduce",
            "--input", str(self.tables_dir),
            "--schema", str(self.schema_path),
            "--oracle-config", str(self.oracle_path),
            "--workdir", str(self.workdir),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        cert = json.loads((self.workdir / "certificate.json").read_text())
        self.assertEqual(
            sorted(cert["result"]["row_ids"]),
            ['C/str:"c00399-7"', 'P/str:"p00399"'],
        )
        self.assertGreaterEqual(cert["input"]["row_count"], total_rows)

        # No live candidate directories remain at any point after completion.
        self.assertEqual(list((self.workdir / "candidates").glob("*")), [])

        # Cache stores one small JSON per distinct candidate: bounded count and
        # no single oversized file.
        cache_files = list((self.workdir / "cache").rglob("*.json"))
        self.assertGreater(len(cache_files), 0)
        biggest = max(f.stat().st_size for f in cache_files)
        # Each entry only stores run signals, not a copy of the candidate.
        self.assertLess(biggest, 256 * 1024)

        # Result package materializes only the two surviving rows.
        result_size = sum(
            f.stat().st_size for f in (self.workdir / "result").glob("*.jsonl")
        )
        self.assertLess(result_size, 4096)

        # Independent verification on the large search's small result package.
        verify = run_cli(
            "verify",
            "--input", str(self.tables_dir),
            "--schema", str(self.schema_path),
            "--oracle-config", str(self.oracle_path),
            "--workdir", str(self.workdir),
        )
        self.assertEqual(verify.returncode, 0, verify.stderr)
        report = json.loads((self.workdir / "verify-report.json").read_text())
        self.assertEqual(report["verdict"], "VERIFIED_MINIMAL")


if __name__ == "__main__":
    unittest.main()
