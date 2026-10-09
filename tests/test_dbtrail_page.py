"""The /dbtrail page's data: failure log, DBTrail self-report, Prometheus phases."""
import json
import os
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tplytics import compare as C  # noqa: E402
from tplytics import dbtrail_info as DI  # noqa: E402
from tplytics import perf  # noqa: E402

METRICS = """# HELP x
bintrail_stream_events_indexed_total 1000
bintrail_stream_availability_lag_seconds{source="a"} 4.5
bintrail_stream_replication_lag_seconds{source="a"} 0
bintrail_sql_statement_phase_seconds_sum{phase="total"} 30
bintrail_sql_statement_phase_seconds_count{phase="total"} 10
bintrail_sql_statement_phase_seconds_sum{phase="query"} 20
bintrail_sql_statement_phase_seconds_count{phase="query"} 10
bintrail_sql_statement_phase_seconds_bucket{phase="query",le="1"} 4
process_cpu_seconds_total 50
process_resident_memory_bytes 2.097152e+08
process_start_time_seconds 1.7e+09
"""


class FailLogTests(unittest.TestCase):
    def test_kept_newest_first_and_survives_restart(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "f.jsonl")
            log = C.FailLog(path, keep=3)
            for i in range(5):
                log.add({"t": i, "target": "DBTrail" if i % 2 else "MySQL", "q": "Q%02d" % i, "sql": "SELECT %d" % i})
            self.assertEqual([p["q"] for p in log.latest("DBTrail")], ["Q03"])     # Q01 aged out of the 3 kept
            self.assertEqual([p["q"] for p in log.latest()], ["Q04", "Q03", "Q02"])
            again = C.FailLog(path, keep=3)
            self.assertEqual([p["q"] for p in again.latest()], ["Q04", "Q03", "Q02"])
            with open(path) as fh:
                self.assertEqual(len(fh.readlines()), 3)      # cut back to `keep` at start


class DBTrailInfoTests(unittest.TestCase):
    def test_token_from_env_file_or_plain(self):
        with tempfile.TemporaryDirectory() as d:
            env, plain = os.path.join(d, ".env"), os.path.join(d, "tok")
            pathlib.Path(env).write_text("INDEX_BUFFER_POOL=1G\nCONSOLE_TOKEN=abc123\n")
            pathlib.Path(plain).write_text("xyz\n")
            self.assertEqual(DI.read_token(env), "abc123")
            self.assertEqual(DI.read_token(plain), "xyz")
            self.assertIsNone(DI.read_token(None))

    def test_jobs_from_history(self):
        hist = {"servers": {"s1": [
            {"kind": "dump", "started_at": "2026-10-09T06:00:21Z", "finished_at": "2026-10-09T06:15:03Z", "rows": 5},
            {"kind": "refresh", "started_at": "2026-10-09T06:20:07Z", "finished_at": "2026-10-09T06:20:27Z", "events": 7},
            {"kind": "dump", "started_at": "2026-10-09T06:30:05Z", "finished_at": "2026-10-09T06:30:05Z",
             "skip_reason": "another snapshot job was running"}]}}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(hist, f)
        try:
            jobs = DI.DBTrailInfo("http://127.0.0.1:1", history_file=f.name)._jobs({"s1"})["s1"]
        finally:
            os.unlink(f.name)
        self.assertEqual([j["outcome"] for j in jobs], ["skipped", "ok", "ok"])      # newest first
        self.assertEqual(jobs[2]["seconds"], 882.0)
        self.assertEqual(jobs[1]["events"], 7)


    def test_rewrites_from_container_log(self):
        line = ('{"log":"time=2026-10-09T20:07:56.640Z level=INFO msg=\\"table rewritten in full with deltas on\\" schema=tpcc '
                'table=stock reason=\\"the changes beside the table (49 MB) passed 48 MB, the most a refresh leaves beside a '
                'table for SQL on the copy\\" events_applied=8726 rows_written=20000000 fetch_ms=584 fold_ms=90\\n",'
                '"stream":"stderr","time":"2026-10-09T20:07:56.640Z"}')
        log = DI.RewriteLog()
        log._parse(line)
        log._parse('{"log":"time=2026-10-09T20:07:57Z level=INFO msg=\\"checkpoint saved\\"\\n"}')
        self.assertEqual(len(log.events), 1)
        e = log.events[0]
        self.assertEqual((e["table"], e["rows"]), ("stock", 20000000))
        self.assertEqual(e["reason"], "changes beside it reached 49 MB (limit 48 MB)")
        self.assertEqual(len(log.within(e["t"] - 60, e["t"] + 60)), 1)
        self.assertEqual(log.within(e["t"] + 10, e["t"] + 60), [])
        self.assertEqual(DI.short_reason("the chain started at 2026-10-09T15:25:21Z, too close to the oldest events the index "
                                         "keeps (chains that start at or before X are ended)"),
                         "its changes reached the oldest events the index keeps")

    def test_unreachable_console_is_reported_not_fatal(self):
        doc = DI.DBTrailInfo("http://127.0.0.1:1").snapshot()
        self.assertIn("sql", doc["errors"])
        self.assertEqual(doc["servers"], [])


class PrometheusTests(unittest.TestCase):
    def test_phases_and_process(self):
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
            f.write(METRICS)
        try:
            s = perf.prometheus_sample(pathlib.Path(f.name).as_uri())
        finally:
            os.unlink(f.name)
        self.assertEqual(s["c"]["sqlph_total_count"], 10)
        self.assertEqual(s["c"]["sqlph_query_sum"], 20)
        self.assertEqual(s["c"]["proc_cpu"], 50)
        self.assertEqual(s["g"]["stream_lag"], 4.5)
        self.assertEqual(s["g"]["started"], 1.7e9)
        # two samples 10 s apart -> rates and per-statement phase times
        a = {"t": 0, "os": {"disk": {"rio": 0, "wio": 0, "rbytes": 0, "wbytes": 0, "busy_ms": 0}, "net": {"rx": 0, "tx": 0}},
             "units": {}, "db": s}
        c2 = dict(s["c"], sqlph_total_count=30, sqlph_total_sum=50, sqlph_query_count=30, sqlph_query_sum=30, proc_cpu=55)
        b = dict(a, t=10, db={"c": c2, "g": s["g"]})
        ps = perf.PerfSampler()
        ps._add(a, b)
        p = ps.series[-1]
        self.assertEqual(p["db_sql_per_s"], 2.0)
        self.assertEqual(p["db_sql_total_s"], 1.0)        # 20 s over 20 statements
        self.assertEqual(p["db_sql_query_s"], 0.5)
        self.assertEqual(p["db_proc_cpu"], 50.0)          # 5 CPU-seconds in 10 s
        self.assertEqual(p["db_rss_mb"], 200)


if __name__ == "__main__":
    unittest.main()
