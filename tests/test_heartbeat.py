"""Heartbeat writer and monitor against an in-memory SQLite database."""
import os
import sqlite3
import sys
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tplytics import heartbeat as H  # noqa: E402
from tplytics.engines import ENGINES, Engine, Session  # noqa: E402


class SQLiteSession(Session):
    def __init__(self, engine, db):
        super().__init__(engine)
        self.conn, self.server_version = db, "SQLite " + sqlite3.sqlite_version

    def execute(self, sql, fetch=True):
        c = self.conn.execute(sql)
        self.conn.commit()
        if not fetch or c.description is None:
            return [], []
        return [d[0] for d in c.description], c.fetchall()

    def close(self):
        pass


class SQLiteEngine(Engine):
    name = "sqlite"

    def __init__(self, opts, db):
        super().__init__(opts)
        self.db = db

    def literal(self, v):
        if hasattr(v, "strftime"):            # SQLite has no TIMESTAMP '...' literal
            return "'%s'" % v.strftime("%Y-%m-%d %H:%M:%S")
        return super().literal(v)

    def connect(self):
        return SQLiteSession(self, self.db)

    def describe(self):
        return "sqlite :memory:"


def opts(**kw):
    o = dict(table=H.DEFAULT_TABLE, id=1, interval=0.01, label="test", status_file=None,
             max_lag=None, init_sql=[], skew=None)
    o.update(kw)
    return types.SimpleNamespace(**o)


class HeartbeatTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.engine = SQLiteEngine(opts(), self.db)

    def test_write_then_read(self):
        w = H.Writer(self.engine, H.DEFAULT_TABLE, 1, 1.0)
        w.connect()                            # creates the table and inserts seq 1
        w.beat()
        m = H.Monitor(opts(), self.engine)
        lag, err = m.step()
        self.assertIsNone(err)
        self.assertEqual(lag, 0.0)               # the newest beat is visible
        self.assertLess(m.status["age"], 5)
        self.assertEqual(m.status["seq"], 2)
        self.assertEqual(m.snapshot()["stats"]["1m"]["samples"], 1)

    def test_sequence_continues_after_restart(self):
        w = H.Writer(self.engine, H.DEFAULT_TABLE, 1, 1.0)
        w.connect()
        for _ in range(3):
            w.beat()
        w2 = H.Writer(self.engine, H.DEFAULT_TABLE, 1, 1.0)
        w2.connect()
        w2.beat()
        self.assertEqual(w2.seq, 5)

    def test_separate_rows_per_writer(self):
        for row in (1, 2):
            w = H.Writer(self.engine, H.DEFAULT_TABLE, row, 1.0)
            w.connect()
            w.beat()
        _, rows = self.engine.connect().execute("SELECT id, seq FROM %s ORDER BY id" % H.DEFAULT_TABLE)
        self.assertEqual([tuple(r) for r in rows], [(1, 2), (2, 2)])

    def test_missing_table_and_row(self):
        m = H.Monitor(opts(), self.engine)
        lag, err = m.step()
        self.assertIsNone(lag)
        self.assertEqual(m.status["state"], "error")
        H.Writer(self.engine, H.DEFAULT_TABLE, 1, 1.0).connect()
        m = H.Monitor(opts(id=7), self.engine)
        lag, err = m.step()
        self.assertEqual(m.status["state"], "no heartbeat")

    def test_check_exit_codes(self):
        w = H.Writer(self.engine, H.DEFAULT_TABLE, 1, 1.0)
        w.connect()
        self.assertEqual(H.run_check(opts(max_lag=60), self.engine), 0)
        self.db.execute("UPDATE %s SET ts_us = ts_us - 120000000" % H.DEFAULT_TABLE)
        self.assertEqual(H.run_check(opts(max_lag=60), self.engine), 1)
        self.assertEqual(H.run_check(opts(id=9), self.engine), 2)

    def test_lag_definition(self):
        beat = 1_000_000_000                      # a beat written at t=1000 s, 1 s interval
        self.assertEqual(H.lag_of(1000.5, beat, 1_000_000), 0.0)    # next beat not due yet
        self.assertEqual(H.lag_of(1001.5, beat, 1_000_000), 0.5)    # next beat 0.5 s overdue
        self.assertAlmostEqual(H.lag_of(1300.0, beat, 1_000_000), 299.0)

    def test_stats_windows(self):
        s = H.LagStats()
        for i in range(100):
            s.add(1000.0 + i, float(i % 10))
        out = s.summary(1099.0)
        self.assertEqual(out["1m"]["samples"], 61)
        self.assertEqual(out["1m"]["max"], 9.0)
        self.assertEqual(out["15m"]["samples"], 100)

    def test_ddl_per_engine(self):
        o = types.SimpleNamespace(schema=None)
        self.assertIn("ts DATETIME", ENGINES["mysql"](o).heartbeat_ddl("t"))
        self.assertIn("ts TIMESTAMP", ENGINES["postgresql"](o).heartbeat_ddl("t"))
        self.assertFalse(ENGINES["dbtrail"](o).heartbeat_writable)
        with self.assertRaises(SystemExit):
            H._check_table_name("t; DROP TABLE x")


if __name__ == "__main__":
    unittest.main()
