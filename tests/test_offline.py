"""Checks that need no database: every query renders for every engine."""
import datetime as dt
import os
import random
import sys
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tplytics import params as P  # noqa: E402
from tplytics import queries as Q  # noqa: E402
from tplytics.engines import ENGINES  # noqa: E402

try:
    import pymysql  # noqa: F401
except ImportError:
    pymysql = None


def fake_state(warehouses=40, span_s=3600):
    t_max = dt.datetime(2026, 10, 7, 12, 0, 0)
    return types.SimpleNamespace(warehouses=warehouses, w_min=1, w_max=warehouses, districts=10,
                                 items=100000, customers_per_district=3000, t_min=t_max - dt.timedelta(seconds=span_s),
                                 t_max=t_max, span_seconds=float(span_s))


class OfflineTests(unittest.TestCase):
    def setUp(self):
        self.queries = Q.load()
        opts = types.SimpleNamespace(schema=None)
        self.engines = [cls(opts) for cls in ENGINES.values()]

    def test_unique_queries(self):
        self.assertEqual(len(self.queries), 14)
        # numbered Q01..Q14 without gaps, so sequential order and the per-query history line up
        self.assertEqual([q.id for q in self.queries], ["Q%02d" % i for i in range(1, 15)])

    def test_doris_engine(self):
        from tplytics.engines import get_engine
        e = get_engine("doris", types.SimpleNamespace(host=None, port=None, socket=None, user=None, password=None,
                                                      database="tpcc", defaults_file=None, schema=None, init_sql=None))
        self.assertEqual(e.dialects, ["doris", "mysql", "default"])
        self.assertEqual(e.default_port, 9030)
        self.assertEqual(e.timeout_statement(299.6), "SET query_timeout = 300")
        self.assertEqual(get_engine("apache-doris", e.opts).name, "doris")
        # Doris runs the MySQL text unless a query file has a [sql].doris variant
        q4 = next(q for q in self.queries if q.id == "Q04")
        self.assertIn("FROM_UNIXTIME", q4.sql_for(e))

    def test_render_all_engines(self):
        rng = random.Random(1)
        for state in (fake_state(1, 5), fake_state(40, 3600), fake_state(5000, 86400 * 30)):
            for q in self.queries:
                for e in self.engines:
                    if not q.supported(e):
                        continue
                    for _ in range(20):
                        v = q.new_params(state, rng)
                        sql = q.render(e, v)
                        self.assertNotIn("{", sql, q.id)
                        for k, val in v.items():
                            if k.endswith("_lo"):
                                self.assertLessEqual(val, v[k[:-3] + "_hi"])
                            if k.endswith("_hi") and "wh" in k:
                                self.assertLessEqual(val, state.w_max)

    def test_time_window_inside_range(self):
        rng = random.Random(2)
        st = fake_state(10, 7200)
        for _ in range(500):
            v = P.generate({"w": {"type": "time_window", "fraction": [0.05, 0.5]}}, st, rng)
            self.assertLess(v["w_start"], v["w_end"])
            self.assertGreaterEqual(v["w_start"], st.t_min - dt.timedelta(seconds=1))
            self.assertLessEqual(v["w_end"], st.t_max + dt.timedelta(seconds=2))
            self.assertLessEqual(v["w_start"], v["w_mid"])

    def test_weights_positive(self):
        for q in self.queries:
            self.assertGreater(q.weight["serial"], 0)
            self.assertGreater(q.weight["concurrent"], 0)

    def test_unsupported(self):
        q = self.queries[0]
        e = self.engines[0]
        q.unsupported = [e.name]
        self.assertFalse(q.supported(e))

    def test_dbtrail_uses_duckdb_text(self):
        e = ENGINES["dbtrail"](types.SimpleNamespace(schema=None))
        by_id = {q.id: q for q in self.queries}
        rng = random.Random(1)
        for qid, fn in (("Q04", "time_bucket"), ("Q13", "time_bucket")):
            sql = by_id[qid].render(e, by_id[qid].new_params(fake_state(), rng))
            self.assertIn(fn, sql, qid)
            self.assertNotIn("UNIX_TIMESTAMP", sql, qid)
            self.assertNotIn("TIMESTAMPDIFF", sql, qid)

    @unittest.skipIf(pymysql is None, "PyMySQL is not installed")
    def test_defaults_file_port(self):
        import tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".cnf", delete=False) as f:
            f.write("[client]\nhost=10.0.0.5\nport=3309\nuser=u\n")
        try:
            def args(engine, **kw):
                o = dict(defaults_file=f.name, socket=None, host=None, port=None, user=None,
                         password=None, database="tpcc")
                o.update(kw)
                return ENGINES[engine](types.SimpleNamespace(**o)).connect_args()
            a = args("mysql")
            self.assertEqual(a["port"], 3309)
            self.assertNotIn("host", a)              # PyMySQL reads it from the file
            self.assertEqual(args("mysql", port="3400")["port"], 3400)
            self.assertEqual(args("mysql", host="db1")["host"], "db1")
            with open(f.name, "w") as g:
                g.write("[client]\nuser=u\n")
            self.assertEqual(args("mysql")["port"], 3306)
            self.assertEqual(args("dbtrail")["port"], 3309)
        finally:
            os.unlink(f.name)


if __name__ == "__main__":
    unittest.main()
