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

    def test_twenty_unique_queries(self):
        self.assertEqual(len(self.queries), 20)
        self.assertEqual(len({q.id for q in self.queries}), 20)

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


if __name__ == "__main__":
    unittest.main()
