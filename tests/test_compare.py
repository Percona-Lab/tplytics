"""Compare dashboard: config parsing, common state and statistics (no database)."""
import datetime as dt
import math
import os
import sys
import tempfile
import types
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tplytics import compare as C  # noqa: E402
from tplytics import queries as Q  # noqa: E402
from tplytics.runner import ERROR, OK, TIMEOUT  # noqa: E402

CONFIG = b"""
[dashboard]
listen = "0.0.0.0"
http_port = 8088

[primary]
label = "primary"
defaults_file = "/etc/primary.cnf"

[[target]]
label = "MySQL replica"
defaults_file = "/etc/replica.cnf"
status_url = "http://10.0.0.2:8091"

[[target]]
label = "DBTrail"
engine = "dbtrail"
host = "10.0.0.3"
port = 3309
"""


def state(t_min, t_max, w=40):
    return types.SimpleNamespace(warehouses=w, w_min=1, w_max=w, districts=10, items=100000,
                                 customers_per_district=3000, t_min=t_min, t_max=t_max)


class CompareTests(unittest.TestCase):
    def test_load_config(self):
        with tempfile.NamedTemporaryFile(suffix=".toml", delete=False) as f:
            f.write(CONFIG)
        try:
            settings, primary, targets = C.load_config(f.name)
        finally:
            os.unlink(f.name)
        self.assertEqual(settings, {"listen": "0.0.0.0", "http_port": 8088})
        self.assertEqual(primary.defaults_file, "/etc/primary.cnf")
        self.assertEqual([t.label for t in targets], ["MySQL replica", "DBTrail"])
        self.assertEqual(targets[0].engine, "mysql")
        self.assertEqual(targets[1].port, "3309")
        self.assertEqual(targets[1].database, "tpcc")
        self.assertIsNone(targets[1].status_url)

    def test_unknown_key_refused(self):
        with self.assertRaises(SystemExit):
            C.target_opts({"label": "x", "hots": "typo"}, "target 1")

    def test_common_state_is_the_overlap(self):
        t0 = dt.datetime(2026, 10, 7, 10, 0)
        live = state(t0, t0 + dt.timedelta(hours=3))
        copy = state(t0 + dt.timedelta(minutes=5), t0 + dt.timedelta(hours=2, minutes=55))
        cs = C.CommonState([live, copy])
        self.assertEqual(cs.t_min, copy.t_min)
        self.assertEqual(cs.t_max, copy.t_max)
        self.assertEqual(cs.warehouses, 40)
        self.assertAlmostEqual(cs.span_seconds, 2 * 3600 + 50 * 60)
        # every query can draw parameters from it
        import random
        rng = random.Random(3)
        for q in Q.load():
            v = q.new_params(cs, rng)
            for k, val in v.items():
                if k.endswith("_start"):
                    self.assertGreaterEqual(val, cs.t_min - dt.timedelta(seconds=1), q.id)
                if k.endswith("_end"):
                    self.assertLessEqual(val, cs.t_max + dt.timedelta(seconds=1), q.id)

    def test_geomean_and_pairs(self):
        g = C.GeoStats()
        for s, e in ((OK, 1.0), (OK, 4.0), (ERROR, 0.1), (TIMEOUT, 120.0)):
            g.add(s, e)
        d = g.as_dict()
        self.assertAlmostEqual(d["geomean"], 2.0)
        self.assertEqual((d["ok"], d["errors"], d["timeouts"], d["runs"]), (2, 1, 1, 4))
        p = C.PairStats()
        p.add(10.0, 1.0)
        p.add(1.0, 1.0)
        self.assertAlmostEqual(p.ratio, math.sqrt(10))


    def test_pass_log_persists_and_prunes(self):
        import json as J
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "sub", "passes.jsonl")
            log = C.PassLog(path, keep_hours=24, timeout=120.0, n_queries=2)
            now = 1_000_000.0
            log.add_round({"A": {"status": OK, "elapsed": 1.0}, "B": {"status": OK, "elapsed": 0.1}}, now)
            self.assertEqual(log.snapshot()["points"], [])          # pass not complete yet
            log.add_round({"A": {"status": OK, "elapsed": 4.0}, "B": {"status": ERROR, "elapsed": 0.0}}, now + 10)
            pts = log.snapshot()["points"]
            self.assertEqual(len(pts), 1)
            self.assertAlmostEqual(pts[0]["targets"]["A"]["geomean"], 2.0)
            self.assertAlmostEqual(pts[0]["targets"]["B"]["geomean"], 0.1)
            self.assertEqual(pts[0]["targets"]["B"]["errors"], 1)
            # a timeout counts as the timeout value
            log.add_round({"A": {"status": TIMEOUT, "elapsed": 121.0}, "B": {"status": OK, "elapsed": 1.0}}, now + 20)
            log.add_round({"A": {"status": OK, "elapsed": 1.2}, "B": {"status": "unsupported"}}, now + 30)
            a = log.snapshot()["points"][1]["targets"]["A"]
            self.assertAlmostEqual(a["geomean"], math.sqrt(120.0 * 1.2), places=3)
            self.assertEqual(a["timeouts"], 1)
            # a restart reloads the points; a cut last line is ignored
            with open(path, "a") as fh:
                fh.write('{"t": 12')
            again = C.PassLog(path, keep_hours=24 * 365 * 100, timeout=120.0, n_queries=2)
            self.assertEqual(len(again.snapshot()["points"]), 2)
            # points older than keep_hours are dropped on load and from the file
            old = C.PassLog(path, keep_hours=1, timeout=120.0, n_queries=2)
            self.assertEqual(old.snapshot()["points"], [])
            with open(path) as fh:
                self.assertEqual([J.loads(x) for x in fh], [])


if __name__ == "__main__":
    unittest.main()
