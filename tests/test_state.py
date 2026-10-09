"""Discovery of the time ranges, and parameters drawn inside them."""
import datetime as dt
import os
import random
import re
import sys
import types
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tplytics import compare as C  # noqa: E402
from tplytics import params as P  # noqa: E402
from tplytics import queries as Q  # noqa: E402
from tplytics.state import DatabaseState, time_range  # noqa: E402

T0 = dt.datetime(2026, 10, 7, 9, 0)


class FakeSession:
    """A TPROC-C database of 4 warehouses x 10 districts, answering only the discovery queries.

    Orders 1..3000 of every district were loaded at T0; HammerDB then added
    orders 3001..3100, one per minute from T0 + 1 h. Delivery has reached order
    2500 (new_order holds 2501..3100). History ids 101..200 hold payments from
    T0 + 20 h to T0 + 30 h (the older ones were purged).
    """

    def __init__(self, history_id=True, backlog=True):
        self.history_id, self.backlog = history_id, backlog

    def entry(self, o_id):
        return T0 if o_id <= 3000 else T0 + dt.timedelta(hours=1, minutes=o_id - 3001)

    def execute(self, sql):
        s = " ".join(sql.split())
        if s.startswith("SELECT COUNT(*), MIN(w_id), MAX(w_id) FROM warehouse"):
            return None, [(4, 1, 4)]
        if s == "SELECT MAX(d_id) FROM district":
            return None, [(10,)]
        if s == "SELECT MAX(i_id) FROM item":
            return None, [(100000,)]
        if s.startswith("SELECT MAX(c_id) FROM customer"):
            return None, [(3000,)]
        if s.startswith("SELECT MIN(d_next_o_id)"):
            return None, [(3101, 3101, 40 * 3100)]
        pairs = [(int(w), int(d)) for w, d in re.findall(r"_w_id = (\d+) AND \w+_d_id = (\d+)", s)]
        if s.startswith("SELECT o_w_id, o_d_id, MIN(o_id), MAX(o_id), COUNT(*) FROM orders"):
            return None, [(w, d, 1, 3100, 3100) for w, d in pairs]
        if s.startswith("SELECT no_w_id, no_d_id, MIN(no_o_id) FROM new_order"):
            return None, [(w, d, 2501) for w, d in pairs] if self.backlog else []
        if s.startswith("SELECT o_w_id, o_d_id, MAX(o_id) FROM orders"):
            return None, [(int(w), int(d), int(b) - 1)
                          for w, d, b in re.findall(r"o_w_id = (\d+) AND o_d_id = (\d+) AND o_id < (\d+)", s)]
        if s.startswith("SELECT o_w_id, o_d_id, o_id, o_entry_d FROM orders"):
            out = []
            for w, d, ids in re.findall(r"o_w_id = (\d+) AND o_d_id = (\d+) AND o_id IN \(([\d, ]+)\)", s):
                out += [(int(w), int(d), int(o), self.entry(int(o))) for o in ids.split(", ")]
            return None, out
        if s == "SELECT MIN(id), MAX(id) FROM history":
            if not self.history_id:
                raise RuntimeError("Unknown column 'id'")
            return None, [(101, 200)]
        m = re.match(r"SELECT h_date FROM history WHERE id = (\d+)", s)
        if m:
            return None, [(T0 + dt.timedelta(hours=20 + (int(m.group(1)) - 101) / 99 * 10),)]
        if s == "SELECT MIN(h_date), MAX(h_date) FROM history":
            return None, [(T0 + dt.timedelta(hours=5), T0 + dt.timedelta(hours=6))]
        raise AssertionError("unexpected discovery query: " + s)


class StateTests(unittest.TestCase):
    def test_ranges_from_discovery(self):
        st = DatabaseState(FakeSession())
        last = T0 + dt.timedelta(hours=1, minutes=99)
        self.assertEqual(st.ranges["orders"], (T0, last))
        # delivered up to order 2500, which was loaded at T0
        self.assertEqual(st.ranges["delivered"], (T0, T0))
        self.assertEqual(st.ranges["history"], (T0 + dt.timedelta(hours=20), T0 + dt.timedelta(hours=30)))
        self.assertIn("payments from", st.summary())

    def test_no_backlog_and_plain_history(self):
        st = DatabaseState(FakeSession(history_id=False, backlog=False))
        self.assertEqual(st.ranges["delivered"], st.ranges["orders"])
        self.assertEqual(st.ranges["history"], (T0 + dt.timedelta(hours=5), T0 + dt.timedelta(hours=6)))

    def test_windows_fall_in_their_source(self):
        st = DatabaseState(FakeSession())
        rng = random.Random(1)
        for src in ("history", "delivered", "orders"):
            lo, hi = time_range(st, src)
            spec = {"win": {"type": "time_window", "source": src, "fraction": [0.05, 0.5], "anchor": "mixed"}}
            for _ in range(50):
                v = P.generate(spec, st, rng)
                self.assertGreaterEqual(v["win_start"], lo - dt.timedelta(seconds=1), src)
                self.assertLessEqual(v["win_end"], hi + dt.timedelta(seconds=61), src)
        for q in Q.load():                       # every shipped query draws its parameters from this state
            q.new_params(st, rng)

    def test_missing_source_falls_back_to_orders(self):
        st = types.SimpleNamespace(t_min=T0, t_max=T0 + dt.timedelta(hours=1), ranges={"history": None})
        self.assertEqual(time_range(st, "history"), (st.t_min, st.t_max))
        with self.assertRaises(ValueError):
            time_range(st, "payments")

    def test_common_ranges_are_the_overlap(self):
        live = DatabaseState(FakeSession())
        copy = DatabaseState(FakeSession())
        copy.ranges = dict(copy.ranges, history=(T0 + dt.timedelta(hours=21), T0 + dt.timedelta(hours=29)))
        copy.t_max = T0 + dt.timedelta(hours=1, minutes=90)
        cs = C.CommonState([live, copy])
        r = cs.ranges
        self.assertEqual(r["history"], (T0 + dt.timedelta(hours=21), T0 + dt.timedelta(hours=29)))
        self.assertEqual(r["orders"], (T0, copy.t_max))
        self.assertEqual(r["delivered"], (T0, T0))
        copy.ranges = dict(copy.ranges, history=None)    # a target without payments
        self.assertIsNone(cs.ranges["history"])
        self.assertEqual(time_range(cs, "history"), (cs.t_min, cs.t_max))


if __name__ == "__main__":
    unittest.main()
