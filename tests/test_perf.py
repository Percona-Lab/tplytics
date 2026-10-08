"""Performance sampler: averaging used by the 6 h / 24 h views (no host access needed)."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tplytics import perf as P  # noqa: E402


class PerfTests(unittest.TestCase):
    def test_average_and_downsample(self):
        pts = [{"t": 60.0 * i, "cpu_user": float(i), "load1": 1.0} for i in range(10)]
        self.assertEqual(P._average(pts[:2]), {"t": 60.0, "cpu_user": 0.5, "load1": 1.0})
        five = P._downsample(pts, 5)
        self.assertEqual([p["t"] for p in five], [240.0, 540.0])          # last time of each group
        self.assertEqual([p["cpu_user"] for p in five], [2.0, 7.0])
        self.assertEqual(len(P._downsample(pts, 1)), 10)
        part = P._downsample(pts[:7], 5)                                   # newest group partial
        self.assertEqual(part[-1]["cpu_user"], 5.5)

    def test_missing_fields_are_averaged_over_what_is_there(self):
        avg = P._average([{"t": 1, "db_qps": 10.0}, {"t": 2}, {"t": 3, "db_qps": 20.0}])
        self.assertEqual(avg, {"t": 3, "db_qps": 15.0})

    def test_ranges(self):
        self.assertEqual(P.RANGES["24h"], 86400)
        self.assertGreaterEqual(P.MINUTE_POINTS * 60, P.RANGES["24h"])
        self.assertGreaterEqual(P.RAW_POINTS * 5, P.RANGES["1h"])


if __name__ == "__main__":
    unittest.main()
