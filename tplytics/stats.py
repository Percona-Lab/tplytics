"""Statistics and text reports."""
import math
import statistics

from .runner import OK, ERROR, TIMEOUT, CANCELLED

# Response time distribution buckets (upper bounds, seconds)
BUCKETS = [0.01, 0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, float("inf")]


def bucket_label(b):
    if b == float("inf"):
        return ">%gs" % BUCKETS[-2]
    return "<=%gms" % (b * 1000) if b < 1 else "<=%gs" % b


def percentile(sorted_vals, p):
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = (len(sorted_vals) - 1) * p / 100.0
    f = math.floor(k)
    c = min(f + 1, len(sorted_vals) - 1)
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


class QueryStats:
    def __init__(self, query):
        self.query = query
        self.times = []        # successful executions only
        self.rows = []
        self.ok = self.errors = self.timeouts = 0
        self.error_samples = {}
        self.buckets = [0] * len(BUCKETS)

    def add(self, r):
        if r.status == CANCELLED:
            return
        if r.status == OK:
            self.ok += 1
            self.times.append(r.elapsed)
            self.rows.append(r.rows)
            for i, b in enumerate(BUCKETS):
                if r.elapsed <= b:
                    self.buckets[i] += 1
                    break
        elif r.status == TIMEOUT:
            self.timeouts += 1
            self.error_samples.setdefault("timeout", r.error)
        else:
            self.errors += 1
            key = (r.error or "")[:80]
            if len(self.error_samples) < 5:
                self.error_samples.setdefault(key, r.error)

    @property
    def runs(self):
        return self.ok + self.errors + self.timeouts

    def summary(self):
        t = sorted(self.times)
        d = {
            "id": self.query.id, "name": self.query.name, "complexity": self.query.complexity,
            "weight_serial": self.query.weight["serial"], "weight_concurrent": self.query.weight["concurrent"],
            "runs": self.runs, "ok": self.ok, "errors": self.errors, "timeouts": self.timeouts,
            "success_pct": round(100.0 * self.ok / self.runs, 2) if self.runs else None,
            "min": t[0] if t else None, "max": t[-1] if t else None,
            "avg": statistics.fmean(t) if t else None,
            "stddev": statistics.pstdev(t) if len(t) > 1 else 0.0 if t else None,
            "p50": percentile(t, 50), "p90": percentile(t, 90), "p95": percentile(t, 95),
            "p99": percentile(t, 99),
            "total_time": sum(t),
            "avg_rows": round(statistics.fmean(self.rows), 1) if self.rows else None,
            "distribution": dict([(bucket_label(b), n) for b, n in zip(BUCKETS, self.buckets)]
                                 + [("error", self.errors), ("timeout", self.timeouts)]),
            "errors_sample": list(self.error_samples.values()),
        }
        return d


def fmt_t(v):
    if v is None:
        return "-"
    if v < 1:
        return "%.1fms" % (v * 1000)
    if v < 100:
        return "%.2fs" % v
    return "%.0fs" % v


def table(headers, rows, align=None):
    """Plain text table."""
    rows = [["" if c is None else str(c) for c in r] for r in rows]
    w = [len(h) for h in headers]
    for r in rows:
        for i, c in enumerate(r):
            w[i] = max(w[i], len(c))
    align = align or ["<"] + [">"] * (len(headers) - 1)

    def fmt(cells):
        return "  ".join(format(c, "%s%d" % (align[i], w[i])) for i, c in enumerate(cells)).rstrip()
    out = [fmt(headers), "  ".join("-" * x for x in w)]
    for r in rows:
        out.append(fmt(r))
    return "\n".join(out)


def latency_table(summaries, extra=None):
    headers = ["query", "cx", "runs", "ok", "err", "tmo", "succ%", "min", "avg", "p50", "p95", "p99", "max", "rows"]
    if extra:
        headers += [h for h, _ in extra]
    rows = []
    for s in summaries:
        r = [s["id"] + " " + s["name"][:30], s["complexity"], s["runs"], s["ok"], s["errors"], s["timeouts"],
             "-" if s["success_pct"] is None else "%.1f" % s["success_pct"],
             fmt_t(s["min"]), fmt_t(s["avg"]), fmt_t(s["p50"]), fmt_t(s["p95"]), fmt_t(s["p99"]),
             fmt_t(s["max"]), "-" if s["avg_rows"] is None else s["avg_rows"]]
        if extra:
            r += [f(s) for _, f in extra]
        rows.append(r)
    return table(headers, rows)


def distribution_table(summaries):
    labels = [bucket_label(b) for b in BUCKETS] + ["error", "timeout"]
    headers = ["query"] + labels
    rows = []
    for s in summaries:
        n = s["runs"] or 1
        rows.append([s["id"]] + ["%.1f%%" % (100.0 * s["distribution"][l] / n) if s["distribution"][l] else "." for l in labels])
    return table(headers, rows)


def geomean(vals):
    vals = [v for v in vals if v and v > 0]
    if not vals:
        return None
    return math.exp(sum(math.log(v) for v in vals) / len(vals))


def error_report(summaries):
    out = []
    for s in summaries:
        for e in s["errors_sample"]:
            out.append("  %s: %s" % (s["id"], e))
    return "\n".join(out)
