"""The three benchmark modes: demo, serial and concurrent."""
import csv
import datetime as dt
import decimal
import json
import random
import sys
import threading
import time

from . import params as P
from . import stats as S
from .runner import Runner, OK, ERROR, TIMEOUT, CANCELLED
from .state import StateRefresher, refresh_state


def out(*a):
    print(*a, flush=True)


def fmt_value(v):
    if v is None:
        return "NULL"
    if isinstance(v, float):
        return "%.6g" % v
    if isinstance(v, decimal.Decimal):
        return format(v.normalize(), "f") if v == v.to_integral() else str(v)
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat(sep=" ") if isinstance(v, dt.datetime) else v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "replace")
    return str(v)


def json_default(o):
    if isinstance(o, (dt.datetime, dt.date)):
        return o.isoformat()
    if isinstance(o, decimal.Decimal):
        return float(o)
    return str(o)


def supported_queries(queries, engine, mode):
    run, skipped = [], []
    for q in queries:
        if not q.supported(engine):
            skipped.append(q)
        elif mode is None or q.weight[mode] > 0:
            run.append(q)
    return run, skipped


class RawLog:
    """Optional CSV log of every execution."""

    def __init__(self, path):
        self.fh = open(path, "w", newline="") if path else None
        self.lock = threading.Lock()
        if self.fh:
            self.w = csv.writer(self.fh)
            self.w.writerow(["query", "thread", "start", "elapsed_s", "status", "rows", "error", "params"])

    def add(self, r):
        if not self.fh:
            return
        with self.lock:
            self.w.writerow([r.query_id, r.thread, "%.3f" % r.start, "%.6f" % r.elapsed, r.status, r.rows,
                             r.error or "", P.describe(r.params or {})])

    def close(self):
        if self.fh:
            self.fh.close()


def write_json(path, doc):
    if path:
        with open(path, "w") as fh:
            json.dump(doc, fh, indent=2, default=json_default)
        out("results written to %s" % path)


# ================================================================== demo
def demo(ctx):
    a, engine, state = ctx.args, ctx.engine, ctx.state
    rng = random.Random(a.seed)
    runner = Runner(engine, a.timeout, a.init_sql)
    runner.connect()
    results = []
    for q in ctx.queries:
        out("=" * 100)
        out("%s  %s   [complexity %d, weight serial %g / concurrent %g, tables: %s]"
            % (q.id, q.name, q.complexity, q.weight["serial"], q.weight["concurrent"], ", ".join(q.tables)))
        out("-" * 100)
        out(q.description)
        if not q.supported(engine):
            out("\n>>> NOT SUPPORTED by %s - skipped" % engine.name)
            results.append({"id": q.id, "status": "unsupported"})
            continue
        for v in range(a.variants):
            values = q.new_params(state, rng)
            sql = q.render(engine, values)
            out("\nParameters: %s" % P.describe(values))
            out("\n" + sql + ";\n")
            r = runner.run(q.id, sql, keep_rows=a.show_rows, params=values)
            if r.status == OK:
                out(">>> %s: %s, %d row%s" % ("OK", S.fmt_t(r.elapsed), r.rows, "" if r.rows == 1 else "s"))
                if r.data:
                    out(S.table(r.columns, [[fmt_value(x) for x in row] for row in r.data],
                                align=[">" if isinstance(x, (int, float, decimal.Decimal)) else "<" for x in r.data[0]]))
                    if r.rows > len(r.data):
                        out("... %d more row%s" % (r.rows - len(r.data), "" if r.rows - len(r.data) == 1 else "s"))
            else:
                out(">>> %s after %s: %s" % (r.status.upper(), S.fmt_t(r.elapsed), r.error))
            results.append({"id": q.id, "status": r.status, "elapsed": r.elapsed, "rows": r.rows,
                            "error": r.error, "params": values})
        if a.pause and sys.stdin.isatty():
            try:
                input("\n[Enter] next query ")
            except EOFError:
                pass
    runner.close()
    out("=" * 100)
    out(S.table(["query", "status", "time", "rows"],
                [[r["id"], r["status"], S.fmt_t(r.get("elapsed")), r.get("rows", "-")] for r in results]))
    write_json(a.json, ctx.report_doc("demo", {"results": results}))


# ================================================================ serial
def serial(ctx):
    a, engine, state = ctx.args, ctx.engine, ctx.state
    rng = random.Random(a.seed)
    queries, skipped = supported_queries(ctx.queries, engine, "serial")
    plan = []
    for q in queries:
        n = max(1, int(round(a.iterations * q.weight["serial"])))
        plan.append((q, n))
    total = sum(n for _, n in plan)
    out("serial mode: %d queries, %d executions (iterations %d x serial weight), timeout %gs, order %s"
        % (len(plan), total, a.iterations, a.timeout, a.order))
    for q in skipped:
        out("  %s %s: not supported by %s, skipped" % (q.id, q.name, engine.name))

    if a.order == "grouped":
        sequence = [q for q, n in plan for _ in range(n)]
    elif a.order == "interleaved":
        left = {q.id: n for q, n in plan}
        sequence = []
        while any(left.values()):
            for q, _ in plan:
                if left[q.id]:
                    sequence.append(q)
                    left[q.id] -= 1
    else:
        sequence = [q for q, n in plan for _ in range(n)]
        rng.shuffle(sequence)

    st = {q.id: S.QueryStats(q) for q, _ in plan}
    raw = RawLog(a.raw)
    runner = Runner(engine, a.timeout, a.init_sql)
    runner.connect()

    if a.warmup_runs:
        out("warm-up: %d run(s) of every query (not counted)" % a.warmup_runs)
        for q, _ in plan:
            for _ in range(a.warmup_runs):
                runner.run_query(q, state, rng)

    started = time.time()
    last_progress = started
    done = 0
    for i, q in enumerate(sequence):
        if i and i % len(plan) == 0:
            # a round is one execution per query: re-read the database before the next one
            err = refresh_state(engine, state)
            if err:
                out("  could not re-read the database, keeping the previous ranges: %s" % err)
        r = runner.run_query(q, state, rng)
        st[q.id].add(r)
        raw.add(r)
        done += 1
        if a.verbose:
            out("  %-4s %-8s %10s %6d rows  %s%s" % (q.id, r.status, S.fmt_t(r.elapsed), r.rows,
                                                     P.describe(r.params or {}), "  " + r.error if r.error else ""))
        last_of_query = (i + 1 == len(sequence)) or sequence[i + 1].id != q.id
        if a.order == "grouped" and last_of_query:
            s = st[q.id].summary()
            out("%-4s %-38s %4d runs  ok %-4d err %-3d tmo %-3d  avg %-9s min %-9s max %s"
                % (q.id, q.name[:38], s["runs"], s["ok"], s["errors"], s["timeouts"],
                   S.fmt_t(s["avg"]), S.fmt_t(s["min"]), S.fmt_t(s["max"])))
        elif a.order != "grouped" and time.time() - last_progress >= a.report_interval:
            last_progress = time.time()
            out("  progress: %d/%d executions, %s elapsed" % (done, total, S.fmt_t(last_progress - started)))
    elapsed = time.time() - started
    runner.close()
    raw.close()

    summaries = [st[q.id].summary() for q, _ in plan]
    out("\n" + "=" * 100)
    out("SERIAL RESULTS  %s  -  %s" % (ctx.server_version, state.summary()))
    out("=" * 100)
    out(S.latency_table(summaries, extra=[("stddev", lambda s: S.fmt_t(s["stddev"]))]))
    out("\nResponse time distribution (share of executions):")
    out(S.distribution_table(summaries))
    errs = S.error_report(summaries)
    if errs:
        out("\nErrors and timeouts:\n" + errs)
    ok = sum(s["ok"] for s in summaries)
    runs = sum(s["runs"] for s in summaries)
    totals = {
        "executions": runs, "ok": ok, "errors": sum(s["errors"] for s in summaries),
        "timeouts": sum(s["timeouts"] for s in summaries),
        "success_pct": round(100.0 * ok / runs, 2) if runs else None,
        "wall_time_s": round(elapsed, 3),
        "sum_of_avg_s": sum(s["avg"] for s in summaries if s["avg"] is not None),
        "geomean_avg_s": S.geomean([s["avg"] for s in summaries]),
        "queries_without_success": [s["id"] for s in summaries if not s["ok"]],
        "skipped_unsupported": [q.id for q in skipped],
    }
    out("\nTotal: %d executions in %s, %d ok, %d errors, %d timeouts (%.1f%% success)"
        % (runs, S.fmt_t(elapsed), ok, totals["errors"], totals["timeouts"], totals["success_pct"] or 0))
    out("Sum of per-query averages: %s   geometric mean of per-query averages: %s"
        % (S.fmt_t(totals["sum_of_avg_s"]), S.fmt_t(totals["geomean_avg_s"])))
    write_json(a.json, ctx.report_doc("serial", {"totals": totals, "queries": summaries}))


# ============================================================ concurrent
class Worker(threading.Thread):
    def __init__(self, idx, ctx, queries, weights, start_evt, stop_evt, sink):
        super().__init__(daemon=True, name="worker-%d" % idx)
        self.idx, self.ctx, self.queries, self.weights = idx, ctx, queries, weights
        self.start_evt, self.stop_evt, self.sink = start_evt, stop_evt, sink
        a = ctx.args
        self.rng = random.Random(None if a.seed is None else a.seed * 1000 + idx)
        self.runner = Runner(ctx.engine, a.timeout, a.init_sql, thread=idx)
        self.connect_error = None

    def prepare(self):
        try:
            self.runner.connect()
        except Exception as e:
            self.connect_error = e

    def run(self):
        a = self.ctx.args
        self.start_evt.wait()
        # spread the first queries out a little so all threads do not start in lock-step
        if self.stop_evt.wait(self.rng.uniform(0, min(1.0, a.think or 0.2))):
            return
        while not self.stop_evt.is_set():
            q = self.rng.choices(self.queries, weights=self.weights)[0]
            r = self.runner.run_query(q, self.ctx.state, self.rng)
            self.sink(r)
            if a.think > 0:
                pause = a.think / q.weight["concurrent"]
                if a.think_jitter:
                    pause *= self.rng.uniform(1 - a.think_jitter, 1 + a.think_jitter)
                if self.stop_evt.wait(pause):
                    break
        self.runner.close()


def concurrent(ctx):
    a, engine, state = ctx.args, ctx.engine, ctx.state
    queries, skipped = supported_queries(ctx.queries, engine, "concurrent")
    if not queries:
        raise SystemExit("no supported query with a concurrent weight > 0")
    weights = [q.weight["concurrent"] for q in queries]
    wsum = sum(weights)
    out("concurrent mode: %d threads, %s measured (+%s warm-up), timeout %gs, think %gs/weight (jitter %g)"
        % (a.threads, S.fmt_t(a.duration), S.fmt_t(a.warmup), a.timeout, a.think, a.think_jitter))
    out(S.table(["query", "weight", "probability"],
                [[q.id + " " + q.name, "%g" % q.weight["concurrent"], "%.1f%%" % (100 * q.weight["concurrent"] / wsum)]
                 for q in queries]))
    for q in skipped:
        out("  %s %s: not supported by %s, skipped" % (q.id, q.name, engine.name))

    lock = threading.Lock()
    results = []
    raw = RawLog(a.raw)

    def sink(r):
        with lock:
            results.append(r)
        raw.add(r)

    start_evt, stop_evt = threading.Event(), threading.Event()
    workers = [Worker(i + 1, ctx, queries, weights, start_evt, stop_evt, sink) for i in range(a.threads)]
    out("opening %d connections ..." % len(workers))
    preps = [threading.Thread(target=w.prepare) for w in workers]
    for t in preps:
        t.start()
    for t in preps:
        t.join()
    bad = [w for w in workers if w.connect_error]
    if bad:
        raise SystemExit("%d connection(s) failed: %s" % (len(bad), bad[0].connect_error))

    refresher = StateRefresher(engine, state, a.refresh) if a.refresh > 0 else None
    if refresher:
        refresher.start()
    for w in workers:
        w.start()
    t0 = time.time()
    ctx.measure_start = t0 + a.warmup
    ctx.end_time = ctx.measure_start + a.duration
    start_evt.set()

    timeline = []
    seen = 0
    prev_t = t0
    try:
        while True:
            now = time.time()
            if now >= ctx.end_time:
                break
            time.sleep(min(a.report_interval, ctx.end_time - now))
            now = time.time()
            with lock:
                new = results[seen:]
                seen = len(results)
            interval = now - prev_t
            prev_t = now
            n_ok = sum(1 for r in new if r.status == OK)
            n_err = sum(1 for r in new if r.status == ERROR)
            n_tmo = sum(1 for r in new if r.status == TIMEOUT)
            phase = "warm-up" if now <= ctx.measure_start + 0.5 else "measure"
            lat = sorted(r.elapsed for r in new if r.status == OK)
            point = {"t": round(now - t0, 1), "phase": phase, "qps": round(len(new) / interval, 3),
                     "ok": n_ok, "errors": n_err, "timeouts": n_tmo,
                     "p50": S.percentile(lat, 50), "p95": S.percentile(lat, 95)}
            timeline.append(point)
            out("[%6.0fs %-7s] qps %7.2f  ok %4d  err %3d  tmo %3d  p50 %-9s p95 %-9s  data to %s"
                % (point["t"], phase, point["qps"], n_ok, n_err, n_tmo, S.fmt_t(point["p50"]),
                   S.fmt_t(point["p95"]), state.t_max.strftime("%H:%M:%S")))
    except KeyboardInterrupt:
        out("interrupted - stopping")
        ctx.end_time = time.time()
    stop_evt.set()
    for w in workers:
        w.runner.stop()
    for w in workers:
        w.join(timeout=30)
    if refresher:
        refresher.stop()
    raw.close()

    measured = [r for r in results if r.status != CANCELLED
                and r.start + r.elapsed >= ctx.measure_start and r.start + r.elapsed <= ctx.end_time + 0.001]
    span = max(0.001, ctx.end_time - ctx.measure_start)
    st = {q.id: S.QueryStats(q) for q in queries}
    for r in measured:
        st[r.query_id].add(r)
    summaries = [st[q.id].summary() for q in queries]
    for s in summaries:
        s["share_pct"] = round(100.0 * s["runs"] / len(measured), 2) if measured else 0
        s["qps"] = round(s["runs"] / span, 4)
    ok = sum(s["ok"] for s in summaries)
    totals = {
        "threads": a.threads, "measured_s": round(span, 3), "executions": len(measured), "ok": ok,
        "errors": sum(s["errors"] for s in summaries), "timeouts": sum(s["timeouts"] for s in summaries),
        "success_pct": round(100.0 * ok / len(measured), 2) if measured else None,
        "qps": round(len(measured) / span, 4), "qps_ok": round(ok / span, 4),
        "cancelled_at_end": sum(1 for r in results if r.status == CANCELLED),
        "skipped_unsupported": [q.id for q in skipped],
    }
    all_ok = sorted(r.elapsed for r in measured if r.status == OK)
    totals.update({"p50": S.percentile(all_ok, 50), "p95": S.percentile(all_ok, 95),
                   "p99": S.percentile(all_ok, 99)})

    out("\n" + "=" * 100)
    out("CONCURRENT RESULTS  %s  -  %s" % (ctx.server_version, state.summary()))
    out("=" * 100)
    out(S.latency_table(summaries, extra=[("share%", lambda s: "%.1f" % s["share_pct"]),
                                          ("qps", lambda s: "%.3f" % s["qps"])]))
    out("\nResponse time distribution (share of executions):")
    out(S.distribution_table(summaries))
    errs = S.error_report(summaries)
    if errs:
        out("\nErrors and timeouts:\n" + errs)
    out("\nThreads %d, measured %s: %d executions, %d ok, %d errors, %d timeouts (%.1f%% success)"
        % (a.threads, S.fmt_t(span), len(measured), ok, totals["errors"], totals["timeouts"], totals["success_pct"] or 0))
    out("Throughput: %.3f QPS (%.3f successful QPS); all queries p50 %s, p95 %s, p99 %s; %d in-flight cancelled at end"
        % (totals["qps"], totals["qps_ok"], S.fmt_t(totals["p50"]), S.fmt_t(totals["p95"]), S.fmt_t(totals["p99"]),
           totals["cancelled_at_end"]))
    write_json(a.json, ctx.report_doc("concurrent", {"totals": totals, "queries": summaries, "timeline": timeline}))
