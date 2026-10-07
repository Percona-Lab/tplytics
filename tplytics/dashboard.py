"""Live demo dashboard: runs the queries one at a time and shows them in a browser.

Top of the page: system load (CPU, disk I/O) and database transactions/s.
Middle: the current query, either "running" or "waiting" (finished; the
result stays on screen until the next query starts). Bottom: the last N
finished queries, newest first.

The dashboard has no query knowledge of its own. Everything it shows (name,
description, SQL, parameters, engine support) comes from the query files and
the engine layer, exactly as in demo mode, so adding an engine or a query
needs no change here.
"""
import collections
import json
import os
import random
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import __version__
from . import params as P
from .modes import fmt_value
from .runner import Runner, OK
from .state import StateRefresher
from .stats import geomean

PAGE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
SERIES_POINTS = 180          # 3 minutes of 1-second samples


# ================================================================ system load
def _read_cpu():
    with open("/proc/stat") as fh:
        f = [int(x) for x in fh.readline().split()[1:]]
    user, nice, system, idle, iowait, irq, softirq, steal = (f + [0] * 8)[:8]
    return {"user": user + nice, "system": system + irq + softirq, "iowait": iowait,
            "steal": steal, "idle": idle}


def _disks():
    names = []
    for d in sorted(os.listdir("/sys/block")):
        if d.startswith(("loop", "ram", "zram", "dm-", "sr", "fd")):
            continue
        names.append(d)
    return set(names)


def _read_disk(disks):
    rd = wr = rio = wio = 0
    with open("/proc/diskstats") as fh:
        for line in fh:
            p = line.split()
            if len(p) >= 10 and p[2] in disks:
                rio += int(p[3]); rd += int(p[5]); wio += int(p[7]); wr += int(p[9])
    return {"read_bytes": rd * 512, "write_bytes": wr * 512, "read_ios": rio, "write_ios": wio}


class Sampler(threading.Thread):
    """Samples CPU, disk and database activity once a second (engine None: system only)."""

    def __init__(self, engine, system_stats):
        super().__init__(daemon=True, name="sampler")
        self.engine = engine
        self.system = system_stats and os.path.exists("/proc/stat") and os.path.isdir("/sys/block")
        self.disks = _disks() if self.system else set()
        self.series = collections.deque(maxlen=SERIES_POINTS)
        self.lock = threading.Lock()
        self.db_enabled = engine is not None and engine.activity_sql is not None
        self.qps_enabled = engine is not None and engine.queries_sql is not None

    def run(self):
        session = None
        prev = None
        while True:
            t = time.time()
            cur = {"t": t}
            if self.system:
                try:
                    cur["cpu"] = _read_cpu()
                    cur["disk"] = _read_disk(self.disks)
                except OSError:
                    self.system = False
            if self.db_enabled:
                try:
                    if session is None:
                        session = self.engine.connect()
                    cur["tx"] = self.engine.activity_counter(session)
                    if self.qps_enabled:
                        cur["q"] = self.engine.queries_counter(session)
                except Exception:  # noqa: BLE001 - counters are best effort
                    if session is not None:
                        session.close()
                    session = None
            if prev is not None:
                self._add_point(prev, cur)
            prev = cur
            time.sleep(max(0.05, 1.0 - (time.time() - t)))

    def _add_point(self, a, b):
        dt = max(0.001, b["t"] - a["t"])
        pt = {"t": round(b["t"], 1)}
        if "cpu" in a and "cpu" in b:
            d = {k: b["cpu"][k] - a["cpu"][k] for k in b["cpu"]}
            tot = max(1, sum(d.values()))
            pt["cpu_busy"] = round(100.0 * (tot - d["idle"] - d["iowait"]) / tot, 1)
            pt["cpu_iowait"] = round(100.0 * d["iowait"] / tot, 1)
        if "disk" in a and "disk" in b:
            d = {k: b["disk"][k] - a["disk"][k] for k in b["disk"]}
            pt["read_mb"] = round(d["read_bytes"] / dt / 1048576, 2)
            pt["write_mb"] = round(d["write_bytes"] / dt / 1048576, 2)
            pt["iops"] = round((d["read_ios"] + d["write_ios"]) / dt, 0)
        if a.get("tx") is not None and b.get("tx") is not None:
            pt["tps"] = round(max(0.0, b["tx"] - a["tx"]) / dt, 1)
        if a.get("q") is not None and b.get("q") is not None:
            pt["qps"] = round(max(0.0, b["q"] - a["q"]) / dt, 1)
        with self.lock:
            self.series.append(pt)

    def snapshot(self):
        with self.lock:
            return list(self.series)


# ================================================================ query loop
class Loop(threading.Thread):
    """Runs one query at a time, at most one start every --interval seconds."""

    def __init__(self, ctx):
        super().__init__(daemon=True, name="dashboard-loop")
        a = ctx.args
        self.ctx = ctx
        self.queries = [q for q in ctx.queries if q.supported(ctx.engine)]
        self.unsupported = [q.id for q in ctx.queries if not q.supported(ctx.engine)]
        self.lock = threading.Lock()
        self.running = threading.Event()
        if not a.start_paused:
            self.running.set()
        self.current = None
        self.history = collections.deque(maxlen=a.history)
        self.next_start = None
        self.executed = 0
        self.stats = {q.id: {"runs": 0, "ok": 0, "total": 0.0} for q in self.queries}
        # a round is one execution of every supported query (len(queries) executions)
        self.rounds = collections.deque(maxlen=a.rounds)
        self.rounds_done = 0
        self.round = None
        self.rng = random.Random(a.seed)
        self.runner = Runner(ctx.engine, a.timeout, a.init_sql)

    def _pick(self):
        if self.ctx.args.order == "weighted":
            return self.rng.choices(self.queries, weights=[q.weight["concurrent"] for q in self.queries])[0]
        q = self.queries[self.executed % len(self.queries)]
        return q

    def _wait_until(self, t):
        while True:
            self.running.wait()
            left = t - time.time()
            if left <= 0:
                return
            time.sleep(min(left, 0.2))

    def run(self):
        a = self.ctx.args
        if not self.queries:
            return
        while True:
            if self.next_start:
                self._wait_until(self.next_start)
            self.running.wait()
            q = self._pick()
            try:
                values = q.new_params(self.ctx.state, self.rng)
                sql = q.render(self.ctx.engine, values)
            except Exception as e:  # noqa: BLE001
                values, sql = {}, "-- could not generate parameters: %s" % e
            entry = {
                "seq": self.executed + 1, "id": q.id, "name": q.name, "complexity": q.complexity,
                "tables": q.tables, "description": q.description, "weight": q.weight,
                "params": P.describe(values), "sql": sql, "state": "running",
                "started": time.time(), "finished": None,
            }
            with self.lock:
                if self.current is not None:
                    self.history.appendleft(self.current)
                self.current = entry
                self.next_start = None
                if self.round is None:
                    self.round = {"n": self.rounds_done + 1, "started": time.time(), "finished": None,
                                  "times": [], "ok": 0, "errors": 0, "timeouts": 0, "executed": 0}
            started = time.time()
            r = self.runner.run(q.id, sql, keep_rows=a.show_rows, params=values)
            with self.lock:
                self.executed += 1
                st = self.stats[q.id]
                st["runs"] += 1
                if r.status == OK:
                    st["ok"] += 1
                    st["total"] += r.elapsed
                self._count_round(r)
                entry.update({
                    "state": "waiting", "status": r.status, "elapsed": r.elapsed, "rows": r.rows,
                    "error": r.error, "finished": time.time(), "columns": r.columns or [],
                    "data": [[fmt_value(v) for v in row] for row in (r.data or [])],
                    "avg": st["total"] / st["ok"] if st["ok"] else None, "runs": st["runs"],
                })
                # at most one query start per interval, and every result stays
                # visible for at least min_display seconds
                self.next_start = max(started + a.interval, time.time() + a.min_display)

    def _count_round(self, r):
        rd = self.round
        rd["executed"] += 1
        if r.status == OK:
            rd["ok"] += 1
            rd["times"].append(r.elapsed)
        elif r.status == "timeout":
            rd["timeouts"] += 1
        else:
            rd["errors"] += 1
        if rd["executed"] >= len(self.queries):
            rd["finished"] = time.time()
            rd["geomean"] = geomean(rd["times"])
            rd["total"] = sum(rd["times"])
            del rd["times"]
            self.rounds.appendleft(rd)
            self.rounds_done += 1
            self.round = None

    def overall_geomean(self):
        """Geometric mean of the per-query average times, once every query has run (round 1 done)."""
        if not self.rounds_done:
            return None
        return geomean([st["total"] / st["ok"] for st in self.stats.values() if st["ok"]])

    def snapshot(self):
        with self.lock:
            rd = self.round
            return {"current": dict(self.current) if self.current else None,
                    "history": [dict(h) for h in self.history],
                    "next_start": self.next_start, "executed": self.executed,
                    "queries_per_round": len(self.queries),
                    "geomean": self.overall_geomean(), "rounds_done": self.rounds_done,
                    "rounds": [dict(x) for x in self.rounds],
                    "round_progress": {"n": rd["n"], "executed": rd["executed"],
                                       "geomean": geomean(rd["times"])} if rd else None}



def make_handler(loop, sampler, ctx):
    def state():
        st = ctx.state
        d = loop.snapshot()
        d.update({
            "version": __version__, "engine": ctx.engine.name, "server_version": ctx.server_version,
            "target": ctx.engine.describe(), "host": socket.gethostname(),
            "database": st.summary(), "data_to": st.t_max.strftime("%Y-%m-%d %H:%M:%S"),
            "running": loop.running.is_set(), "timeout": ctx.args.timeout,
            "interval": ctx.args.interval, "order": ctx.args.order,
            "unsupported": loop.unsupported, "now": time.time(),
            "system_stats": sampler.system, "db_stats": sampler.db_enabled,
            "series": sampler.snapshot(),
        })
        return d

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype):
            data = body.encode() if isinstance(body, str) else body
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path in ("/", "/index.html"):
                with open(PAGE, "rb") as fh:
                    self._send(200, fh.read(), "text/html; charset=utf-8")
            elif self.path == "/api/state":
                self._send(200, json.dumps(state()), "application/json")
            else:
                self._send(404, "not found", "text/plain")

        def do_POST(self):
            if self.path == "/api/pause":
                loop.running.clear()
            elif self.path == "/api/resume":
                loop.running.set()
            else:
                return self._send(404, "not found", "text/plain")
            self._send(200, json.dumps({"running": loop.running.is_set()}), "application/json")

    return Handler


def dashboard(ctx):
    a = ctx.args
    loop = Loop(ctx)
    loop.runner.connect()
    sampler = Sampler(ctx.engine, a.system_stats == "auto")
    sampler.start()
    refresher = StateRefresher(ctx.engine, ctx.state, a.refresh) if a.refresh > 0 else None
    if refresher:
        refresher.start()
    loop.start()
    server = ThreadingHTTPServer((a.listen, a.http_port), make_handler(loop, sampler, ctx))
    host = "localhost" if a.listen in ("0.0.0.0", "") else a.listen
    print("dashboard on http://%s:%d/  (%s; Ctrl+C to stop)" % (host, a.http_port,
          "paused" if a.start_paused else "running"), flush=True)
    if not sampler.system and a.system_stats == "auto":
        print("note: /proc not available, CPU and disk charts are hidden", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        if refresher:
            refresher.stop()
