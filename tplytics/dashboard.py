"""Live demo dashboard: runs the queries in a loop and shows them in a browser.

The dashboard has no query knowledge of its own. Everything it shows (name,
description, SQL, parameters, engine support) comes from the query files and
the engine layer, exactly as in demo mode, so adding an engine or a query
needs no change here.
"""
import json
import os
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import __version__
from . import params as P
from .modes import fmt_value
from .runner import Runner, OK
from .state import StateRefresher

PAGE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")


class QueryBoard:
    """Latest result and running statistics of one query."""

    def __init__(self, q, supported):
        self.q = q
        self.supported = supported
        self.last = None
        self.runs = self.ok = self.errors = self.timeouts = 0
        self.total = 0.0
        self.min = self.max = None

    def record(self, r, sql, values):
        self.runs += 1
        if r.status == OK:
            self.ok += 1
            self.total += r.elapsed
            self.min = r.elapsed if self.min is None else min(self.min, r.elapsed)
            self.max = r.elapsed if self.max is None else max(self.max, r.elapsed)
        elif r.status == "timeout":
            self.timeouts += 1
        else:
            self.errors += 1
        self.last = {
            "status": r.status, "elapsed": r.elapsed, "rows": r.rows, "error": r.error,
            "finished": time.time(), "sql": sql, "params": P.describe(values or {}),
            "columns": r.columns or [],
            "data": [[fmt_value(v) for v in row] for row in (r.data or [])],
        }

    def as_dict(self):
        q = self.q
        return {
            "id": q.id, "name": q.name, "complexity": q.complexity, "tables": q.tables,
            "description": q.description, "weight": q.weight, "supported": self.supported,
            "runs": self.runs, "ok": self.ok, "errors": self.errors, "timeouts": self.timeouts,
            "avg": self.total / self.ok if self.ok else None, "min": self.min, "max": self.max,
            "last": self.last,
        }


class Loop(threading.Thread):
    """Runs every supported query in turn, forever, until paused."""

    def __init__(self, ctx):
        super().__init__(daemon=True, name="dashboard-loop")
        a = ctx.args
        self.ctx = ctx
        self.boards = [QueryBoard(q, q.supported(ctx.engine)) for q in ctx.queries]
        self.lock = threading.Lock()
        self.running = threading.Event()
        if not a.start_paused:
            self.running.set()
        self.current = None
        self.cycle = 0
        self.rng = random.Random(a.seed)
        self.runner = Runner(ctx.engine, a.timeout, a.init_sql)

    def run(self):
        boards = [b for b in self.boards if b.supported]
        if not boards:
            return
        while True:
            self.cycle += 1
            for b in boards:
                self.running.wait()
                q = b.q
                try:
                    values = q.new_params(self.ctx.state, self.rng)
                    sql = q.render(self.ctx.engine, values)
                except Exception as e:  # noqa: BLE001
                    values, sql = {}, "-- could not generate parameters: %s" % e
                self.current = {"id": q.id, "started": time.time(), "sql": sql}
                r = self.runner.run(q.id, sql, keep_rows=self.ctx.args.show_rows, params=values)
                with self.lock:
                    b.record(r, sql, values)
                    self.current = None
                time.sleep(self.ctx.args.delay)

    def snapshot(self):
        with self.lock:
            st = self.ctx.state
            return {
                "version": __version__, "engine": self.ctx.engine.name,
                "server_version": self.ctx.server_version, "target": self.ctx.engine.describe(),
                "database": st.summary(), "data_to": st.t_max.strftime("%Y-%m-%d %H:%M:%S"),
                "running": self.running.is_set(), "cycle": self.cycle, "timeout": self.ctx.args.timeout,
                "current": self.current, "now": time.time(),
                "queries": [b.as_dict() for b in self.boards],
            }


def make_handler(loop):
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
                self._send(200, json.dumps(loop.snapshot()), "application/json")
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
    refresher = StateRefresher(ctx.engine, ctx.state, a.refresh) if a.refresh > 0 else None
    if refresher:
        refresher.start()
    loop.start()
    server = ThreadingHTTPServer((a.listen, a.http_port), make_handler(loop))
    host = "localhost" if a.listen in ("0.0.0.0", "") else a.listen
    print("dashboard on http://%s:%d/  (%s; Ctrl+C to stop)" % (host, a.http_port,
          "paused" if a.start_paused else "running"), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        if refresher:
            refresher.stop()
