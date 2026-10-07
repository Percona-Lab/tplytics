"""Side-by-side dashboard: the same queries on several targets at once.

Every round runs one query, with the same parameters, on every target at the
same time (for example a MySQL replica and the DBTrail copy of the same
primary), and the page shows the targets next to each other: replication lag
from the heartbeat, the geometric mean of every execution so far, host load,
the current query and the recent rounds, plus how the targets compare.

Targets, and optionally the primary, are described in a TOML file:

    [dashboard]                       # any compare option, e.g. listen, http_port, interval
    listen = "0.0.0.0"
    http_port = 8080

    [primary]                         # optional: transactions/s of the primary
    label = "lab-hdb (primary)"
    engine = "mysql"
    defaults_file = "/etc/parmaham/mysql-monitor.cnf"

    [[target]]
    label = "MySQL replica"
    engine = "mysql"
    defaults_file = "/etc/tplytics/replica.cnf"
    status_url = "http://192.168.193.161:8091"   # heartbeat monitor with --system-stats

    [[target]]
    label = "DBTrail"
    engine = "dbtrail"
    defaults_file = "/etc/tplytics/dbtrail.cnf"
    status_url = "http://192.168.230.14:8091"

A target's connection keys are the command line's (host, port, socket, user,
password, database, defaults_file, init_sql, schema). Without status_url the
lag is read from the heartbeat table on the target itself and host load is
not shown.
"""
import collections
import json
import math
import os
import random
import socket
import threading
import time
import types
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import __version__
from . import params as P
from . import queries as Q
from .dashboard import Sampler
from .engines import get_engine
from .heartbeat import DEFAULT_TABLE, lag_of, read_heartbeat
from .modes import fmt_value
from .runner import OK, TIMEOUT, Runner
from .state import DatabaseState, StateRefresher, human_duration

PAGE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "compare.html")
LAG_STEP = 5.0               # seconds between points of the lag history
LAG_POINTS = 180             # 15 minutes
CONN_KEYS = ("engine", "host", "port", "socket", "user", "password", "database", "schema",
             "defaults_file", "init_sql")
TARGET_KEYS = CONN_KEYS + ("label", "status_url", "heartbeat_table", "heartbeat_id")


# =================================================================== config
def _load_toml(path):
    try:
        import tomllib
    except ImportError:          # Python < 3.11
        import tomli as tomllib
    with open(path, "rb") as fh:
        return tomllib.load(fh)


def target_opts(spec, what):
    """A target table from the config as the namespace the engines expect."""
    unknown = set(spec) - set(TARGET_KEYS)
    if unknown:
        raise SystemExit("%s: unknown key(s) %s (known: %s)" % (what, ", ".join(sorted(unknown)),
                                                                 ", ".join(TARGET_KEYS)))
    o = {k: None for k in TARGET_KEYS}
    o.update(database="tpcc", engine="mysql", init_sql=[], heartbeat_table=DEFAULT_TABLE, heartbeat_id=1)
    o.update(spec)
    if o["port"] is not None:
        o["port"] = str(o["port"])
    o["label"] = o["label"] or o["engine"]
    return types.SimpleNamespace(**o)


def load_config(path):
    doc = _load_toml(path)
    targets = [target_opts(t, "target %d" % (i + 1)) for i, t in enumerate(doc.get("target", []))]
    if not targets:
        raise SystemExit("%s: no [[target]] tables" % path)
    if len({t.label for t in targets}) != len(targets):
        raise SystemExit("%s: target labels must be unique" % path)
    primary = target_opts(doc["primary"], "primary") if "primary" in doc else None
    return doc.get("dashboard", {}), primary, targets


# ============================================================ common state
class CommonState:
    """What every target has: same database, time range limited to the overlap.

    Drawing parameters from it gives every target the same statement over data
    all of them have, even when one of them (a copy refreshed every few
    minutes) is behind the others.
    """

    def __init__(self, states):
        self.states = states

    def __getattr__(self, name):
        if name in ("t_min", "t_max", "span_seconds"):
            raise AttributeError(name)
        return getattr(self.states[0], name)

    @property
    def t_min(self):
        return max(s.t_min for s in self.states)

    @property
    def t_max(self):
        return max(self.t_min, min(s.t_max for s in self.states))

    @property
    def span_seconds(self):
        return max(1.0, (self.t_max - self.t_min).total_seconds())


# ================================================================ statistics
class GeoStats:
    """Count, errors and the geometric mean of successful execution times."""

    def __init__(self):
        self.ok = self.errors = self.timeouts = 0
        self.logsum = 0.0
        self.total = 0.0

    def add(self, status, elapsed):
        if status == OK:
            self.ok += 1
            self.total += elapsed
            self.logsum += math.log(max(elapsed, 1e-4))
        elif status == TIMEOUT:
            self.timeouts += 1
        else:
            self.errors += 1

    @property
    def geomean(self):
        return math.exp(self.logsum / self.ok) if self.ok else None

    @property
    def avg(self):
        return self.total / self.ok if self.ok else None

    def as_dict(self):
        return {"ok": self.ok, "errors": self.errors, "timeouts": self.timeouts,
                "runs": self.ok + self.errors + self.timeouts, "geomean": self.geomean, "avg": self.avg}


class PairStats:
    """Geometric mean of time(base) / time(other) over rounds where both succeeded."""

    def __init__(self):
        self.n = 0
        self.logsum = 0.0

    def add(self, base, other):
        self.n += 1
        self.logsum += math.log(max(base, 1e-4) / max(other, 1e-4))

    @property
    def ratio(self):
        return math.exp(self.logsum / self.n) if self.n else None


# =================================================================== targets
class Target:
    def __init__(self, opts, refresh):
        self.opts = opts
        self.label = opts.label
        self.engine = get_engine(opts.engine, opts)
        session = self.engine.connect()
        self.server_version = session.server_version
        self.state = DatabaseState(session)
        session.close()
        self.refresher = StateRefresher(self.engine, self.state, refresh) if refresh > 0 else None
        self.status = {"lag": None, "lag_state": "starting", "series": [], "system_stats": False,
                       "host": None, "cpus": None}
        self.lag_series = collections.deque(maxlen=LAG_POINTS)
        self.lock = threading.Lock()

    def add_lag(self, t, lag):
        """One lag point every LAG_STEP seconds: the history behind the big number."""
        if not self.lag_series or t - self.lag_series[-1]["t"] >= LAG_STEP:
            self.lag_series.append({"t": round(t, 1), "lag": lag})

    def describe(self):
        return self.engine.describe()


class StatusPoller(threading.Thread):
    """Lag and host load of one target, from its heartbeat monitor or the table itself."""

    def __init__(self, target, interval=1.0):
        super().__init__(daemon=True, name="status-%s" % target.label)
        self.t, self.interval = target, interval
        self.session = None

    def _get(self, path):
        url = self.t.opts.status_url.rstrip("/") + path
        with urllib.request.urlopen(url, timeout=3) as r:
            return json.load(r)

    def poll_url(self):
        upd = {}
        try:
            hb = self._get("/api/heartbeat")
            upd.update(lag=hb.get("lag") if hb.get("state") == "ok" else None, lag_state=hb.get("state"),
                       lag_error=hb.get("error"), heartbeat_at=hb.get("heartbeat_at"),
                       lag_stats=hb.get("stats"))
        except Exception as e:  # noqa: BLE001
            upd.update(lag=None, lag_state="monitor unreachable", lag_error=str(e)[:200])
        try:
            sysd = self._get("/api/system")
            upd.update(series=sysd.get("series", []), system_stats=sysd.get("system_stats", False),
                       host=sysd.get("host"), cpus=sysd.get("cpus"))
        except Exception:  # noqa: BLE001 - load is optional
            upd.update(system_stats=False)
        return upd

    def poll_table(self):
        o = self.t.opts
        try:
            if self.session is None:
                self.session = self.t.engine.connect()
            t0 = time.time()
            hb = read_heartbeat(self.session, o.heartbeat_table, int(o.heartbeat_id))
            if hb is None:
                return {"lag": None, "lag_state": "no heartbeat", "lag_error": "no heartbeat row"}
            seq, ts_us, interval_us, _ = hb
            return {"lag": round(lag_of(t0, ts_us, interval_us), 3), "lag_state": "ok", "lag_error": None,
                    "heartbeat_at": ts_us / 1e6}
        except Exception as e:  # noqa: BLE001
            if self.session is not None:
                self.session.close()
            self.session = None
            return {"lag": None, "lag_state": "error", "lag_error": str(e)[:200]}

    def run(self):
        while True:
            t = time.time()
            upd = self.poll_url() if self.t.opts.status_url else self.poll_table()
            with self.t.lock:
                self.t.status.update(upd)
                self.t.add_lag(t, upd.get("lag"))
            time.sleep(max(0.1, (self.interval if self.t.opts.status_url else 2.0) - (time.time() - t)))


# ================================================================ round loop
class CompareLoop(threading.Thread):
    """One query per round, the same statement on every target at the same time."""

    def __init__(self, args, targets, queries):
        super().__init__(daemon=True, name="compare-loop")
        self.args, self.targets = args, targets
        self.queries = [q for q in queries if any(q.supported(t.engine) for t in targets)]
        self.state = CommonState([t.state for t in targets])
        self.runners = {t.label: Runner(t.engine, args.timeout, t.opts.init_sql or []) for t in targets}
        self.lock = threading.Lock()
        self.running = threading.Event()
        if not args.start_paused:
            self.running.set()
        self.current = None
        self.history = collections.deque(maxlen=args.history)
        self.next_start = None
        self.rounds = 0
        self.rng = random.Random(args.seed)
        self.overall = {t.label: GeoStats() for t in targets}
        self.per_query = {q.id: {t.label: GeoStats() for t in targets} for q in self.queries}
        base = targets[0].label
        self.base = base
        self.pairs = {t.label: PairStats() for t in targets[1:]}
        self.pairs_q = {q.id: {t.label: PairStats() for t in targets[1:]} for q in self.queries}

    def _pick(self):
        if self.args.order == "weighted":
            return self.rng.choices(self.queries, weights=[q.weight["concurrent"] for q in self.queries])[0]
        return self.queries[self.rounds % len(self.queries)]

    def _wait_until(self, t):
        while True:
            self.running.wait()
            left = t - time.time()
            if left <= 0:
                return
            time.sleep(min(left, 0.2))

    def _run_one(self, t, q, sql, values, res):
        r = self.runners[t.label].run(q.id, sql, keep_rows=self.args.show_rows, params=values)
        with self.lock:
            res.update({"state": "done", "status": r.status, "elapsed": r.elapsed, "rows": r.rows,
                        "error": r.error, "finished": time.time(), "columns": r.columns or [],
                        "data": [[fmt_value(v) for v in row] for row in (r.data or [])]})
            self.overall[t.label].add(r.status, r.elapsed)
            st = self.per_query[q.id][t.label]
            st.add(r.status, r.elapsed)
            res["avg"] = st.avg

    def run(self):
        a = self.args
        if not self.queries:
            return
        while True:
            if self.next_start:
                self._wait_until(self.next_start)
            self.running.wait()
            q = self._pick()
            try:
                values = q.new_params(self.state, self.rng)
            except Exception as e:  # noqa: BLE001
                values = None
                perr = "could not generate parameters: %s" % e
            results, threads = {}, []
            started = time.time()
            for t in self.targets:
                res = {"state": "running", "started": started, "finished": None}
                if values is None:
                    res.update(state="done", status="error", error=perr, elapsed=0.0, rows=0, sql="")
                elif not q.supported(t.engine):
                    res.update(state="done", status="unsupported", error="not supported by %s" % t.engine.name,
                               elapsed=None, rows=0, sql="")
                else:
                    res["sql"] = q.render(t.engine, values)
                    th = threading.Thread(target=self._run_one, args=(t, q, res["sql"], values, res), daemon=True)
                    threads.append(th)
                results[t.label] = res
            entry = {"seq": self.rounds + 1, "id": q.id, "name": q.name, "complexity": q.complexity,
                     "tables": q.tables, "description": q.description,
                     "params": P.describe(values) if values else "", "state": "running",
                     "started": started, "finished": None, "results": results}
            with self.lock:
                if self.current is not None:
                    self.history.appendleft(self.current)
                self.current = entry
                self.next_start = None
            for th in threads:
                th.start()
            for th in threads:
                th.join()
            with self.lock:
                self.rounds += 1
                b = results[self.base]
                for lbl, ps in self.pairs.items():
                    o = results[lbl]
                    if b.get("status") == OK and o.get("status") == OK:
                        ps.add(b["elapsed"], o["elapsed"])
                        self.pairs_q[q.id][lbl].add(b["elapsed"], o["elapsed"])
                        o["speedup"] = max(b["elapsed"], 1e-4) / max(o["elapsed"], 1e-4)
                entry.update(state="done", finished=time.time())
                self.next_start = max(started + a.interval, time.time() + a.min_display)

    def snapshot(self):
        with self.lock:
            per_q = []
            for q in self.queries:
                row = {"id": q.id, "name": q.name, "complexity": q.complexity,
                       "stats": {lbl: s.as_dict() for lbl, s in self.per_query[q.id].items()},
                       "speedup": {lbl: p.ratio for lbl, p in self.pairs_q[q.id].items()},
                       "supported": {t.label: q.supported(t.engine) for t in self.targets}}
                per_q.append(row)
            return {"current": _copy_entry(self.current), "history": [_copy_entry(h) for h in self.history],
                    "next_start": self.next_start, "rounds": self.rounds,
                    "overall": {lbl: s.as_dict() for lbl, s in self.overall.items()},
                    "speedup": {lbl: {"ratio": p.ratio, "rounds": p.n} for lbl, p in self.pairs.items()},
                    "base": self.base, "per_query": per_q}


def _copy_entry(e):
    if e is None:
        return None
    d = dict(e)
    d["results"] = {k: dict(v) for k, v in e["results"].items()}
    return d


# ================================================================== server
def make_handler(loop, targets, local, primary, args):
    def state():
        d = loop.snapshot()
        common = loop.state
        d.update({
            "version": __version__, "host": socket.gethostname(), "now": time.time(),
            "running": loop.running.is_set(), "timeout": args.timeout, "interval": args.interval,
            "order": args.order, "title": args.title,
            "database": targets[0].state.summary().split(";")[0],
            "common_window": "%s to %s (%s)" % (common.t_min.strftime("%H:%M:%S"),
                                                common.t_max.strftime("%Y-%m-%d %H:%M:%S"),
                                                human_duration(common.span_seconds)),
            "local": {"label": primary.label if primary else socket.gethostname(),
                      "host": socket.gethostname(), "cpus": os.cpu_count(),
                      "system_stats": local.system, "db_stats": local.db_enabled, "qps_stats": local.qps_enabled,
                      "series": local.snapshot()},
            "targets": [],
        })
        for t in targets:
            with t.lock:
                st = dict(t.status)
                st["lag_series"] = list(t.lag_series)
            d["targets"].append({
                "label": t.label, "engine": t.engine.name, "server_version": t.server_version,
                "target": t.describe(), "status_url": t.opts.status_url,
                "data_to": t.state.t_max.strftime("%Y-%m-%d %H:%M:%S"), **st})
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

        def do_GET(self):  # noqa: N802
            if self.path in ("/", "/index.html"):
                with open(PAGE, "rb") as fh:
                    self._send(200, fh.read(), "text/html; charset=utf-8")
            elif self.path == "/api/state":
                self._send(200, json.dumps(state(), default=str), "application/json")
            else:
                self._send(404, "not found", "text/plain")

        def do_POST(self):  # noqa: N802
            if self.path == "/api/pause":
                loop.running.clear()
            elif self.path == "/api/resume":
                loop.running.set()
            else:
                return self._send(404, "not found", "text/plain")
            self._send(200, json.dumps({"running": loop.running.is_set()}), "application/json")

    return Handler


def compare(args):
    settings, primary, target_specs = load_config(args.config)
    for k, v in settings.items():          # [dashboard] values unless given on the command line
        k = k.replace("-", "_")
        if not hasattr(args, k):
            raise SystemExit("%s: unknown [dashboard] key %r" % (args.config, k))
        if getattr(args, k) == args.defaults.get(k):
            setattr(args, k, v)
    from .cli import apply_weights
    queries = Q.load(args.query_dir, _ids(args.queries), _ids(args.exclude))
    apply_weights(queries, args.set_weight, "concurrent")
    targets = []
    for spec in target_specs:
        try:
            targets.append(Target(spec, args.refresh))
        except SystemExit:
            raise
        except Exception as e:  # noqa: BLE001
            raise SystemExit("cannot connect to target %s: %s" % (spec.label, e))
        print("target %-20s %s (%s): %s" % (targets[-1].label, targets[-1].describe(), targets[-1].server_version,
                                            targets[-1].state.summary()), flush=True)
    loop = CompareLoop(args, targets, queries)
    if not loop.queries:
        raise SystemExit("no query is supported by any target")
    for r in loop.runners.values():
        r.connect()
    pengine = get_engine(primary.engine, primary) if primary else None
    local = Sampler(pengine, args.system_stats == "auto")
    local.start()
    for t in targets:
        StatusPoller(t).start()
        if t.refresher:
            t.refresher.start()
    loop.start()
    server = ThreadingHTTPServer((args.listen, args.http_port), make_handler(loop, targets, local, primary, args))
    host = "localhost" if args.listen in ("0.0.0.0", "") else args.listen
    print("compare dashboard on http://%s:%d/  (%d targets, %d queries; Ctrl+C to stop)"
          % (host, args.http_port, len(targets), len(loop.queries)), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def _ids(s):
    return [x for x in (s or "").split(",") if x.strip()] or None
