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

    def __init__(self, sources):
        self.sources = sources          # DatabaseState objects, or Targets whose state may be None yet

    @property
    def states(self):
        out = [getattr(s, "state", s) for s in self.sources]
        out = [s for s in out if s is not None]
        if not out:
            raise RuntimeError("no target has answered yet")
        return out

    def __getattr__(self, name):
        if name in ("t_min", "t_max", "span_seconds", "sources", "states"):
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


# ============================================================ long-term log
class PassLog:
    """Geometric mean per target of every full pass over the queries, kept on disk.

    A pass is one run of every query (Q01..Qnn in sequential order). Its point
    is the geometric mean of the pass's execution times per target; a timeout
    counts as the timeout (a lower bound), errors and unsupported queries are
    left out and counted. Points are appended to a JSON-lines file, so the
    chart survives restarts; points older than --keep-hours are dropped.
    """

    def __init__(self, path, keep_hours, timeout, n_queries):
        self.path, self.keep, self.timeout, self.n = path, keep_hours * 3600, timeout, n_queries
        self.points = []
        self.cur = None
        self.lock = threading.Lock()
        self.version = 0
        if path:
            self._load()

    def _load(self):
        cutoff = time.time() - self.keep
        try:
            with open(self.path) as fh:
                for line in fh:
                    try:
                        p = json.loads(line)
                    except ValueError:
                        continue                  # a line cut by a crash
                    if p.get("t", 0) >= cutoff:
                        self.points.append(p)
        except FileNotFoundError:
            pass
        self._rewrite()                           # drop what aged out

    def _rewrite(self):
        if not self.path:
            return
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w") as fh:
            for p in self.points:
                fh.write(json.dumps(p, separators=(",", ":")) + "\n")
        os.replace(tmp, self.path)

    def add_round(self, results, now):
        """Count one finished round; close the pass after n rounds."""
        if self.cur is None:
            self.cur = {"start": now, "rounds": 0, "t": {}}
        self.cur["rounds"] += 1
        for label, r in results.items():
            acc = self.cur["t"].setdefault(label, {"logsum": 0.0, "n": 0, "errors": 0, "timeouts": 0})
            st = r.get("status")
            if st == OK:
                acc["logsum"] += math.log(max(r["elapsed"], 1e-4))
                acc["n"] += 1
            elif st == TIMEOUT:
                acc["logsum"] += math.log(self.timeout)
                acc["n"] += 1
                acc["timeouts"] += 1
            elif st != "unsupported":
                acc["errors"] += 1
        if self.cur["rounds"] >= self.n:
            self._close(now)

    def _close(self, now):
        cur, self.cur = self.cur, None
        point = {"t": round(now, 1), "start": round(cur["start"], 1), "queries": cur["rounds"], "targets": {}}
        for label, acc in cur["t"].items():
            point["targets"][label] = {"geomean": round(math.exp(acc["logsum"] / acc["n"]), 4) if acc["n"] else None,
                                       "n": acc["n"], "errors": acc["errors"], "timeouts": acc["timeouts"]}
        with self.lock:
            self.points.append(point)
            cutoff = now - self.keep
            aged = self.points and self.points[0]["t"] < cutoff
            self.points = [p for p in self.points if p["t"] >= cutoff]
            self.version += 1
        if self.path:
            if aged:
                self._rewrite()
            else:
                with open(self.path, "a") as fh:
                    fh.write(json.dumps(point, separators=(",", ":")) + "\n")

    def snapshot(self):
        with self.lock:
            return {"version": self.version, "keep_hours": self.keep / 3600, "points": list(self.points),
                    "in_progress": self.cur and {"start": self.cur["start"], "rounds": self.cur["rounds"]},
                    "queries": self.n}


class ExecLog:
    """Every execution (time, query, target, status, seconds) of the last keep_hours.

    Kept per query in memory and appended to a JSON-lines file when a path is
    given, so the per-query time charts survive restarts. The file is
    rewritten without the aged-out lines at start and whenever the oldest line
    is an hour past the window, which keeps it about keep_hours long.
    """

    def __init__(self, path, keep_hours):
        self.path, self.keep = path, keep_hours * 3600
        self.by_query = collections.defaultdict(collections.deque)
        self.lock = threading.Lock()
        self.oldest = None
        if path:
            self._load()

    def _load(self):
        cutoff = time.time() - self.keep
        try:
            with open(self.path) as fh:
                for line in fh:
                    try:
                        p = json.loads(line)
                    except ValueError:
                        continue
                    if p.get("t", 0) >= cutoff and p.get("q"):
                        self.by_query[p["q"]].append(p)
        except FileNotFoundError:
            pass
        self._rewrite()

    def _all(self):
        return sorted((p for d in self.by_query.values() for p in d), key=lambda p: p["t"])

    def _rewrite(self):
        pts = self._all()
        self.oldest = pts[0]["t"] if pts else None
        if not self.path:
            return
        os.makedirs(os.path.dirname(os.path.abspath(self.path)), exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w") as fh:
            for p in pts:
                fh.write(json.dumps(p, separators=(",", ":")) + "\n")
        os.replace(tmp, self.path)

    def add(self, t, qid, label, status, elapsed):
        p = {"t": round(t, 1), "q": qid, "target": label, "status": status,
             "s": None if elapsed is None else round(elapsed, 4)}
        with self.lock:
            self.by_query[qid].append(p)
            if self.oldest is None:
                self.oldest = p["t"]
            cutoff = t - self.keep
            if self.oldest < cutoff - 3600:
                for d in self.by_query.values():
                    while d and d[0]["t"] < cutoff:
                        d.popleft()
                self._rewrite()
            elif self.path:
                with open(self.path, "a") as fh:
                    fh.write(json.dumps(p, separators=(",", ":")) + "\n")

    def for_query(self, qid, now=None):
        cutoff = (now or time.time()) - self.keep
        with self.lock:
            return [p for p in self.by_query.get(qid, ()) if p["t"] >= cutoff]


def _dialect_used(q, engine):
    for d in engine.dialects:
        if d in q.sql:
            return d
    return None


# =================================================================== targets
class Target:
    """One system under test. Its size and time range are read when it first
    answers; a target that does not answer at start (down, or refusing the
    discovery queries) is retried in the background instead of stopping the
    dashboard, and its rounds are reported as unavailable meanwhile."""
    RETRY = 30.0

    def __init__(self, opts, refresh):
        self.opts = opts
        self.label = opts.label
        self.engine = get_engine(opts.engine, opts)
        self.refresh = refresh
        self.state = None
        self.error = "not connected yet"
        self.server_version = "?"
        self.refresher = None
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

    def discover(self):
        """Read the target's size and time range; True once it has answered."""
        session = None
        try:
            session = self.engine.connect()
            self.server_version = session.server_version
            self.state = DatabaseState(session)
            self.error = None
        except Exception as e:  # noqa: BLE001 - reported, retried
            self.error = str(e).replace("\n", " ")[:300]
            return False
        finally:
            if session is not None:
                session.close()
        if self.refresh > 0:
            self.refresher = StateRefresher(self.engine, self.state, self.refresh)
            self.refresher.start()
        return True

    def start_discovery(self):
        def retry():
            while not self.discover():
                time.sleep(self.RETRY)
            print("target %s answered: %s" % (self.label, self.state.summary()), flush=True)
        threading.Thread(target=retry, daemon=True, name="discover-%s" % self.label).start()


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
        self.state = CommonState(targets)
        self.runners = {t.label: Runner(t.engine, args.timeout, t.opts.init_sql or []) for t in targets}
        self.lock = threading.Lock()
        self.running = threading.Event()
        if not args.start_paused:
            self.running.set()
        self.current = None
        self.history = collections.deque(maxlen=max(args.history, 25))   # the "see more" list shows 25
        self.next_start = None
        self.rounds = 0
        self.rng = random.Random(args.seed)
        self.overall = {t.label: GeoStats() for t in targets}
        self.per_query = {q.id: {t.label: GeoStats() for t in targets} for q in self.queries}
        base = targets[0].label
        self.base = base
        self.pairs = {t.label: PairStats() for t in targets[1:]}
        self.pairs_q = {q.id: {t.label: PairStats() for t in targets[1:]} for q in self.queries}
        self.passes = PassLog(args.history_file, args.keep_hours, args.timeout, len(self.queries))
        exec_path = None
        if args.history_file:
            root, ext = os.path.splitext(args.history_file)
            exec_path = root + "-executions" + (ext or ".jsonl")
        self.execlog = ExecLog(exec_path, args.keep_hours)

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
        try:
            self.execlog.add(time.time(), q.id, t.label, r.status, r.elapsed)
        except OSError as e:
            print("cannot write %s: %s" % (self.execlog.path, e), flush=True)

    def run(self):
        a = self.args
        if not self.queries:
            return
        while True:
            if self.next_start:
                self._wait_until(self.next_start)
            self.running.wait()
            if all(t.state is None for t in self.targets):
                time.sleep(5)                 # nothing to draw parameters from yet
                continue
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
                elif t.state is None:
                    res.update(state="done", status="unavailable", elapsed=None, rows=0, sql="",
                               error="target unavailable (retried every %ds): %s" % (Target.RETRY, t.error))
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
            try:
                self.passes.add_round(results, time.time())
            except OSError as e:             # the chart is a bonus: never stop the board for it
                print("cannot write %s: %s" % (self.passes.path, e), flush=True)

    def query_detail(self, qid):
        """What the per-query pop-up shows: the query, its SQL per target and 24 h of executions."""
        q = next((x for x in self.queries if x.id == qid), None)
        if q is None:
            return None
        sql = {}
        for t in self.targets:
            text = q.sql_for(t.engine)
            sql[t.label] = {"engine": t.engine.name, "dialect": _dialect_used(q, t.engine) if text else None,
                            "text": text}
        texts = [v["text"] for v in sql.values() if v["text"] is not None]
        with self.lock:
            stats = {lbl: s.as_dict() for lbl, s in self.per_query[q.id].items()}
            speedup = {lbl: p.ratio for lbl, p in self.pairs_q[q.id].items()}
        return {"id": q.id, "name": q.name, "description": q.description, "complexity": q.complexity,
                "tables": q.tables, "weight": q.weight, "params": q.params, "sql": sql,
                "same_sql": len(set(texts)) <= 1, "stats": stats, "speedup": speedup,
                "executions": self.execlog.for_query(q.id), "keep_hours": self.execlog.keep / 3600,
                "timeout": self.args.timeout, "targets": [t.label for t in self.targets], "now": time.time()}

    def find_round(self, seq):
        """The current or a recent round by number, or None once it scrolled out."""
        with self.lock:
            for e in ([self.current] if self.current else []) + list(self.history):
                if e["seq"] == seq:
                    return _copy_entry(e)
        return None

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
                    "base": self.base, "per_query": per_q,
                    "passes_version": self.passes.version,
                    "pass_in_progress": self.passes.cur and self.passes.cur["rounds"],
                    "pass_queries": self.passes.n}


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
            "database": next((t.state.summary().split(";")[0] for t in targets if t.state), "waiting for the targets"),
            "common_window": "%s to %s (%s)" % (common.t_min.strftime("%H:%M:%S"),
                                                common.t_max.strftime("%Y-%m-%d %H:%M:%S"),
                                                human_duration(common.span_seconds)),
            "local": {"label": primary.label if primary else socket.gethostname(),
                      "host": socket.gethostname(), "cpus": os.cpu_count(),
                      "system_stats": local.system, "db_stats": local.db_enabled, "qps_stats": local.qps_enabled, "rows_stats": local.rows_enabled,
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
                "data_to": t.state.t_max.strftime("%Y-%m-%d %H:%M:%S") if t.state else "unavailable",
                "unavailable": None if t.state else t.error, **st})
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
            elif self.path.startswith("/api/query?"):
                from urllib.parse import parse_qs, urlsplit
                qid = (parse_qs(urlsplit(self.path).query).get("id") or [""])[0]
                d = loop.query_detail(qid)
                if d is None:
                    return self._send(404, json.dumps({"error": "unknown query %r" % qid}), "application/json")
                self._send(200, json.dumps(d, default=str), "application/json")
            elif self.path == "/api/passes":
                self._send(200, json.dumps(loop.passes.snapshot()), "application/json")
            elif self.path.startswith("/sql?"):
                self._sql()
            else:
                self._send(404, "not found", "text/plain")

        def _sql(self):
            """The statement one target ran in a round, as its own page."""
            from urllib.parse import parse_qs, urlsplit
            qs = parse_qs(urlsplit(self.path).query)
            try:
                seq, label = int(qs["round"][0]), qs["target"][0]
            except (KeyError, ValueError):
                return self._send(400, "use /sql?round=N&target=LABEL", "text/plain")
            e = loop.find_round(seq)
            r = e and e["results"].get(label)
            if not r:
                return self._send(404, "round %d is no longer kept (the page keeps the last %d)"
                                  % (seq, args.history + 1), "text/plain")
            self._send(200, render_sql_page(e, label, r, args.title), "text/html; charset=utf-8")

        def do_POST(self):  # noqa: N802
            if self.path == "/api/pause":
                loop.running.clear()
            elif self.path == "/api/resume":
                loop.running.set()
            else:
                return self._send(404, "not found", "text/plain")
            self._send(200, json.dumps({"running": loop.running.is_set()}), "application/json")

    return Handler


def render_sql_page(e, label, r, title):
    from html import escape as h
    status = r.get("status") or "running"
    took = "" if r.get("elapsed") is None else " in %.3f s" % r["elapsed"]
    return """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>%(qid)s on %(label)s · round %(seq)d</title>
<style>
:root { color-scheme: light dark; --bg: #f4f5f7; --fg: #0b0b0b; --mut: #52514e; --code: #fcfcfb; --line: #e1e0dc; }
@media (prefers-color-scheme: dark) { :root { --bg: #121211; --fg: #fff; --mut: #c3c2b7; --code: #1a1a19; --line: #2c2c2a; } }
body { margin: 0; background: var(--bg); color: var(--fg); font: 15px/1.5 system-ui, sans-serif; }
.wrap { max-width: 1100px; margin: 0 auto; padding: 20px 16px 40px; }
h1 { font-size: 20px; margin: 0 0 4px; } p { color: var(--mut); margin: 4px 0; }
pre { background: var(--code); border: 1px solid var(--line); border-radius: 8px; padding: 14px 16px;
      overflow-x: auto; font: 13.5px/1.55 ui-monospace, Menlo, Consolas, "DejaVu Sans Mono", monospace; white-space: pre; }
pre.err { color: #d03b3b; white-space: pre-wrap; }
</style></head><body><div class="wrap">
<h1>%(qid)s %(name)s · %(label)s</h1>
<p>%(title)s · round %(seq)d · %(status)s%(took)s</p>
<p>Parameters: %(params)s</p>
<pre>%(sql)s;</pre>%(err)s
</div></body></html>""" % {
        "qid": h(e["id"]), "name": h(e["name"]), "label": h(label), "seq": e["seq"], "title": h(title),
        "status": h(status), "took": took, "params": h(e.get("params") or "none"),
        "sql": h((r.get("sql") or "-- not run").strip()),
        "err": "<pre class=\"err\">%s</pre>" % h(r["error"]) if r.get("error") else ""}


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
        t = Target(spec, args.refresh)
        targets.append(t)
        if t.discover():
            print("target %-20s %s (%s): %s" % (t.label, t.describe(), t.server_version, t.state.summary()), flush=True)
        else:
            print("target %-20s %s does not answer yet, retrying every %ds: %s"
                  % (t.label, t.describe(), Target.RETRY, t.error), flush=True)
            t.start_discovery()
    loop = CompareLoop(args, targets, queries)
    if not loop.queries:
        raise SystemExit("no query is supported by any target")
    for r in loop.runners.values():
        try:
            r.connect()
        except Exception:  # noqa: BLE001 - the runner reconnects on its next query
            r.session = None
    pengine = get_engine(primary.engine, primary) if primary else None
    local = Sampler(pengine, args.system_stats == "auto")
    local.start()
    for t in targets:
        StatusPoller(t).start()
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
