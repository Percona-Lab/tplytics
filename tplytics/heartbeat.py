"""Heartbeat: end-to-end replication lag, in the style of pt-heartbeat.

``update`` runs against the primary and writes one row of a heartbeat table
every --interval seconds, on round multiples of the interval: a sequence
number, the writer's clock in UTC microseconds and the write interval. ``monitor`` reads that row from a replica (a MySQL replica, a
PostgreSQL standby, the Parquet copy behind a DBTrail port, ...) and reports
how old it is: the time from the write on the primary to the moment the row
can be read on the replica, whatever path it took.

The timestamp is written by the client, not taken from a database function,
so the same table and statements work on every engine. The monitor compares
it with its own clock: run both on hosts kept in sync by NTP (the same
requirement as pt-heartbeat).

Lag, as in pt-heartbeat, is how long the replica has been missing the beat
after the one it shows: ``max(0, read time - (beat time + writer interval))``.
A replica that has every beat already written reports 0; one that is behind
reports its delay with a resolution of one interval. The monitor reads
--skew seconds after the writer's ticks (half an interval by default) so it
does not race the write it is waiting for. ``age`` is the plain age of the
row (read time - beat time), which includes up to one interval of sampling.
"""
import collections
import datetime as dt
import json
import os
import socket
import threading
import time

DEFAULT_TABLE = "tplytics_heartbeat"


def now_us():
    return int(time.time() * 1_000_000)


def log(msg):
    print("%s %s" % (dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), msg), flush=True)


def _check_table_name(name):
    import re
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?", name):
        raise SystemExit("--table must be an identifier (optionally schema.table), got %r" % name)
    return name


def _sleep_until(t):
    d = t - time.time()
    if d > 0:
        time.sleep(d)


def _next_tick(interval, after=None):
    """Next multiple of interval on the wall clock, so writes land on round times."""
    t = time.time() if after is None else after
    return (int(t / interval) + 1) * interval


# ================================================================ writer
class Writer:
    def __init__(self, engine, table, row_id, interval):
        self.engine, self.table, self.row_id, self.interval = engine, table, row_id, interval
        self.session = None
        self.seq = 0
        self.writer = socket.gethostname()[:255]

    def connect(self):
        if not self.engine.heartbeat_writable:
            raise SystemExit("%s is read-only: run heartbeat update against the primary" % self.engine.name)
        self.session = self.engine.connect()
        self.session.run_init_sql(getattr(self.engine.opts, "init_sql", None) or [])
        self.session.execute(self.engine.heartbeat_ddl(self.table), fetch=False)
        _, rows = self.session.execute("SELECT seq FROM %s WHERE id = %d" % (self.table, self.row_id))
        if rows:
            self.seq = int(rows[0][0])          # continue the sequence across restarts
        else:
            self._write("INSERT INTO %s (id, seq, ts_us, interval_us, ts, writer) "
                        "VALUES (%d, {seq}, {ts_us}, {iv}, {ts}, {w})" % (self.table, self.row_id))

    def _write(self, template):
        t = now_us()
        self.seq += 1
        lit = self.engine.literal
        sql = template.format(seq=self.seq, ts_us=t, iv=int(self.interval * 1_000_000), w=lit(self.writer),
                              ts=lit(dt.datetime.fromtimestamp(t / 1e6, dt.timezone.utc).replace(tzinfo=None)))
        self.session.execute(sql, fetch=False)
        return t

    def beat(self):
        return self._write("UPDATE %s SET seq = {seq}, ts_us = {ts_us}, interval_us = {iv}, ts = {ts}, "
                           "writer = {w} WHERE id = %d" % (self.table, self.row_id))

    def close(self):
        if self.session is not None:
            self.session.close()
            self.session = None


def run_update(args, engine):
    w = Writer(engine, _check_table_name(args.table), args.id, args.interval)
    log("heartbeat update: %s table %s id %d every %gs" % (engine.describe(), w.table, w.row_id, w.interval))
    beats, errors, last_report = 0, 0, time.time()
    tick = _next_tick(args.interval)
    while True:
        try:
            if w.session is None:
                w.connect()
                log("connected (%s), continuing at seq %d" % (w.session.server_version, w.seq))
            _sleep_until(tick)
            w.beat()
            beats += 1
        except SystemExit:
            raise
        except Exception as e:  # noqa: BLE001 - keep beating through outages
            errors += 1
            log("error: %s" % str(e).replace("\n", " ")[:300])
            w.close()
            time.sleep(min(args.interval, 5))
        tick = _next_tick(args.interval, max(tick, time.time()))
        if args.count and beats >= args.count:
            break
        if time.time() - last_report >= args.report_interval:
            log("seq %d, %d beats, %d errors in the last %ds" % (w.seq, beats, errors, args.report_interval))
            beats, errors, last_report = 0, 0, time.time()
    w.close()
    return 0


# =============================================================== monitor
class LagStats:
    """Rolling lag samples for the last 15 minutes."""
    WINDOWS = (("1m", 60), ("5m", 300), ("15m", 900))

    def __init__(self):
        self.samples = collections.deque()   # (epoch, lag seconds or None)

    def add(self, t, lag):
        self.samples.append((t, lag))
        while self.samples and self.samples[0][0] < t - self.WINDOWS[-1][1]:
            self.samples.popleft()

    def summary(self, t=None):
        t = time.time() if t is None else t
        out = {}
        for name, secs in self.WINDOWS:
            vals = sorted(v for ts, v in self.samples if ts >= t - secs and v is not None)
            if not vals:
                out[name] = None
                continue
            out[name] = {"samples": len(vals), "min": round(vals[0], 3), "avg": round(sum(vals) / len(vals), 3),
                         "p95": round(vals[min(len(vals) - 1, int(0.95 * len(vals)))], 3),
                         "max": round(vals[-1], 3)}
        return out


def read_heartbeat(session, table, row_id):
    """Return (seq, ts_us, interval_us, writer) or None when the row is not there (yet)."""
    _, rows = session.execute("SELECT seq, ts_us, interval_us, writer FROM %s WHERE id = %d" % (table, row_id))
    if not rows:
        return None
    seq, ts_us, interval_us, writer = rows[0]
    return int(seq), int(ts_us), int(interval_us), writer


def lag_of(read_at, ts_us, interval_us):
    """Seconds the replica has been missing the beat after ts (0 when it has every beat due)."""
    return max(0.0, read_at - (ts_us + interval_us) / 1e6)


class Monitor:
    def __init__(self, args, engine):
        self.args, self.engine = args, engine
        self.table = _check_table_name(args.table)
        self.label = args.label or engine.name
        self.session = None
        self.stats = LagStats()
        self.status = {"label": self.label, "engine": engine.name, "target": engine.describe(),
                       "table": self.table, "id": args.id, "interval": args.interval, "state": "starting"}
        self.lock = threading.Lock()

    def sample(self):
        if self.session is None:
            self.session = self.engine.connect()
            self.session.run_init_sql(getattr(self.engine.opts, "init_sql", None) or [])
            self.status["server_version"] = self.session.server_version
        t0 = time.time()
        hb = read_heartbeat(self.session, self.table, self.args.id)
        t = time.time()
        read_s = t - t0
        if hb is None:
            return t, None, None, read_s
        seq, ts_us, interval_us, writer = hb
        # The row was readable when the read started; the read's own time is
        # not replication lag (it can be large on a busy DBTrail port).
        return t, lag_of(t0, ts_us, interval_us), (seq, ts_us, writer, t0 - ts_us / 1e6), read_s

    def step(self):
        try:
            t, lag, hb, read_s = self.sample()
        except Exception as e:  # noqa: BLE001
            err = str(e).replace("\n", " ")[:300]
            if self.session is not None:
                self.session.close()
                self.session = None
            with self.lock:
                self.stats.add(time.time(), None)
                self.status.update(state="error", error=err, checked_at=_iso(time.time()))
            return None, err
        with self.lock:
            self.stats.add(t, lag)
            if hb is None:
                self.status.update(state="no heartbeat", error="row id %d not found in %s"
                                   % (self.args.id, self.table), checked_at=_iso(t))
                return None, self.status["error"]
            seq, ts_us, writer, age = hb
            self.status.update(state="ok", error=None, lag=round(lag, 3), age=round(age, 3), seq=seq,
                               writer=writer,
                               heartbeat_at=_iso(ts_us / 1e6), checked_at=_iso(t), read_seconds=round(read_s, 3))
        return lag, None

    def snapshot(self):
        with self.lock:
            doc = dict(self.status)
            doc["stats"] = self.stats.summary()
            doc["max_lag"] = self.args.max_lag
            return doc

    def write_status(self):
        if not self.args.status_file:
            return
        tmp = self.args.status_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.snapshot(), f, indent=1)
        os.replace(tmp, self.args.status_file)


def _iso(epoch):
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _fmt(x):
    return "-" if x is None else "%.3fs" % x


def serve_http(monitor, listen, port, sampler=None, perf=None, dbtrail=None):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            path = self.path.split("?")[0]
            if path in ("/", "/api/heartbeat"):
                doc = monitor.snapshot()
            elif path == "/api/perf" and perf is not None:
                from urllib.parse import parse_qs, urlsplit
                doc = perf.snapshot((parse_qs(urlsplit(self.path).query).get("range") or ["1h"])[0])
            elif path == "/api/dbtrail" and dbtrail is not None:
                doc = dbtrail.snapshot()
            elif path == "/api/system" and sampler is not None:
                doc = {"host": socket.gethostname(), "cpus": os.cpu_count(), "system_stats": sampler.system,
                       "series": sampler.snapshot()}
            else:
                self.send_error(404)
                return
            body = json.dumps(doc, indent=1).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer((listen, port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log("serving lag as JSON on http://%s:%d/api/heartbeat" % (listen, port))


def run_monitor(args, engine):
    m = Monitor(args, engine)
    log("heartbeat monitor [%s]: %s table %s id %d every %gs"
        % (m.label, engine.describe(), m.table, args.id, args.interval))
    if args.http_port:
        sampler = None
        if args.system_stats:
            from .dashboard import Sampler
            sampler = Sampler(None, True)
            sampler.start()
        index_mysql = None
        if args.dbtrail_console and args.dbtrail_index_secret != "off":
            from .dbtrail_info import IndexMySQL
            index_mysql = IndexMySQL(args.dbtrail_index_secret)
        perf = None
        if args.perf:
            from .perf import PerfSampler
            # DBTrail's index MySQL gets its own connection here: the sampler and the settings
            # snapshot run in different threads
            ix = None
            if index_mysql is not None:
                from .dbtrail_info import IndexMySQL
                ix = IndexMySQL(args.dbtrail_index_secret)
            extra = None
            if args.dbtrail_console and args.prometheus_url:
                from .dbtrail_info import router_metrics_urls
                extra = router_metrics_urls
            perf = PerfSampler(None if args.prometheus_url else engine, prometheus_url=args.prometheus_url, index_mysql=ix,
                               extra_prom=extra)
            perf.start()
            log("serving detailed performance on http://%s:%d/api/perf" % (args.listen, args.http_port))
        info = None
        if args.dbtrail_console:
            from .dbtrail_info import DBTrailInfo
            info = DBTrailInfo(args.dbtrail_console, args.dbtrail_token_file, args.dbtrail_history, index_mysql)
            log("serving DBTrail version, settings and copy jobs on http://%s:%d/api/dbtrail" % (args.listen, args.http_port))
        serve_http(m, args.listen, args.http_port, sampler, perf, info)
        if sampler is not None:
            log("serving this host's CPU and disk load on http://%s:%d/api/system" % (args.listen, args.http_port))
    skew = args.interval / 2 if args.skew is None else args.skew
    last_report, n = time.time(), 0
    tick = _next_tick(args.interval)
    while True:
        _sleep_until(tick + skew)
        lag, err = m.step()
        n += 1
        if args.verbose:
            log("[%s] lag %s age %s%s" % (m.label, _fmt(lag), _fmt(m.status.get("age") if lag is not None else None),
                                          ("  (%s)" % err) if err else ""))
        if time.time() - last_report >= args.report_interval:
            s = m.snapshot()
            w = s["stats"].get("1m") or {}
            log("[%s] %s lag %s  1m avg %s max %s  seq %s%s"
                % (m.label, s["state"], _fmt(s.get("lag") if s["state"] == "ok" else None),
                   _fmt(w.get("avg")), _fmt(w.get("max")), s.get("seq", "-"),
                   ("  error: " + s["error"]) if s.get("error") else ""))
            last_report = time.time()
        try:
            m.write_status()
        except OSError as e:
            log("cannot write %s: %s" % (args.status_file, e))
        if args.count and n >= args.count:
            return 0
        tick = _next_tick(args.interval, max(tick, time.time()))


def run_check(args, engine):
    """One read; exit 0 when lag <= --max-lag, 1 when above, 2 when it cannot be measured."""
    args.label = args.label or engine.name
    m = Monitor(args, engine)
    lag, err = m.step()
    if lag is None:
        print("UNKNOWN [%s]: %s" % (m.label, err))
        return 2
    state = "OK" if args.max_lag is None or lag <= args.max_lag else "CRITICAL"
    print("%s [%s]: lag %.3fs, age %.3fs, seq %d, heartbeat at %s"
          % (state, m.label, lag, m.status["age"], m.status["seq"], m.status["heartbeat_at"]))
    return 0 if state == "OK" else 1


def heartbeat(args):
    from .engines import get_engine
    if args.interval <= 0:
        raise SystemExit("--interval must be positive")
    engine = get_engine(args.engine, args)
    try:
        if args.action == "update":
            return run_update(args, engine)
        if args.action == "monitor":
            return run_monitor(args, engine)
        return run_check(args, engine)
    except ImportError as e:
        raise SystemExit("missing driver for %s: %s (pip install -r requirements.txt)" % (engine.name, e))
    except KeyboardInterrupt:
        return 130
