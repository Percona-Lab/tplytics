"""What the DBTrail service says about itself, for the compare dashboard's /dbtrail page.

Run by the heartbeat monitor on the DBTrail host (its web interface and API
listen on localhost only) and served on /api/dbtrail:

* version and start time, from the API and the container;
* the settings that shape the copy: SQL memory, index retention, the copy
  schedule, capacity of the index;
* capture status (stream position, freshness, continuity) per server;
* DBTrail's own index MySQL (the index-mysql container): version, uptime,
  the settings that matter for it next to MySQL's defaults, and its size
  per schema and largest tables; its status counters are sampled by the
  /api/perf collector into ix_* series. It is reached on the container's
  address on the Docker network with the root password DBTrail keeps in its
  secret volume, and only read (SHOW and information_schema);
* the latest copy jobs (full reads and refreshes) with their duration and
  outcome, from DBTrail's own run history file, and the tables each one
  rewrote in full and why, from the container's log file (read directly,
  so no access to the Docker API is needed).

The API token comes from a file: either a plain token or DBTrail's .env file
with a CONSOLE_TOKEN= line. Every part is best effort and cached for a few
seconds; what cannot be read is reported, not fatal.
"""
import collections
import datetime as dt
import glob
import json
import os
import re
import threading
import time
import urllib.request

CACHE_SECONDS = 15
JOBS_KEPT = 60
LOG_FIRST_READ = 32 << 20        # at start, read at most the last 32 MB of the container log
RESOLVED_RE = re.compile(r'time=(\S+) .*msg="resolved pairs: done for the newest snapshot" .*snapshot=(\S+) '
                         r'tables_merged=(\d+) tables_unchanged=(\d+) tables_failed=(\d+) took_ms=(\d+)')
REWRITE_RE = re.compile(r'time=(\S+) .*msg="table rewritten in full[^"]*" schema=(\S+) table=(\S+) reason="([^"]*)"'
                        r'.*?rows_written=(\d+)')


def read_token(path):
    if not path:
        return None
    with open(path) as fh:
        text = fh.read()
    for line in text.splitlines():
        if line.startswith("CONSOLE_TOKEN="):
            return line.split("=", 1)[1].strip().strip('"\'')
    return text.strip() or None


def _ts(s):
    """ISO / 'YYYY-MM-DD HH:MM:SS' (UTC) -> epoch seconds, or None."""
    if not s:
        return None
    try:
        d = dt.datetime.fromisoformat(str(s).replace("Z", "+00:00").replace(" UTC", ""))
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.timestamp()


def _container(name_part="bintrail"):
    for cfg in glob.glob("/var/lib/docker/containers/*/config.v2.json"):
        try:
            with open(cfg) as fh:
                c = json.load(fh)
        except (OSError, ValueError):
            continue
        if name_part in c.get("Name", "") and c.get("State", {}).get("Running"):
            return c
    return None


def container_started(name_part="bintrail"):
    """(image, started epoch) of the Docker container whose name contains name_part, read from Docker's files."""
    c = _container(name_part)
    if not c:
        return None, None
    return c.get("Config", {}).get("Image"), _ts(c["State"].get("StartedAt", "")[:26] + "Z")


def container_address(name_part):
    c = _container(name_part)
    for net in ((c or {}).get("NetworkSettings") or {}).get("Networks", {}).values():
        if net.get("IPAddress"):
            return net["IPAddress"]
    return None


def router_metrics_urls():
    """The router's Prometheus endpoint (DBTrail 0.104+ serves the MySQL port and its SQL metrics there)."""
    ip = container_address("router")
    return ["http://%s:9091/metrics" % ip] if ip else []


def short_reason(reason):
    """DBTrail's reason for rewriting a table in full, in a few words."""
    m = re.match(r"the changes beside the table \((\d+) MB\) passed (\d+) MB", reason)
    if m:
        return "changes beside it reached %s MB (limit %s MB)" % (m.group(1), m.group(2))
    if "too close to the oldest events the index keeps" in reason:
        return "its changes reached the oldest events the index keeps"
    m = re.match(r"the changes beside the table \((\d+) bytes\) passed (\d+)% of the table", reason)
    if m:
        return "changes beside it passed %s%% of the table" % m.group(2)
    return reason[:160]


class RewriteLog:
    """'table rewritten in full' events from DBTrail's container log (Docker json-file), read incrementally."""

    def __init__(self, keep=400):
        self.events = collections.deque(maxlen=keep)
        self.resolved = {}          # snapshot epoch -> the merge DBTrail ran after it (0.103+)
        self.path, self.inode, self.pos = None, None, 0
        self.error = None

    def _parse(self, line):
        try:
            text = json.loads(line).get("log", "")
        except ValueError:
            return
        if "resolved pairs: done" in text:
            m = RESOLVED_RE.search(text)
            if m:
                snap = _ts(re.sub(r"T(\d\d)-(\d\d)-(\d\d)", r"T\1:\2:\3", m.group(2)))
                self.resolved[snap] = {"t": _ts(m.group(1)), "merged": int(m.group(3)), "unchanged": int(m.group(4)),
                                       "failed": int(m.group(5)), "seconds": int(m.group(6)) / 1000.0}
                if len(self.resolved) > 400:
                    for k in sorted(self.resolved)[:100]:
                        del self.resolved[k]
            return
        if "rewritten in full" not in text:
            return
        m = REWRITE_RE.search(text)
        if m:
            self.events.append({"t": _ts(m.group(1)), "table": m.group(3), "rows": int(m.group(5)),
                                "reason": short_reason(m.group(4))})

    def update(self):
        c = _container()
        path = c and c.get("LogPath")
        if not path:
            self.error = "DBTrail container log not found"
            return
        try:
            st = os.stat(path)
            if path != self.path or st.st_ino != self.inode or st.st_size < self.pos:
                # new container or rotated file: start again from (the end of) the new file
                self.path, self.inode, self.events = path, st.st_ino, collections.deque(maxlen=self.events.maxlen)
                self.resolved = {}
                self.pos = max(0, st.st_size - LOG_FIRST_READ)
            with open(path, "rb") as fh:
                fh.seek(self.pos)
                if self.pos:
                    fh.readline()                       # skip a partial first line
                for raw in fh:
                    if not raw.endswith(b"\n"):
                        break                           # a line still being written: next time
                    self._parse(raw.decode("utf-8", "replace"))
                    self.pos = fh.tell()
            self.error = None
        except OSError as e:
            self.error = str(e)[:200]

    def within(self, start, end):
        return [e for e in self.events if e["t"] and start - 1 <= e["t"] <= end + 1]


INDEX_SECRET = "/var/lib/docker/volumes/dbtrail_bintrail-index-secret/_data/index_password"
# settings that shape the index MySQL, with MySQL 8.4's defaults
INDEX_SETTINGS = [
    ("innodb_buffer_pool_size", "134217728"), ("innodb_redo_log_capacity", "104857600"),
    ("innodb_flush_log_at_trx_commit", "1"), ("innodb_flush_method", "O_DIRECT"), ("innodb_doublewrite", "ON"),
    ("innodb_io_capacity", "10000"), ("innodb_io_capacity_max", "20000"), ("log_bin", "ON"),
    ("sort_buffer_size", "262144"), ("max_allowed_packet", "67108864"), ("innodb_buffer_pool_instances", None),
    ("innodb_adaptive_hash_index", "OFF"), ("innodb_change_buffering", "none"), ("max_connections", "151"),
]


class IndexMySQL:
    """Read-only connection to DBTrail's index MySQL container; reconnects on demand."""

    def __init__(self, secret_file=INDEX_SECRET, name_part="index-mysql"):
        self.secret_file, self.name_part = secret_file, name_part
        self.conn = None
        self.lock = threading.Lock()

    def address(self):
        ip = container_address(self.name_part)
        if not ip:
            raise RuntimeError("index MySQL container not running or without a network address")
        return ip

    def execute(self, sql):
        import pymysql
        with self.lock:
            if self.conn is None:
                with open(self.secret_file) as fh:
                    pw = fh.read().strip()
                self.conn = pymysql.connect(host=self.address(), port=3306, user="root", password=pw, autocommit=True,
                                            connect_timeout=5, read_timeout=30)
            with self.conn.cursor() as c:
                c.execute(sql)
                rows = c.fetchall()
                cols = [d[0] for d in c.description] if c.description else []
            return cols, rows

    def reset(self):
        with self.lock:
            try:
                if self.conn is not None:
                    self.conn.close()
            except Exception:  # noqa: BLE001
                pass
            self.conn = None

    def settings(self):
        names = [n for n, _ in INDEX_SETTINGS]
        _, rows = self.execute("SHOW GLOBAL VARIABLES WHERE Variable_name IN (%s)" % ", ".join("'%s'" % n for n in names + ["version", "datadir"]))
        v = {str(r[0]): str(r[1]) for r in rows}
        _, rows = self.execute("SHOW GLOBAL STATUS WHERE Variable_name IN ('Uptime', 'Innodb_buffer_pool_pages_total', "
                               "'Innodb_buffer_pool_pages_data', 'Innodb_buffer_pool_pages_dirty', 'Innodb_page_size')")
        st = {str(r[0]): float(r[1] or 0) for r in rows}
        _, rows = self.execute("SELECT table_schema, SUM(data_length + index_length), SUM(table_rows), COUNT(*) "
                               "FROM information_schema.tables WHERE table_schema NOT IN "
                               "('mysql', 'sys', 'performance_schema', 'information_schema') GROUP BY table_schema ORDER BY 2 DESC")
        schemas = [{"schema": r[0], "bytes": int(r[1] or 0), "rows": int(r[2] or 0), "tables": int(r[3])} for r in rows]
        _, rows = self.execute("SELECT table_schema, table_name, data_length + index_length, table_rows "
                               "FROM information_schema.tables WHERE table_schema NOT IN "
                               "('mysql', 'sys', 'performance_schema', 'information_schema') ORDER BY 3 DESC LIMIT 6")
        tables = [{"schema": r[0], "table": r[1], "bytes": int(r[2] or 0), "rows": int(r[3] or 0)} for r in rows]
        return {"version": v.get("version"), "uptime": st.get("Uptime"),
                "settings": [{"name": n, "value": v.get(n), "default": d} for n, d in INDEX_SETTINGS if n in v],
                "buffer_pool": {k: st.get(k) for k in st if k.startswith("Innodb_")},
                "schemas": schemas, "tables": tables}


class DBTrailInfo:
    def __init__(self, console_url, token_file=None, history_file=None, index_mysql=None):
        self.index_mysql = index_mysql
        self.url = console_url.rstrip("/")
        self.token_file = token_file
        self.history_file = history_file
        self.lock = threading.Lock()
        self.cached, self.cached_at = None, 0.0
        self.rewrites = RewriteLog()

    def _get(self, path, timeout=10):
        req = urllib.request.Request(self.url + path)
        tok = read_token(self.token_file)
        if tok:
            req.add_header("Authorization", "Bearer " + tok)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8", "replace"))

    def _jobs(self, server_ids):
        """Latest copy jobs per server (newest first): DBTrail's copy-jobs API (0.104+), else its run history file."""
        runs_by = {}
        for sid in server_ids:
            try:
                runs_by[sid] = list(reversed(self._get("/api/servers/%s/copy-jobs" % sid).get("jobs") or []))
            except Exception:  # noqa: BLE001 - older DBTrail: no such route
                runs_by = {}
                break
        if not runs_by and self.history_file:
            with open(self.history_file) as fh:
                runs_by = (json.load(fh).get("servers") or {})
        out = {}
        for sid, runs in runs_by.items():
            if server_ids and sid not in server_ids:
                continue
            jobs = []
            for e in runs[-JOBS_KEPT:]:
                start, end = _ts(e.get("started_at")), _ts(e.get("finished_at"))
                err = e.get("error") or None
                skip = e.get("skip_reason") or (e.get("outcome") == "skipped" and "skipped") or None
                jobs.append({"kind": e.get("kind"), "started": start, "finished": end,
                             "seconds": round(end - start, 1) if start and end else None,
                             "outcome": "skipped" if skip else ("failed" if err else "ok"),
                             "detail": (err or skip or "")[:500],
                             "trigger": e.get("trigger"), "why": e.get("why"), "snapshot": _ts(e.get("snapshot_time")),
                             "tables": e.get("tables"), "rows": e.get("rows"), "events": e.get("events")})
            out[sid] = list(reversed(jobs))
        self.rewrites.update()
        for jobs in out.values():
            for j in jobs:
                if j["outcome"] != "skipped" and j["started"] and j["finished"]:
                    j["rewrites"] = self.rewrites.within(j["started"], j["finished"])
                    snap = j.get("snapshot")
                    if snap is None:
                        # copy-jobs carries no snapshot time: the snapshot is anchored within a few
                        # seconds of the job's start
                        snap = next((s for s in self.rewrites.resolved if j["started"] - 2 <= s <= j["finished"]), None)
                    if snap in self.rewrites.resolved:
                        j["resolved"] = self.rewrites.resolved[snap]
        return out

    def snapshot(self):
        with self.lock:
            if self.cached and time.time() - self.cached_at < CACHE_SECONDS:
                return self.cached
        doc = {"now": time.time(), "errors": {}}

        def part(key, fn):
            try:
                doc[key] = fn()
            except Exception as e:  # noqa: BLE001 - best effort, reported on the page
                doc["errors"][key] = str(e)[:300]
        part("capabilities", lambda: self._get("/api/capabilities"))
        doc["version"] = (doc.get("capabilities") or {}).get("version")
        image, started = container_started()
        doc["image"], doc["container_started"] = image, started
        part("sql", lambda: self._get("/api/sql-settings"))
        part("rotation", lambda: self._get("/api/rotation"))
        part("capacity", lambda: self._get("/api/capacity"))
        part("backup", lambda: self._get("/api/backup-settings"))
        part("status", lambda: self._get("/api/status"))
        try:
            r = self._get("/api/router")
            if r.get("configured") and r.get("status"):
                doc["router"] = r["status"]
                fb = self._get("/api/flashback")             # the MySQL port's saved settings
                doc["router"]["routing_on"] = bool(fb.get("routing_on"))
                doc["router"]["port_enabled"] = bool(fb.get("enabled"))
        except Exception:  # noqa: BLE001 - DBTrail before 0.104 has no router
            pass
        servers = []
        try:
            listed = self._get("/api/servers")
            listed = listed.get("servers", listed) if isinstance(listed, dict) else listed
        except Exception as e:  # noqa: BLE001
            doc["errors"]["servers"] = str(e)[:300]
            listed = []
        sched = {s.get("id"): s for s in (doc.get("backup") or {}).get("servers", [])}
        for s in listed or []:
            sid = s.get("id")
            if not sid:
                continue
            row = {"id": sid, "name": s.get("name"), "source": "%s:%s" % (s.get("source_host"), s.get("source_port")),
                   "schemas": s.get("schemas"), "flavor": s.get("flavor"), "monitor_state": s.get("monitor_state"),
                   "connected": s.get("connected")}
            b = sched.get(sid, {})
            row["schedule"] = {"every": b.get("schedule_every"), "at": b.get("schedule_at"),
                               "full_every": b.get("schedule_full_every"), "keep_newest": b.get("keep_newest")}
            for key, path in (("monitor", "/api/servers/%s/monitor"), ("baseline", "/api/servers/%s/baseline")):
                try:
                    v = self._get(path % sid)
                    row[key] = v.get(key, v)
                except Exception as e:  # noqa: BLE001
                    row[key] = {"error": str(e)[:200]}
            servers.append(row)
        doc["servers"] = servers
        part("jobs", lambda: self._jobs({s["id"] for s in servers}))
        if self.index_mysql is not None:
            try:
                doc["index_mysql"] = self.index_mysql.settings()
            except Exception as e:  # noqa: BLE001
                doc["errors"]["index_mysql"] = str(e)[:300]
                self.index_mysql.reset()
        if self.rewrites.error:
            doc["errors"]["log"] = self.rewrites.error
        # keep what the page shows: the version, not every capability and permission
        doc.pop("capabilities", None)
        doc.pop("backup", None)
        # the status document lists every index partition: keep the summary only
        st = doc.get("status")
        if isinstance(st, dict):
            cov = dict(st.get("coverage") or {})
            for k in ("earliest_event", "latest_event"):
                cov[k] = _ts(cov.get(k))
            doc["status"] = {"stream": st.get("stream"), "partitions": len(st.get("partitions") or []), "coverage": cov}
        with self.lock:
            self.cached, self.cached_at = doc, time.time()
        return doc
