"""What the DBTrail service says about itself, for the compare dashboard's /dbtrail page.

Run by the heartbeat monitor on the DBTrail host (its web interface and API
listen on localhost only) and served on /api/dbtrail:

* version and start time, from the API and the container;
* the settings that shape the copy: SQL memory, index retention, the copy
  schedule, capacity of the index;
* capture status (stream position, freshness, continuity) per server;
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
        self.path, self.inode, self.pos = None, None, 0
        self.error = None

    def _parse(self, line):
        try:
            text = json.loads(line).get("log", "")
        except ValueError:
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


class DBTrailInfo:
    def __init__(self, console_url, token_file=None, history_file=None):
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
        """Latest copy jobs per server from DBTrail's run history (newest first)."""
        if not self.history_file:
            return {}
        with open(self.history_file) as fh:
            doc = json.load(fh)
        out = {}
        for sid, runs in (doc.get("servers") or {}).items():
            if server_ids and sid not in server_ids:
                continue
            jobs = []
            for e in runs[-JOBS_KEPT:]:
                start, end = _ts(e.get("started_at")), _ts(e.get("finished_at"))
                err = e.get("error") or None
                skip = e.get("skip_reason") or None
                jobs.append({"kind": e.get("kind"), "started": start, "finished": end,
                             "seconds": round(end - start, 1) if start and end else None,
                             "outcome": "skipped" if skip else ("failed" if err else "ok"),
                             "detail": (err or skip or "")[:500],
                             "trigger": e.get("trigger"), "why": e.get("why"),
                             "tables": e.get("tables"), "rows": e.get("rows"), "events": e.get("events")})
            out[sid] = list(reversed(jobs))
        self.rewrites.update()
        for jobs in out.values():
            for j in jobs:
                if j["outcome"] != "skipped" and j["started"] and j["finished"]:
                    j["rewrites"] = self.rewrites.within(j["started"], j["finished"])
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
        if self.rewrites.error:
            doc["errors"]["log"] = self.rewrites.error
        # keep what the page shows: the version, not every capability and permission
        doc.pop("capabilities", None)
        doc.pop("backup", None)
        # the status document lists every index partition: keep the summary only
        st = doc.get("status")
        if isinstance(st, dict):
            doc["status"] = {"stream": st.get("stream"), "partitions": len(st.get("partitions") or [])}
        with self.lock:
            self.cached, self.cached_at = doc, time.time()
        return doc
