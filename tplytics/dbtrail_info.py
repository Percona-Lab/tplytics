"""What the DBTrail service says about itself, for the compare dashboard's /dbtrail page.

Run by the heartbeat monitor on the DBTrail host (its web interface and API
listen on localhost only) and served on /api/dbtrail:

* version and start time, from the API and the container;
* the settings that shape the copy: SQL memory, index retention, the copy
  schedule, capacity of the index;
* capture status (stream position, freshness, continuity) per server;
* the latest copy jobs (full reads and refreshes) with their duration and
  outcome, from DBTrail's own run history file.

The API token comes from a file: either a plain token or DBTrail's .env file
with a CONSOLE_TOKEN= line. Every part is best effort and cached for a few
seconds; what cannot be read is reported, not fatal.
"""
import datetime as dt
import glob
import json
import threading
import time
import urllib.request

CACHE_SECONDS = 15
JOBS_KEPT = 60


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


def container_started(name_part="bintrail"):
    """(image, started epoch) of the Docker container whose name contains name_part, read from Docker's files."""
    for cfg in glob.glob("/var/lib/docker/containers/*/config.v2.json"):
        try:
            with open(cfg) as fh:
                c = json.load(fh)
        except (OSError, ValueError):
            continue
        if name_part in c.get("Name", "") and c.get("State", {}).get("Running"):
            return c.get("Config", {}).get("Image"), _ts(c["State"].get("StartedAt", "")[:26] + "Z")
    return None, None


class DBTrailInfo:
    def __init__(self, console_url, token_file=None, history_file=None):
        self.url = console_url.rstrip("/")
        self.token_file = token_file
        self.history_file = history_file
        self.lock = threading.Lock()
        self.cached, self.cached_at = None, 0.0

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
