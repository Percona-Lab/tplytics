"""Detailed host and database performance for the compare dashboard's /performance page.

PerfSampler samples, every --step seconds (5 by default) and keeps 15
minutes of points:

* OS, from /proc: CPU split (user, system, iowait, steal, other), load and
  run queue, pressure stall information (cpu/memory/io), memory and swap,
  swap and major-fault rates, disk IOPS / MB/s / utilisation over whole
  disks, network throughput, context switches.
* Units: CPU % and memory of every systemd service and Docker container,
  read from their cgroups (cgroup v2); a container is named from Docker's own
  config file, so no Docker client is needed.
* Database: for a MySQL-family engine, rates and gauges from SHOW GLOBAL STATUS
  and the InnoDB history list; for a Prometheus endpoint (DBTrail's
  /metrics), its capture and index metrics.

Everything is best effort: what a host does not have is left out.
"""
import collections
import json
import os
import re
import socket
import threading
import time
import urllib.request

RAW_POINTS = 720             # 1 hour at 5 s
MINUTE_POINTS = 1440         # 24 hours of 1-minute averages
RANGES = {"15m": 900, "1h": 3600, "6h": 6 * 3600, "24h": 24 * 3600}


def _average(points):
    """One point from several: the mean of every numeric field, the last time."""
    out = {"t": points[-1]["t"]}
    keys = set().union(*points)
    for k in keys:
        if k == "t":
            continue
        vals = [p[k] for p in points if isinstance(p.get(k), (int, float))]
        if vals:
            out[k] = round(sum(vals) / len(vals), 3)
    return out


def _downsample(points, every):
    """Average consecutive groups of `every` points (newest group may be partial)."""
    if every <= 1:
        return list(points)
    pts = list(points)
    return [_average(pts[i:i + every]) for i in range(0, len(pts), every)]
CG = "/sys/fs/cgroup"


# ======================================================================= OS
def _read(path, default=""):
    try:
        with open(path) as fh:
            return fh.read()
    except OSError:
        return default


def _whole_disks():
    out = set()
    for d in os.listdir("/sys/block"):
        if not d.startswith(("loop", "ram", "zram", "dm-", "sr", "fd", "md")):
            out.add(d)
    return out


def read_os(disks):
    s = {}
    for line in _read("/proc/stat").splitlines():
        p = line.split()
        if not p:
            continue
        if p[0] == "cpu":
            v = [int(x) for x in p[1:]] + [0] * 10
            user, nice, system, idle, iowait, irq, softirq, steal = v[:8]
            s["cpu"] = {"user": user + nice, "system": system, "irq": irq + softirq, "iowait": iowait,
                        "steal": steal, "idle": idle}
        elif p[0] == "ctxt":
            s["ctxt"] = int(p[1])
        elif p[0] == "procs_running":
            s["procs_running"] = int(p[1])
        elif p[0] == "procs_blocked":
            s["procs_blocked"] = int(p[1])
    la = _read("/proc/loadavg").split()
    if la:
        s["load1"] = float(la[0])
    mem = {}
    for line in _read("/proc/meminfo").splitlines():
        k, _, v = line.partition(":")
        mem[k] = int(v.split()[0]) if v.split() else 0
    if mem:
        s["mem"] = {"total": mem.get("MemTotal", 0), "available": mem.get("MemAvailable", 0),
                    "cache": mem.get("Cached", 0) + mem.get("Buffers", 0) + mem.get("SReclaimable", 0),
                    "swap_used": mem.get("SwapTotal", 0) - mem.get("SwapFree", 0),
                    "swap_total": mem.get("SwapTotal", 0), "dirty": mem.get("Dirty", 0)}
    vm = {}
    for line in _read("/proc/vmstat").splitlines():
        k, _, v = line.partition(" ")
        if k in ("pswpin", "pswpout", "pgmajfault"):
            vm[k] = int(v)
    s["vm"] = vm
    rio = wio = rs = ws = ticks = 0
    for line in _read("/proc/diskstats").splitlines():
        p = line.split()
        if len(p) >= 13 and p[2] in disks:
            rio += int(p[3]); rs += int(p[5]); wio += int(p[7]); ws += int(p[9]); ticks = max(ticks, int(p[12]))
    s["disk"] = {"rio": rio, "wio": wio, "rbytes": rs * 512, "wbytes": ws * 512, "busy_ms": ticks}
    rx = tx = 0
    for line in _read("/proc/net/dev").splitlines()[2:]:
        name, _, rest = line.partition(":")
        name = name.strip()
        if name == "lo" or name.startswith(("veth", "docker", "br-", "virbr")):
            continue
        f = rest.split()
        if len(f) >= 9:
            rx += int(f[0]); tx += int(f[8])
    s["net"] = {"rx": rx, "tx": tx}
    psi = {}
    for res in ("cpu", "memory", "io"):
        for line in _read("/proc/pressure/%s" % res).splitlines():
            kind = line.split()[0]
            m = re.search(r"total=(\d+)", line)
            if m:
                psi["%s_%s" % (res, kind)] = int(m.group(1))
    s["psi"] = psi
    return s


# ==================================================================== units
def _docker_name(cid):
    try:
        with open("/var/lib/docker/containers/%s/config.v2.json" % cid) as fh:
            return "container " + json.load(fh).get("Name", cid[:12]).lstrip("/")
    except (OSError, ValueError):
        return "container " + cid[:12]


def unit_cgroup(name, names_cache):
    """The cgroup directory of a unit name as read_units reports it."""
    if name.startswith("container "):
        cid = next((k for k, v in names_cache.items() if v == name), None)
        return os.path.join(CG, "system.slice", "docker-%s.scope" % cid) if cid else None
    return os.path.join(CG, "system.slice", name + ".service")


def main_process(cgdir):
    """(command name, processes) of a cgroup: the process using the most memory names it."""
    try:
        with open(os.path.join(cgdir, "cgroup.procs")) as fh:
            pids = [x.strip() for x in fh if x.strip()]
    except OSError:
        return None, 0
    best, best_rss = None, -1
    for pid in pids[:400]:
        try:
            rss = int(_read("/proc/%s/statm" % pid).split()[1])
            comm = _read("/proc/%s/comm" % pid).strip()
        except (IndexError, ValueError):
            continue
        if rss > best_rss:
            best, best_rss = comm, rss
    return best, len(pids)


def read_units(names_cache):
    """{unit name: (cpu usage µs, memory bytes)} for services and containers."""
    out = {}
    base = os.path.join(CG, "system.slice")
    try:
        entries = os.listdir(base)
    except OSError:
        return out
    for e in entries:
        if e.endswith(".service"):
            name = e[:-8]
        elif e.startswith("docker-") and e.endswith(".scope"):
            cid = e[7:-6]
            if cid not in names_cache:
                names_cache[cid] = _docker_name(cid)
            name = names_cache[cid]
        else:
            continue
        cpu = re.search(r"usage_usec (\d+)", _read(os.path.join(base, e, "cpu.stat")))
        mem = _read(os.path.join(base, e, "memory.current")).strip()
        if cpu:
            out[name] = (int(cpu.group(1)), int(mem) if mem.isdigit() else 0)
    return out


# ================================================================= database
MYSQL_RATES = {
    "qps": ("Questions",), "tps": ("Com_commit", "Com_rollback"),
    "rows_read": ("Innodb_rows_read",), "rows_inserted": ("Innodb_rows_inserted",),
    "rows_updated": ("Innodb_rows_updated",), "rows_deleted": ("Innodb_rows_deleted",),
    "data_read": ("Innodb_data_read",), "data_written": ("Innodb_data_written",),
    "redo_written": ("Innodb_os_log_written",), "bp_requests": ("Innodb_buffer_pool_read_requests",),
    "bp_disk_reads": ("Innodb_buffer_pool_reads",), "lock_waits": ("Innodb_row_lock_waits",),
}
MYSQL_GAUGES = {"threads_running": "Threads_running", "threads_connected": "Threads_connected",
                "bp_pages_dirty": "Innodb_buffer_pool_pages_dirty", "bp_pages_total": "Innodb_buffer_pool_pages_total",
                "bp_pages_free": "Innodb_buffer_pool_pages_free"}


def mysql_sample(session):
    _, rows = session.execute("SHOW GLOBAL STATUS")
    st = {str(r[0]): r[1] for r in rows}

    def num(k):
        try:
            return float(st.get(k) or 0)
        except ValueError:
            return 0.0
    out = {"c": {k: sum(num(n) for n in names) for k, names in MYSQL_RATES.items()},
           "g": {k: num(n) for k, n in MYSQL_GAUGES.items()}}
    try:
        _, rows = session.execute("SELECT `count` FROM information_schema.innodb_metrics WHERE name = 'trx_rseg_history_len'")
        if rows:
            out["g"]["history_list"] = float(rows[0][0] or 0)
    except Exception:  # noqa: BLE001 - not every flavour has it
        pass
    cur, ckpt = num("Innodb_redo_log_current_lsn"), num("Innodb_redo_log_checkpoint_lsn")
    if cur:
        out["g"]["checkpoint_age_mb"] = max(0.0, cur - ckpt) / 1048576
    return out


PROM_RATES = {"events_indexed": "bintrail_stream_events_indexed_total",
              "events_received": "bintrail_stream_events_received_total",
              "batch_flushes": "bintrail_stream_batch_flushes_total",
              "commit_latency_sum": "bintrail_stream_index_commit_latency_seconds_sum",
              "commit_latency_count": "bintrail_stream_index_commit_latency_seconds_count",
              "sql_wait_sum": "bintrail_sql_slot_wait_seconds_sum",
              "proc_cpu": "bintrail_process_cpu_seconds_total",
              "dml_dropped": "bintrail_statement_dml_dropped_total",
              "rows_dropped": "bintrail_unhandled_rows_dropped_total",
              "checkpoints": "bintrail_stream_checkpoint_saves_total"}
PROM_GAUGES = {"stream_lag": "bintrail_stream_availability_lag_seconds",
               "replication_lag": "bintrail_stream_replication_lag_seconds",
               "index_bytes": "bintrail_index_storage_bytes", "index_events": "bintrail_index_events_total",
               "index_gap_hours": "bintrail_index_gap_hours",
               "rotation_healthy": "bintrail_rotation_healthy",
               "gap_lost": "bintrail_continuity_gap_lost",
               "sql_waiting": "bintrail_sql_slot_waiting",
               "rss_bytes": "process_resident_memory_bytes"}
# SQL on the copy: statements and time per phase (histogram _sum / _count by phase label)
PROM_PHASES = ("total", "slot_wait", "spawn", "open", "views", "view_build", "query", "decode", "lockdown")


def prometheus_sample(url):
    with urllib.request.urlopen(url, timeout=3) as r:
        text = r.read().decode("utf-8", "replace")
    vals = collections.defaultdict(float)
    phase = collections.defaultdict(float)
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        name = re.split(r"[{ ]", line, maxsplit=1)[0]
        try:
            v = float(line.rsplit(" ", 1)[1])
        except (ValueError, IndexError):
            continue
        vals[name] += v
        if name.startswith("bintrail_sql_statement_phase_seconds_"):
            m = re.search(r'phase="([^"]+)"', line)
            if m and name.endswith(("_sum", "_count")):
                phase[(m.group(1), name.rsplit("_", 1)[1])] += v
    vals["bintrail_process_cpu_seconds_total"] = vals.get("process_cpu_seconds_total", 0.0)
    c = {k: vals.get(n, 0.0) for k, n in PROM_RATES.items()}
    for ph in PROM_PHASES:
        if (ph, "count") in phase:
            c["sqlph_%s_sum" % ph] = phase[(ph, "sum")]
            c["sqlph_%s_count" % ph] = phase[(ph, "count")]
    g = {k: vals.get(n, 0.0) for k, n in PROM_GAUGES.items() if n in vals}
    if "process_start_time_seconds" in vals:
        g["started"] = vals["process_start_time_seconds"]
    return {"c": c, "g": g}


# ================================================================== sampler
class PerfSampler(threading.Thread):
    def __init__(self, engine=None, prometheus_url=None, step=5.0, db_kind=None):
        super().__init__(daemon=True, name="perf")
        self.engine, self.prom, self.step = engine, prometheus_url, step
        self.db_kind = db_kind or ("dbtrail" if prometheus_url else (engine.name if engine else None))
        self.disks = _whole_disks() if os.path.isdir("/sys/block") else set()
        # 5 s points for the last hour, and 1-minute averages for 24 hours
        self.series = collections.deque(maxlen=RAW_POINTS)
        self.minutes = collections.deque(maxlen=MINUTE_POINTS)
        self.units = collections.defaultdict(lambda: collections.deque(maxlen=RAW_POINTS))
        self.unit_minutes = collections.defaultdict(lambda: collections.deque(maxlen=MINUTE_POINTS))
        self.bucket, self.unit_bucket, self.bucket_minute = [], collections.defaultdict(list), None
        self.names = {}
        self.lock = threading.Lock()
        self.session = None
        self.cpus = os.cpu_count() or 1
        self.db_error = None

    def _db(self):
        if self.prom:
            return prometheus_sample(self.prom)
        if self.engine is None or getattr(self.engine, "name", "") not in ("mysql", "mariadb"):
            return None
        if self.session is None:
            self.session = self.engine.connect()
        return mysql_sample(self.session)

    def _take(self):
        t = time.time()
        cur = {"t": t, "os": read_os(self.disks), "units": read_units(self.names)}
        try:
            cur["db"] = self._db()
            self.db_error = None
        except Exception as e:  # noqa: BLE001 - best effort
            self.db_error = str(e)[:200]
            if self.session is not None:
                try:
                    self.session.close()
                except Exception:  # noqa: BLE001
                    pass
            self.session = None
        return cur

    def run(self):
        prev = self._take()
        while True:
            time.sleep(max(0.2, self.step - (time.time() - prev["t"])))
            cur = self._take()
            try:
                self._add(prev, cur)
            except Exception:  # noqa: BLE001 - a bad sample is skipped, not fatal
                pass
            prev = cur

    def _add(self, a, b):
        dt = max(0.001, b["t"] - a["t"])
        rate = lambda x, y: max(0.0, (y - x) / dt)  # noqa: E731
        p = {"t": round(b["t"], 1)}
        oa, ob = a["os"], b["os"]
        if "cpu" in oa and "cpu" in ob:
            d = {k: ob["cpu"][k] - oa["cpu"][k] for k in ob["cpu"]}
            tot = max(1, sum(d.values()))
            for k in ("user", "system", "irq", "iowait", "steal"):
                p["cpu_" + k] = round(100.0 * d[k] / tot, 1)
        p["load1"] = ob.get("load1")
        p["run_queue"] = ob.get("procs_running")
        p["blocked"] = ob.get("procs_blocked")
        if "ctxt" in oa and "ctxt" in ob:
            p["ctxt"] = round(rate(oa["ctxt"], ob["ctxt"]))
        m = ob.get("mem")
        if m:
            p["mem_used_mb"] = round((m["total"] - m["available"]) / 1024)
            p["mem_cache_mb"] = round(m["cache"] / 1024)
            p["mem_avail_mb"] = round(m["available"] / 1024)
            p["swap_used_mb"] = round(m["swap_used"] / 1024)
            p["dirty_mb"] = round(m["dirty"] / 1024)
        va, vb = oa.get("vm", {}), ob.get("vm", {})
        for k, name in (("pswpin", "swap_in"), ("pswpout", "swap_out"), ("pgmajfault", "major_faults")):
            if k in va and k in vb:
                p[name] = round(rate(va[k], vb[k]) * (4 / 1024 if k != "pgmajfault" else 1), 2)   # swap in MB/s
        da, db = oa["disk"], ob["disk"]
        p["read_iops"] = round(rate(da["rio"], db["rio"]))
        p["write_iops"] = round(rate(da["wio"], db["wio"]))
        p["read_mb"] = round(rate(da["rbytes"], db["rbytes"]) / 1048576, 2)
        p["write_mb"] = round(rate(da["wbytes"], db["wbytes"]) / 1048576, 2)
        p["disk_util"] = round(min(100.0, (db["busy_ms"] - da["busy_ms"]) / (dt * 10)), 1)
        p["net_rx_mb"] = round(rate(oa["net"]["rx"], ob["net"]["rx"]) / 1048576, 3)
        p["net_tx_mb"] = round(rate(oa["net"]["tx"], ob["net"]["tx"]) / 1048576, 3)
        pa, pb = oa.get("psi", {}), ob.get("psi", {})
        for k in pb:
            if k in pa:
                p["psi_" + k] = round(min(100.0, (pb[k] - pa[k]) / (dt * 1e4)), 2)   # µs per s -> %
        if a.get("db") and b.get("db"):
            ca, cb = a["db"]["c"], b["db"]["c"]
            for k in cb:
                if k in ca:
                    p["db_" + k] = round(rate(ca[k], cb[k]), 3)
            for k, v in b["db"]["g"].items():
                p["db_" + k] = v
            if "db_bp_requests" in p and p["db_bp_requests"] > 0:
                p["db_bp_hit"] = round(100.0 * (1 - p.get("db_bp_disk_reads", 0) / p["db_bp_requests"]), 3)
            if p.get("db_commit_latency_count"):
                p["db_commit_latency_ms"] = round(1000 * p["db_commit_latency_sum"] / p["db_commit_latency_count"], 2)
            if "db_proc_cpu" in p:
                p["db_proc_cpu"] = round(100.0 * p["db_proc_cpu"], 1)          # % of one CPU
            if "db_rss_bytes" in p:
                p["db_rss_mb"] = round(p.pop("db_rss_bytes") / 1048576)
            # SQL on the copy: statements/s and the average time per statement of each phase
            n = p.get("db_sqlph_total_count")
            if n is not None:
                p["db_sql_per_s"] = n
                for ph in PROM_PHASES:
                    s, k = p.pop("db_sqlph_%s_sum" % ph, None), p.pop("db_sqlph_%s_count" % ph, None)
                    if s is not None and k:
                        p["db_sql_%s_s" % ph] = round(s / k, 3)
        ua, ub = a["units"], b["units"]
        with self.lock:
            self.series.append(p)
            minute = int(p["t"] // 60)
            if self.bucket_minute is not None and minute != self.bucket_minute and self.bucket:
                self.minutes.append(_average(self.bucket))          # close the finished minute
                for name, pts in self.unit_bucket.items():
                    if pts:
                        self.unit_minutes[name].append((pts[-1][0], round(sum(x[1] for x in pts) / len(pts), 1),
                                                        round(sum(x[2] for x in pts) / len(pts))))
                self.bucket, self.unit_bucket = [], collections.defaultdict(list)
            self.bucket_minute = minute
            self.bucket.append(p)
            for name, (cpu_us, mem) in ub.items():
                if name in ua:
                    cpu = round(100.0 * (cpu_us - ua[name][0]) / (dt * 1e6), 1)   # % of one CPU
                    pt = (p["t"], max(0.0, cpu), round(mem / 1048576))
                    self.units[name].append(pt)
                    self.unit_bucket[name].append(pt)

    def snapshot(self, rng="1h"):
        """The window `rng` (15m, 1h, 6h, 24h): 5 s points up to 1 h, 1-minute
        averages for 6 h and 5-minute averages for 24 h."""
        span = RANGES.get(rng, 3600)
        cutoff = time.time() - span
        with self.lock:
            if span <= 3600:
                series = [x for x in self.series if x["t"] >= cutoff]
                unit_pts = {n: [x for x in d if x[0] >= cutoff] for n, d in self.units.items()}
                every = 1
            else:
                series = [x for x in self.minutes if x["t"] >= cutoff]
                unit_pts = {n: [x for x in d if x[0] >= cutoff] for n, d in self.unit_minutes.items()}
                every = 5 if span > 6 * 3600 else 1
            series = _downsample(series, every)
            units = {}
            for name, pts in unit_pts.items():
                if every > 1:
                    pts = [(g[-1][0], round(sum(x[1] for x in g) / len(g), 1), round(sum(x[2] for x in g) / len(g)))
                           for g in (pts[i:i + every] for i in range(0, len(pts), every))]
                if not pts:
                    continue
                cpu = [x[1] for x in pts]
                last = pts[-1]
                if max(cpu) < 0.5 and last[2] < 64:
                    continue                      # idle and small: not worth a row
                mem = [x[2] for x in pts]
                main, nprocs = main_process(unit_cgroup(name, self.names) or "")
                units[name] = {"cpu": last[1], "mem_mb": last[2], "cpu_avg": round(sum(cpu) / len(cpu), 1),
                               "cpu_max": max(cpu), "mem_min": min(mem), "mem_max": max(mem),
                               "mem_change": mem[-1] - mem[0], "main": main, "procs": nprocs,
                               "points": [[x[0], x[1], x[2]] for x in pts]}
            mem_total = 0
            for line in _read("/proc/meminfo").splitlines():
                if line.startswith("MemTotal:"):
                    mem_total = int(line.split()[1]) // 1024
            oldest = (self.minutes[0]["t"] if self.minutes else (self.series[0]["t"] if self.series else time.time()))
            return {"host": socket.gethostname(), "cpus": self.cpus, "mem_total_mb": mem_total,
                    "step": self.step if span <= 3600 else 60 * every, "range": rng if rng in RANGES else "1h",
                    "span": span, "history_seconds": round(time.time() - oldest), "now": time.time(),
                    "db_kind": self.db_kind, "db_error": self.db_error,
                    "psi": os.path.exists("/proc/pressure/cpu"), "series": series, "units": units}
