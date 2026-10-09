"""Discovery of the database size and of the time ranges its data covers.

The benchmark loops read this again before every round (one run of every
query), so all parameters of a round are drawn from what the database holds at
its start. Three time ranges are found, because they differ on a running
TPROC-C database and a window outside its own range finds nothing:

  orders     entry time of the orders stored (o_entry_d)
  delivered  entry time of the orders already delivered (o_carrier_id set):
             Delivery takes the oldest new_order of a district, so with a
             backlog these can be far older than the newest orders
  history    payment time of the payments stored (h_date): a purge job may
             keep a much shorter history than orders

Orders and delivered orders come from primary-key ranges of a sample of
districts, in a handful of statements. History uses its id column when it has
one (Parma Ham adds it) and otherwise one scan of the table, which is small
next to the scans the history queries do.
"""
import datetime as dt
import threading
import time

SAMPLE_WAREHOUSES = 16
TIME_SOURCES = ("orders", "delivered", "history")


def _one(session, sql):
    _, rows = session.execute(sql)
    return rows[0] if rows else None


def _to_dt(v):
    if v is None or isinstance(v, dt.datetime):
        return v.replace(tzinfo=None) if v is not None and v.tzinfo else v
    if isinstance(v, dt.date):
        return dt.datetime(v.year, v.month, v.day)
    return dt.datetime.fromisoformat(str(v))


def _history_range(session):
    """(first, last) payment time, or None for an empty history."""
    try:
        lo, hi = _one(session, "SELECT MIN(id), MAX(id) FROM history")
    except Exception:  # noqa: BLE001 - no id column: plain HammerDB schema
        r = _one(session, "SELECT MIN(h_date), MAX(h_date) FROM history")
        return (_to_dt(r[0]), _to_dt(r[1])) if r and r[0] is not None else None
    if lo is None:
        return None
    first = _one(session, "SELECT h_date FROM history WHERE id = %d" % int(lo))
    last = _one(session, "SELECT h_date FROM history WHERE id = %d" % int(hi))
    return _to_dt(first[0]), _to_dt(last[0])


class DatabaseState:
    """What the database looks like right now; drives parameter generation."""

    def __init__(self, session):
        self.lock = threading.Lock()
        self.refresh(session)

    def refresh(self, session):
        s = {}
        s["warehouses"], s["w_min"], s["w_max"] = _one(
            session, "SELECT COUNT(*), MIN(w_id), MAX(w_id) FROM warehouse")
        if not s["warehouses"]:
            raise SystemExit("the warehouse table is empty: is this a HammerDB TPROC-C schema?")
        s["districts"] = _one(session, "SELECT MAX(d_id) FROM district")[0]
        s["items"] = _one(session, "SELECT MAX(i_id) FROM item")[0]
        s["customers_per_district"] = _one(
            session, "SELECT MAX(c_id) FROM customer WHERE c_w_id = %d AND c_d_id = 1" % s["w_min"])[0]
        lo, hi, total = _one(session, "SELECT MIN(d_next_o_id), MAX(d_next_o_id), SUM(d_next_o_id - 1) FROM district")
        s["next_o_id_min"], s["next_o_id_max"], s["orders_created"] = int(lo), int(hi), int(total)

        # Orders and delivered orders: first, last and last delivered order of
        # district 1 and the last district in up to 16 warehouses spread over the
        # whole id range. A few statements for all of them (primary-key ranges on
        # MySQL and PostgreSQL; one scan each on a column store, where every
        # statement has a fixed cost).
        n = s["warehouses"]
        step = max(1, n // SAMPLE_WAREHOUSES)
        sample = sorted(set(list(range(s["w_min"], s["w_max"] + 1, step))[:SAMPLE_WAREHOUSES] + [s["w_max"]]))
        pairs = [(w, d) for w in sample for d in sorted({1, s["districts"]})]

        def where(prefix, extra=None):
            return " OR ".join("(%s_w_id = %d AND %s_d_id = %d%s)" % (prefix, w, prefix, d, extra(w, d) if extra else "")
                               for w, d in pairs if not extra or extra(w, d) is not None)

        def rows(sql):
            return session.execute(sql)[1]
        bounds = {(int(r[0]), int(r[1])): (int(r[2]), int(r[3]), int(r[4])) for r in rows(
            "SELECT o_w_id, o_d_id, MIN(o_id), MAX(o_id), COUNT(*) FROM orders WHERE %s GROUP BY o_w_id, o_d_id"
            % where("o")) if r[2] is not None}
        # Delivery removes the oldest new_order of the district: every order below
        # the oldest one still waiting has been delivered (a purge job may have
        # removed some of them, so look for the newest one left).
        waiting = {(int(r[0]), int(r[1])): int(r[2]) for r in rows(
            "SELECT no_w_id, no_d_id, MIN(no_o_id) FROM new_order WHERE %s GROUP BY no_w_id, no_d_id"
            % where("no")) if r[2] is not None}
        delivered = {k: b[1] for k, b in bounds.items() if k not in waiting}
        cond = where("o", lambda w, d: " AND o_id < %d" % waiting[(w, d)] if (w, d) in waiting and (w, d) in bounds
                     else None)
        if cond:
            delivered.update({(int(r[0]), int(r[1])): int(r[2]) for r in rows(
                "SELECT o_w_id, o_d_id, MAX(o_id) FROM orders WHERE %s GROUP BY o_w_id, o_d_id" % cond)
                if r[2] is not None})
        wanted = {k: {b[0], b[1]} | ({delivered[k]} if k in delivered else set()) for k, b in bounds.items()}
        entry = {}
        if wanted:
            for r in rows("SELECT o_w_id, o_d_id, o_id, o_entry_d FROM orders WHERE %s" % where(
                    "o", lambda w, d: " AND o_id IN (%s)" % ", ".join(str(o) for o in sorted(wanted[(w, d)]))
                    if (w, d) in wanted else None)):
                entry[(int(r[0]), int(r[1]), int(r[2]))] = _to_dt(r[3])
        firsts = [entry[k + (b[0],)] for k, b in bounds.items() if k + (b[0],) in entry]
        lasts = [entry[k + (b[1],)] for k, b in bounds.items() if k + (b[1],) in entry]
        d_lasts = [entry[k + (o,)] for k, o in delivered.items() if k + (o,) in entry]
        live = sum(b[2] for b in bounds.values())
        if not firsts:
            raise SystemExit("the orders table is empty: nothing to analyse")
        s["t_min"] = min(firsts)
        s["t_max"] = max(lasts)
        ranges = {"orders": (s["t_min"], s["t_max"])}
        ranges["delivered"] = (s["t_min"], max(d_lasts)) if d_lasts else None
        ranges["history"] = _history_range(session)
        s["ranges"] = ranges
        # Orders still stored (a purge job may have removed old ones), extrapolated from the sample.
        s["orders_stored_est"] = int(live / len(bounds) * n * s["districts"])
        with self.lock:
            self.__dict__.update(s)
            self.refreshed = time.time()

    @property
    def span_seconds(self):
        return max(1.0, (self.t_max - self.t_min).total_seconds())

    def time_range(self, source="orders"):
        return time_range(self, source)

    def as_dict(self):
        keys = ["warehouses", "w_min", "w_max", "districts", "items", "customers_per_district",
                "orders_created", "orders_stored_est", "t_min", "t_max"]
        d = {k: getattr(self, k) for k in keys}
        d["t_min"] = self.t_min.isoformat(sep=" ")
        d["t_max"] = self.t_max.isoformat(sep=" ")
        d["span_seconds"] = round(self.span_seconds)
        for src in TIME_SOURCES[1:]:
            r = self.ranges.get(src)
            d[src + "_range"] = "%s .. %s" % (r[0].isoformat(sep=" "), r[1].isoformat(sep=" ")) if r else "none"
        return d

    def summary(self):
        return ("%d warehouses (%d..%d), %d districts/warehouse, %d items, %d customers/district; "
                "%s orders stored (est.), orders from %s to %s (%s); %s; %s"
                % (self.warehouses, self.w_min, self.w_max, self.districts, self.items,
                   self.customers_per_district, format(self.orders_stored_est, ","),
                   self.t_min.strftime("%Y-%m-%d %H:%M:%S"), self.t_max.strftime("%Y-%m-%d %H:%M:%S"),
                   human_duration(self.span_seconds),
                   describe_range("delivered", self.ranges.get("delivered")),
                   describe_range("payments", self.ranges.get("history"))))


def time_range(state, source="orders"):
    """(start, end) of the data a time window over `source` should fall in.

    A source the database has no data for (or a state without per-source
    ranges, as in tests) falls back to the orders range.
    """
    if source not in TIME_SOURCES:
        raise ValueError("unknown time source %r (one of %s)" % (source, ", ".join(TIME_SOURCES)))
    ranges = getattr(state, "ranges", None) or {}
    r = ranges.get(source)
    return r if r else (state.t_min, state.t_max)


def describe_range(label, r):
    if not r:
        return "no %s" % label
    return "%s from %s to %s (%s)" % (label, r[0].strftime("%Y-%m-%d %H:%M:%S"), r[1].strftime("%Y-%m-%d %H:%M:%S"),
                                      human_duration((r[1] - r[0]).total_seconds()))


def refresh_state(engine, state):
    """Read the database again into `state` on a connection of its own; the error text, or None."""
    session = None
    try:
        session = engine.connect()
        state.refresh(session)
        return None
    except (Exception, SystemExit) as e:  # noqa: BLE001 - keep the previous state, report why
        return str(e).replace("\n", " ")[:300]
    finally:
        if session is not None:
            session.close()


def human_duration(sec):
    sec = int(sec)
    if sec < 120:
        return "%ds" % sec
    if sec < 7200:
        return "%dm" % (sec // 60)
    if sec < 172800:
        return "%.1fh" % (sec / 3600)
    return "%.1fd" % (sec / 86400)


class StateRefresher(threading.Thread):
    """Refreshes DatabaseState periodically on its own connection (concurrent mode: it has no rounds)."""

    def __init__(self, engine, state, interval):
        super().__init__(daemon=True, name="state-refresh")
        self.engine, self.state, self.interval = engine, state, interval
        self.stop_event = threading.Event()

    def run(self):
        session = None
        while not self.stop_event.wait(self.interval):
            try:
                if session is None:
                    session = self.engine.connect()
                self.state.refresh(session)
            except Exception:
                if session is not None:
                    session.close()
                session = None
        if session is not None:
            session.close()

    def stop(self):
        self.stop_event.set()
