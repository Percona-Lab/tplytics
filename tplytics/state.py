"""Discovery of the database size and the time range of its transactions.

Everything here uses primary-key lookups only, so it is cheap on any size of
database and can be refreshed while HammerDB keeps adding orders.
"""
import datetime as dt
import threading
import time

SAMPLE_WAREHOUSES = 16


def _one(session, sql):
    _, rows = session.execute(sql)
    return rows[0] if rows else None


def _to_dt(v):
    if v is None or isinstance(v, dt.datetime):
        return v.replace(tzinfo=None) if v is not None and v.tzinfo else v
    if isinstance(v, dt.date):
        return dt.datetime(v.year, v.month, v.day)
    return dt.datetime.fromisoformat(str(v))


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

        # Time range: first and last order of district 1 in up to 16 warehouses spread
        # over the whole id range (each lookup is a primary-key range read).
        n = s["warehouses"]
        step = max(1, n // SAMPLE_WAREHOUSES)
        sample = sorted(set(list(range(s["w_min"], s["w_max"] + 1, step))[:SAMPLE_WAREHOUSES] + [s["w_max"]]))
        firsts, lasts, live = [], [], 0
        for w in sample:
            for d in (1, s["districts"]):
                r = _one(session, "SELECT MIN(o_id), MAX(o_id) FROM orders WHERE o_w_id = %d AND o_d_id = %d" % (w, d))
                if not r or r[0] is None:
                    continue
                live += int(r[1]) - int(r[0]) + 1
                firsts.append(_to_dt(_one(session, "SELECT o_entry_d FROM orders WHERE o_w_id = %d AND o_d_id = %d AND o_id = %d" % (w, d, r[0]))[0]))
                lasts.append(_to_dt(_one(session, "SELECT o_entry_d FROM orders WHERE o_w_id = %d AND o_d_id = %d AND o_id = %d" % (w, d, r[1]))[0]))
        if not firsts:
            raise SystemExit("the orders table is empty: nothing to analyse")
        s["t_min"] = min(firsts)
        s["t_max"] = max(lasts)
        # Orders still stored (a purge job may have removed old ones), extrapolated from the sample.
        s["orders_stored_est"] = int(live / len(firsts) * n * s["districts"])
        with self.lock:
            self.__dict__.update(s)
            self.refreshed = time.time()

    @property
    def span_seconds(self):
        return max(1.0, (self.t_max - self.t_min).total_seconds())

    def as_dict(self):
        keys = ["warehouses", "w_min", "w_max", "districts", "items", "customers_per_district",
                "orders_created", "orders_stored_est", "t_min", "t_max"]
        d = {k: getattr(self, k) for k in keys}
        d["t_min"] = self.t_min.isoformat(sep=" ")
        d["t_max"] = self.t_max.isoformat(sep=" ")
        d["span_seconds"] = round(self.span_seconds)
        return d

    def summary(self):
        return ("%d warehouses (%d..%d), %d districts/warehouse, %d items, %d customers/district; "
                "%s orders stored (est.), transactions from %s to %s (%s)"
                % (self.warehouses, self.w_min, self.w_max, self.districts, self.items,
                   self.customers_per_district, format(self.orders_stored_est, ","),
                   self.t_min.strftime("%Y-%m-%d %H:%M:%S"), self.t_max.strftime("%Y-%m-%d %H:%M:%S"),
                   human_duration(self.span_seconds)))


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
    """Refreshes DatabaseState periodically on its own connection."""

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
