"""Executing one query with a timeout and classifying the outcome."""
import threading
import time

OK, ERROR, TIMEOUT, UNSUPPORTED, CANCELLED = "ok", "error", "timeout", "unsupported", "cancelled"

# The server is asked to stop the statement at the timeout; if it has not
# returned WATCHDOG_GRACE seconds later the client cancels it (KILL QUERY,
# PostgreSQL cancel request) and reports a timeout.
WATCHDOG_GRACE = 2.0


class Result:
    __slots__ = ("query_id", "status", "start", "elapsed", "rows", "error", "columns", "data",
                 "params", "thread")

    def __init__(self, query_id, status, start, elapsed, rows=0, error=None, columns=None,
                 data=None, params=None, thread=0):
        self.query_id, self.status, self.start, self.elapsed = query_id, status, start, elapsed
        self.rows, self.error, self.columns, self.data = rows, error, columns, data
        self.params, self.thread = params, thread


class Runner:
    """Runs statements on one session; owns reconnects after failures."""

    def __init__(self, engine, timeout, init_sql=(), thread=0):
        self.engine = engine
        self.timeout = timeout
        self.init_sql = list(init_sql)
        self.thread = thread
        self.session = None
        self.abort_reason = None   # set by stop(): cancel the running statement, do not count it
        self._lock = threading.Lock()
        self._running = False

    def connect(self):
        self.session = self.engine.connect()
        self.session.set_timeout(self.timeout)
        self.session.run_init_sql(self.init_sql)
        return self.session

    def close(self):
        if self.session is not None:
            self.session.close()
            self.session = None

    def _cancel(self):
        with self._lock:
            if not self._running:
                return
        try:
            self.session.cancel()
        except Exception:
            pass

    def stop(self, reason="benchmark finished"):
        """Cancel the in-flight statement (from another thread); its result is CANCELLED."""
        self.abort_reason = reason
        self._cancel()

    def run_query(self, query, state, rng, keep_rows=0):
        """Draw parameters, render and run one query; problems become ERROR results."""
        try:
            values = query.new_params(state, rng)
            sql = query.render(self.engine, values)
        except Exception as e:  # noqa: BLE001
            return Result(query.id, ERROR, time.time(), 0.0, error="parameters: %s" % _msg(e), thread=self.thread)
        return self.run(query.id, sql, keep_rows=keep_rows, params=values)

    def run(self, query_id, sql, keep_rows=0, params=None):
        if self.session is None:
            try:
                self.connect()
            except Exception as e:
                return Result(query_id, ERROR, time.time(), 0.0, error="connect: %s" % _msg(e),
                              params=params, thread=self.thread)
        fired = threading.Event()

        def watchdog():
            fired.set()
            self._cancel()

        timer = threading.Timer(self.timeout + WATCHDOG_GRACE, watchdog)
        timer.daemon = True
        wall = time.time()
        t0 = time.perf_counter()
        with self._lock:
            self._running = True
        timer.start()
        try:
            cols, rows = self.session.execute(sql)
            elapsed = time.perf_counter() - t0
            status, err = OK, None
        except Exception as e:  # noqa: BLE001 - any database error is a result
            elapsed = time.perf_counter() - t0
            cols, rows = None, []
            if self.abort_reason:
                status, err = CANCELLED, self.abort_reason
            elif fired.is_set() or self.session.is_timeout_error(e) or elapsed >= self.timeout * 0.995:
                status, err = TIMEOUT, "exceeded %gs: %s" % (self.timeout, _msg(e))
            else:
                status, err = ERROR, _msg(e)
            if self.session.is_disconnect(e) or fired.is_set():
                self._reconnect()
        finally:
            timer.cancel()
            with self._lock:
                self._running = False
        if status == OK and self.abort_reason:
            status = CANCELLED
        return Result(query_id, status, wall, elapsed, rows=len(rows), error=err, columns=cols,
                      data=rows[:keep_rows] if keep_rows else None, params=params, thread=self.thread)

    def _reconnect(self):
        try:
            self.session.close()
        except Exception:
            pass
        self.session = None
        try:
            self.connect()
        except Exception:
            self.session = None


def _msg(e):
    s = str(e).strip().replace("\n", " ")
    if s.startswith("(") and "," in s:   # pymysql: (1064, "message")
        s = s.strip("()")
    return (type(e).__name__ + ": " + s)[:500]
