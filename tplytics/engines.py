"""Database engines.

An engine knows how to connect, how to make the server enforce a statement
timeout, how to cancel a running statement from another thread, how to
format literals, and which SQL dialects it understands.

The ``dialects`` list is the lookup order for query text: a query file can
hold SQL for ``[sql].mariadb``, ``[sql].mysql`` and ``[sql].default``; the
MariaDB engine uses the first one present. Adding an engine that speaks an
existing wire protocol (TiDB, Doris, CockroachDB, YugabyteDB, ...) is
usually a subclass that only changes ``name``, ``dialects`` and the timeout
statement.
"""
import datetime as dt
import decimal
import threading


class QueryTimeout(Exception):
    """The statement ran longer than the configured timeout and was cancelled."""


class Engine:
    name = "abstract"
    dialects = ["default"]
    default_port = None

    def __init__(self, opts):
        self.opts = opts

    # -- literals --------------------------------------------------------
    def literal(self, v):
        if v is None:
            return "NULL"
        if isinstance(v, bool):
            return "1" if v else "0"
        if isinstance(v, (int, decimal.Decimal)):
            return str(v)
        if isinstance(v, float):
            return repr(round(v, 6))
        if isinstance(v, dt.datetime):
            return "TIMESTAMP '%s'" % v.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(v, dt.date):
            return "DATE '%s'" % v.isoformat()
        return "'" + str(v).replace("'", "''") + "'"

    # -- activity ----------------------------------------------------------
    #: statement returning rows whose last column is a cumulative transaction
    #: counter; the dashboard shows its rate as "DB transactions/s". None = not shown.
    activity_sql = None

    #: same, for a cumulative counter of statements: shown as "queries/s". None = not shown.
    queries_sql = None
    #: one row of cumulative row counters, in the order read, inserted, updated,
    #: deleted: shown as rows read/s and rows changed/s. None = not shown.
    rows_sql = None

    def row_counters(self, session):
        if not self.rows_sql:
            return None
        cols, rows = session.execute(self.rows_sql)
        if not rows:
            return None
        if len(rows) > 1 or len(rows[0]) == 2:          # SHOW STATUS: name/value rows
            by_name = {str(r[0]).lower(): float(r[1] or 0) for r in rows}
            return {k: by_name.get("innodb_rows_" + k, 0.0) for k in ("read", "inserted", "updated", "deleted")}
        return dict(zip(("read", "inserted", "updated", "deleted"), (float(v or 0) for v in rows[0])))

    def activity_counter(self, session):
        return self._counter(session, self.activity_sql)

    def queries_counter(self, session):
        return self._counter(session, self.queries_sql)

    @staticmethod
    def _counter(session, sql):
        if not sql:
            return None
        _, rows = session.execute(sql)
        return sum(float(r[-1] or 0) for r in rows)

    # -- heartbeat ---------------------------------------------------------
    #: False for read-only targets (a replica's copy) that cannot be the primary
    heartbeat_writable = True
    heartbeat_ts_type = "TIMESTAMP"

    def heartbeat_ddl(self, table):
        return ("CREATE TABLE IF NOT EXISTS %s (id INT NOT NULL PRIMARY KEY, seq BIGINT NOT NULL, "
                "ts_us BIGINT NOT NULL, interval_us BIGINT NOT NULL, ts %s NOT NULL, writer VARCHAR(255))"
                % (table, self.heartbeat_ts_type))

    # -- connections -----------------------------------------------------
    def connect(self):
        """Return a new Session."""
        raise NotImplementedError

    def describe(self):
        o = self.opts
        if o.socket:
            where = o.socket
        elif o.host or not getattr(o, "defaults_file", None):
            where = "%s:%s" % (o.host or "localhost", o.port or self.default_port)
        else:
            where = "[%s]" % o.defaults_file
        return "%s %s/%s" % (self.name, where, o.database)


class Session:
    """One database connection used by one benchmark thread."""

    #: error codes (driver specific) that mean "statement timeout / cancelled"
    timeout_codes = ()

    def __init__(self, engine):
        self.engine = engine
        self.conn = None
        self.timeout = None
        self._lock = threading.Lock()
        self._running = False
        self.server_version = "?"

    def open(self):
        raise NotImplementedError

    def close(self):
        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            pass
        self.conn = None

    def reopen(self):
        self.close()
        self.open()
        if self.timeout:
            self.set_timeout(self.timeout)

    def set_timeout(self, seconds):
        self.timeout = seconds

    def run_init_sql(self, statements):
        for s in statements:
            self.execute(s, fetch=False)

    def execute(self, sql, fetch=True):
        """Run a statement; return (column names, rows)."""
        raise NotImplementedError

    def cancel(self):
        """Cancel the statement currently running on this session (called from another thread)."""
        raise NotImplementedError

    def is_timeout_error(self, exc):
        code = getattr(exc, "args", [None])[0] if getattr(exc, "args", None) else None
        return code in self.timeout_codes

    def is_disconnect(self, exc):
        return False


# =========================================================== MySQL family
class MySQLEngine(Engine):
    name = "mysql"
    dialects = ["mysql", "default"]
    default_port = 3306

    def connect(self):
        s = MySQLSession(self)
        s.open()
        return s

    def connect_args(self):
        import pymysql
        o = self.opts
        args = dict(database=o.database or "tpcc", autocommit=True, charset="utf8mb4",
                    connect_timeout=30, cursorclass=pymysql.cursors.Cursor)
        if o.defaults_file:
            args["read_default_file"] = o.defaults_file
        if o.socket:
            args["unix_socket"] = o.socket
        elif o.defaults_file:
            # PyMySQL prefers an explicit argument over the option file, so
            # pass the host only when given on the command line, and the
            # port from the command line, else the file, else the engine's.
            if o.host:
                args["host"] = o.host
            args["port"] = int(o.port or _cnf_port(o.defaults_file) or self.default_port)
        else:
            args["host"] = o.host or "127.0.0.1"
            args["port"] = int(o.port or self.default_port)
        if o.user:
            args["user"] = o.user
        elif not o.defaults_file:
            args["user"] = "root"
        if o.password is not None:
            args["password"] = o.password
        return args

    activity_sql = "SHOW GLOBAL STATUS WHERE Variable_name IN ('Com_commit', 'Com_rollback')"
    queries_sql = "SHOW GLOBAL STATUS WHERE Variable_name = 'Queries'"
    rows_sql = ("SHOW GLOBAL STATUS WHERE Variable_name IN "
                "('Innodb_rows_read', 'Innodb_rows_inserted', 'Innodb_rows_updated', 'Innodb_rows_deleted')")
    heartbeat_ts_type = "DATETIME"   # TIMESTAMP would follow the session time zone

    def timeout_statement(self, seconds):
        return "SET SESSION max_execution_time = %d" % int(seconds * 1000)


class MySQLSession(Session):
    # 3024 ER_QUERY_TIMEOUT, 1317 ER_QUERY_INTERRUPTED, 1969 MariaDB ER_STATEMENT_TIMEOUT
    timeout_codes = (3024, 1317, 1969)

    def open(self):
        import pymysql
        self.conn = pymysql.connect(**self.engine.connect_args())
        with self.conn.cursor() as c:
            try:
                c.execute("SELECT CONNECTION_ID(), VERSION()")
            except pymysql.err.OperationalError as e:
                if "connection_id does not exist" in str(e):   # DuckDB behind the MySQL protocol
                    raise RuntimeError("this server runs DuckDB SQL behind the MySQL protocol "
                                       "(a DBTrail port?): use --engine dbtrail") from None
                raise
            self.conn_id, self.server_version = c.fetchone()

    def set_timeout(self, seconds):
        super().set_timeout(seconds)
        try:
            self.execute(self.engine.timeout_statement(seconds), fetch=False)
        except Exception:
            pass  # not supported: the client side watchdog still cancels

    def execute(self, sql, fetch=True):
        with self.conn.cursor() as c:
            c.execute(sql)
            if not fetch or c.description is None:
                return [], []
            cols = [d[0] for d in c.description]
            return cols, list(c.fetchall())

    def cancel(self):
        import pymysql
        k = pymysql.connect(**self.engine.connect_args())
        try:
            with k.cursor() as c:
                c.execute("KILL QUERY %d" % self.conn_id)
        finally:
            k.close()

    def is_disconnect(self, exc):
        import pymysql
        if isinstance(exc, (pymysql.err.InterfaceError,)):
            return True
        code = exc.args[0] if getattr(exc, "args", None) else None
        return code in (2006, 2013, 2055, 0)


class MariaDBEngine(MySQLEngine):
    name = "mariadb"
    dialects = ["mariadb", "mysql", "default"]

    def timeout_statement(self, seconds):
        return "SET SESSION max_statement_time = %g" % seconds


def _cnf_port(path, group="client"):
    """The port in a MySQL option file's [client] group, as PyMySQL reads it."""
    import configparser
    cfg = configparser.RawConfigParser(allow_no_value=True, strict=False)
    try:
        cfg.read(path)
        v = cfg.get(group, "port")
        return int(v.strip("'\"")) if v else None
    except (configparser.Error, ValueError):
        return None


# ================================================================ DBTrail
class DBTrailEngine(MySQLEngine):
    """The MySQL port of a DBTrail daemon: SQL on its Parquet copy.

    The port speaks the MySQL protocol, but the statement runs in DuckDB, so
    the dialect chain tries ``[sql].duckdb`` before the MySQL text. The port
    has no CONNECTION_ID(), max_execution_time or KILL QUERY: it stops a
    statement at its own cap (60 s by default, MySQL error 1317), and a
    client that disconnects ends its statement, which is how cancel works.
    """
    name = "dbtrail"
    dialects = ["dbtrail", "duckdb", "mysql", "default"]
    default_port = 3309
    activity_sql = None   # the copy has no transaction counters
    queries_sql = None
    rows_sql = None
    heartbeat_writable = False

    def connect(self):
        s = DBTrailSession(self)
        s.open()
        return s


class DBTrailSession(MySQLSession):
    def open(self):
        import pymysql
        self.conn = pymysql.connect(**self.engine.connect_args())
        self.conn_id = None
        # SELECT VERSION() returns no row on the port: use the handshake.
        self.server_version = "DBTrail (MySQL protocol %s)" % self.conn.get_server_info()

    def set_timeout(self, seconds):
        Session.set_timeout(self, seconds)   # the port enforces its own cap

    def cancel(self):
        import socket
        sock = getattr(self.conn, "_sock", None)
        if sock is not None:
            sock.shutdown(socket.SHUT_RDWR)


# ============================================================ PostgreSQL
class PostgreSQLEngine(Engine):
    name = "postgresql"
    dialects = ["postgresql", "default"]
    default_port = 5432
    activity_sql = "SELECT SUM(xact_commit + xact_rollback) FROM pg_stat_database"
    rows_sql = ("SELECT SUM(tup_returned + tup_fetched), SUM(tup_inserted), SUM(tup_updated), SUM(tup_deleted) "
                "FROM pg_stat_database")

    def connect(self):
        s = PostgreSQLSession(self)
        s.open()
        return s

    def conninfo(self):
        o = self.opts
        parts = dict(dbname=o.database or "tpcc", connect_timeout=30,
                     application_name="tplytics")
        if o.socket:
            parts["host"] = o.socket
        elif o.host:
            parts["host"] = o.host
        if o.port:
            parts["port"] = o.port
        parts["user"] = o.user or "tpcc"
        if o.password is not None:
            parts["password"] = o.password
        return parts


class PostgreSQLSession(Session):
    def open(self):
        import psycopg
        self.conn = psycopg.connect(autocommit=True, **self.engine.conninfo())
        if self.engine.opts.schema:
            self.execute("SET search_path TO %s" % self.engine.opts.schema, fetch=False)
        cols, rows = self.execute("SELECT pg_backend_pid(), current_setting('server_version')")
        self.conn_id, self.server_version = rows[0]
        self.server_version = "PostgreSQL " + self.server_version

    def set_timeout(self, seconds):
        super().set_timeout(seconds)
        self.execute("SET statement_timeout = %d" % int(seconds * 1000), fetch=False)

    def execute(self, sql, fetch=True):
        with self.conn.cursor() as c:
            c.execute(sql)
            if not fetch or c.description is None:
                return [], []
            cols = [d.name for d in c.description]
            return cols, list(c.fetchall())

    def cancel(self):
        self.conn.cancel()

    def is_timeout_error(self, exc):
        return getattr(exc, "sqlstate", None) == "57014"

    def is_disconnect(self, exc):
        import psycopg
        return isinstance(exc, psycopg.OperationalError) and (self.conn is None or self.conn.closed)


ENGINES = {
    "mysql": MySQLEngine,
    "mariadb": MariaDBEngine,
    "postgresql": PostgreSQLEngine,
    "dbtrail": DBTrailEngine,
}
ALIASES = {"percona": "mysql", "ps": "mysql", "postgres": "postgresql", "pg": "postgresql",
           "pgsql": "postgresql", "bintrail": "dbtrail"}


def get_engine(name, opts):
    key = ALIASES.get(name.lower(), name.lower())
    if key not in ENGINES:
        raise SystemExit("unknown engine %r (choose from %s)" % (name, ", ".join(sorted(ENGINES))))
    return ENGINES[key](opts)
