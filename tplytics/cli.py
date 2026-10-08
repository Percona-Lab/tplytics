"""Command line interface."""
import argparse
import datetime as dt
import os
import platform
import socket
import sys

from . import __version__
from . import modes
from . import queries as Q
from . import stats as S
from .engines import ENGINES, get_engine
from .state import DatabaseState

EPILOG = """\
examples:
  tplytics demo --engine mysql --host db1 --user tpcc --password secret
  tplytics serial --engine postgresql --host db1 --iterations 5 --json serial.json
  tplytics concurrent --threads 8 --duration 600 --think 5 --json conc.json --raw conc.csv
  tplytics dashboard --listen 0.0.0.0 --http-port 8080  # live demo page in the browser
  tplytics list --engine postgresql
  tplytics sql Q15 --engine mysql            # print one random variant of a query

Connection settings can also come from environment variables:
  TPLYTICS_ENGINE, TPLYTICS_HOST, TPLYTICS_PORT, TPLYTICS_USER, TPLYTICS_PASSWORD,
  TPLYTICS_DATABASE, TPLYTICS_SOCKET
"""


class Context:
    def __init__(self, args, engine, queries, state, server_version):
        self.args, self.engine, self.queries, self.state = args, engine, queries, state
        self.server_version = server_version
        self.started = dt.datetime.now()

    def report_doc(self, mode, body):
        a = self.args
        doc = {
            "tool": "tplytics", "version": __version__, "mode": mode,
            "started": self.started.isoformat(timespec="seconds"),
            "finished": dt.datetime.now().isoformat(timespec="seconds"),
            "client_host": socket.gethostname(), "python": platform.python_version(),
            "engine": self.engine.name, "server_version": self.server_version,
            "target": self.engine.describe(),
            "database": self.state.as_dict(),
            "settings": {k: v for k, v in vars(a).items() if k not in ("password", "func")},
        }
        doc.update(body)
        return doc


def env(name, default=None):
    return os.environ.get("TPLYTICS_" + name, default)


def add_common(p, need_db=True):
    add_connection(p, need_db)
    add_query_options(p, need_db)


def add_connection(p, need_db=True):
    g = p.add_argument_group("database connection")
    g.add_argument("--engine", "-e", default=env("ENGINE", "mysql"),
                   help="database engine: %s (default mysql)" % ", ".join(sorted(ENGINES)))
    if need_db:
        g.add_argument("--host", "-H", default=env("HOST"), help="server host (default 127.0.0.1 / libpq default)")
        g.add_argument("--port", "-P", default=env("PORT"), help="server port")
        g.add_argument("--socket", "-S", default=env("SOCKET"), help="unix socket (PostgreSQL: socket directory)")
        g.add_argument("--user", "-u", default=env("USER"), help="user (default root for MySQL, tpcc for PostgreSQL)")
        g.add_argument("--password", "-p", default=env("PASSWORD"), help="password")
        g.add_argument("--database", "-d", default=env("DATABASE", "tpcc"), help="database (default tpcc)")
        g.add_argument("--schema", default=None, help="PostgreSQL schema (search_path)")
        g.add_argument("--defaults-file", default=None, help="MySQL option file with connection settings")
        g.add_argument("--init-sql", action="append", default=[], metavar="SQL",
                       help="statement run on every new connection (repeatable)")


def add_query_options(p, need_db=True):
    g = p.add_argument_group("queries")
    g.add_argument("--queries", "-q", default=None, help="comma separated ids to run (e.g. Q01,Q05,7)")
    g.add_argument("--exclude", "-x", default=None, help="comma separated ids to skip")
    g.add_argument("--query-dir", default=None, help="directory with query definitions (default: bundled)")
    g.add_argument("--set-weight", action="append", default=[], metavar="QID=W",
                   help="override the weight of a query for this mode (repeatable, 0 disables)")
    if need_db:
        g = p.add_argument_group("execution")
        g.add_argument("--timeout", "-t", type=float, default=300.0,
                       help="query timeout in seconds; slower executions are killed and reported as timeout (default 300)")
        g.add_argument("--seed", type=int, default=None, help="random seed for reproducible parameter choices")
        g.add_argument("--json", default=None, metavar="FILE", help="write the full results as JSON")


def build_parser():
    ap = argparse.ArgumentParser(
        prog="tplytics", formatter_class=argparse.RawDescriptionHelpFormatter, epilog=EPILOG,
        description="Analytical queries on a HammerDB TPROC-C schema, alone or next to a running HammerDB workload.")
    ap.add_argument("--version", action="version", version="tplytics " + __version__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("demo", help="explain, show and run each query once with its first rows")
    add_common(p)
    p.add_argument("--show-rows", type=int, default=5, help="result rows to show (default 5)")
    p.add_argument("--variants", type=int, default=1, help="parameter variants to run per query (default 1)")
    p.add_argument("--pause", action="store_true", help="wait for Enter after every query")
    p.set_defaults(func=modes.demo, mode=None)

    p = sub.add_parser("serial", help="one connection; every query iterations x serial weight times")
    add_common(p)
    p.add_argument("--iterations", "-n", type=int, default=10,
                   help="base number of executions, multiplied by each query's serial weight (default 10)")
    p.add_argument("--order", choices=["grouped", "interleaved", "shuffled"], default="grouped",
                   help="grouped: all runs of a query back to back; interleaved: round robin; shuffled: random order")
    p.add_argument("--warmup-runs", type=int, default=0, help="uncounted runs of every query before measuring")
    p.add_argument("--raw", default=None, metavar="FILE", help="CSV log of every execution")
    p.add_argument("--report-interval", type=float, default=30, help="progress interval for non-grouped order")
    p.add_argument("--verbose", "-v", action="store_true", help="print every execution")
    p.set_defaults(func=modes.serial, mode="serial")

    p = sub.add_parser("concurrent", help="N connections running weighted random queries for a fixed time")
    add_common(p)
    p.add_argument("--threads", "-c", type=int, default=4, help="concurrent connections (default 4)")
    p.add_argument("--duration", "-D", type=float, default=300, help="measured seconds (default 300)")
    p.add_argument("--warmup", type=float, default=0, help="seconds run before measuring starts (default 0)")
    p.add_argument("--think", type=float, default=0,
                   help="pause after a query of concurrent weight 1, in seconds; a query of weight W "
                        "is followed by think/W, so cheap frequent queries pause less (default 0 = saturate)")
    p.add_argument("--think-jitter", type=float, default=0.5,
                   help="random +/- fraction applied to every pause (default 0.5)")
    p.add_argument("--refresh", type=float, default=60,
                   help="seconds between re-reading the database size and time range (default 60, 0 = never)")
    p.add_argument("--report-interval", type=float, default=10, help="progress line interval (default 10s)")
    p.add_argument("--raw", default=None, metavar="FILE", help="CSV log of every execution")
    p.set_defaults(func=modes.concurrent, mode="concurrent")

    p = sub.add_parser("dashboard", help="web page that runs the queries in a loop (demo mode, live)")
    add_common(p)
    p.add_argument("--listen", default="127.0.0.1",
                   help="address to listen on (default 127.0.0.1; 0.0.0.0 for all interfaces)")
    p.add_argument("--http-port", type=int, default=8080, help="HTTP port of the dashboard (default 8080)")
    p.add_argument("--interval", type=float, default=10.0,
                   help="minimum seconds between two query starts, so results can be read (default 10)")
    p.add_argument("--min-display", type=float, default=5.0,
                   help="minimum seconds a finished query stays the current query (default 5)")
    p.add_argument("--order", choices=["sequential", "weighted"], default="sequential",
                   help="sequential: Q01..Q20 in turn; weighted: random by concurrent weight")
    p.add_argument("--history", type=int, default=10, help="finished queries listed at the bottom (default 10)")
    p.add_argument("--rounds", type=int, default=10,
                   help="completed rounds (one run of every query) listed at the bottom (default 10)")
    p.add_argument("--system-stats", choices=["auto", "off"], default="auto",
                   help="CPU and disk charts from /proc of this host (auto: on Linux)")
    p.add_argument("--show-rows", type=int, default=5, help="result rows kept per query (default 5)")
    p.add_argument("--start-paused", action="store_true", help="wait for Resume in the browser")
    p.add_argument("--refresh", type=float, default=60,
                   help="seconds between re-reading the database size and time range (default 60)")
    p.set_defaults(func=run_dashboard, mode=None)

    p = sub.add_parser("heartbeat", formatter_class=argparse.RawDescriptionHelpFormatter,
                       help="replication lag from a heartbeat row (like pt-heartbeat)",
                       description="End-to-end replication lag. 'update' writes a heartbeat row on the "
                                   "primary every --interval seconds; 'monitor' reads it on a replica and "
                                   "reports how old it is; 'check' reads it once (exit 0 OK, 1 over "
                                   "--max-lag, 2 unknown). Lag compares the monitor's clock with the "
                                   "writer's, so keep both hosts in sync with NTP.",
                       epilog="examples:\n"
                              "  tplytics heartbeat update  --defaults-file /etc/tplytics/primary.cnf\n"
                              "  tplytics heartbeat monitor --defaults-file ~/.my.cnf --label replica \\\n"
                              "                             --status-file /run/tplytics/replica.json\n"
                              "  tplytics heartbeat monitor --engine dbtrail --defaults-file dbtrail.cnf "
                              "--label dbtrail --http-port 8091\n"
                              "  tplytics heartbeat check   --defaults-file ~/.my.cnf --max-lag 10")
    p.add_argument("action", choices=["update", "monitor", "check"])
    add_connection(p)
    g = p.add_argument_group("heartbeat")
    g.add_argument("--table", default="tplytics_heartbeat",
                   help="heartbeat table, created by update when missing (default tplytics_heartbeat)")
    g.add_argument("--id", type=int, default=1,
                   help="heartbeat row; give each writer its own (default 1)")
    g.add_argument("--interval", type=float, default=1.0, help="seconds between writes / reads (default 1)")
    g.add_argument("--report-interval", type=float, default=60,
                   help="seconds between summary lines (default 60)")
    g.add_argument("--count", type=int, default=0, help="stop after this many writes / reads (default 0 = run forever)")
    g.add_argument("--label", default=None, help="monitor: name of the replica in output (default: engine)")
    g.add_argument("--status-file", default=None, metavar="FILE",
                   help="monitor: JSON file rewritten after every read")
    g.add_argument("--http-port", type=int, default=0, help="monitor: serve the status JSON on this port")
    g.add_argument("--listen", default="127.0.0.1", help="monitor: address for --http-port (default 127.0.0.1)")
    g.add_argument("--system-stats", action="store_true",
                   help="monitor: also serve this host's CPU and disk load on /api/system (for tplytics compare)")
    g.add_argument("--perf", action="store_true",
                   help="monitor: also serve detailed OS, unit and database metrics on /api/perf "
                        "(for the compare dashboard's /performance page)")
    g.add_argument("--prometheus-url", default=None, metavar="URL",
                   help="monitor --perf: read database metrics from this Prometheus endpoint (e.g. DBTrail's "
                        "http://127.0.0.1:9090/metrics) instead of the monitored database")
    g.add_argument("--skew", type=float, default=None,
                   help="monitor: read this many seconds after each write tick (default: half the interval)")
    g.add_argument("--max-lag", type=float, default=None,
                   help="check: lag in seconds above which the exit code is 1 (monitor: reported only)")
    g.add_argument("--verbose", "-v", action="store_true", help="monitor: print every read")
    p.set_defaults(func=run_heartbeat, mode=None)

    p = sub.add_parser("compare", formatter_class=argparse.RawDescriptionHelpFormatter,
                       help="side-by-side dashboard: the same queries on several targets (e.g. replica vs DBTrail)",
                       description="Runs every query, with the same parameters, on all targets of --config at "
                                   "the same time and shows them next to each other in the browser, with "
                                   "replication lag, the geometric mean of all executions and host load. "
                                   "See tplytics/compare.py for the config file format.")
    p.add_argument("--config", required=True, metavar="FILE", help="TOML file with [[target]] tables")
    add_query_options(p, need_db=False)
    g = p.add_argument_group("dashboard")
    g.add_argument("--title", default="tplytics compare", help="page title")
    g.add_argument("--listen", default="127.0.0.1", help="address to listen on (default 127.0.0.1)")
    g.add_argument("--http-port", type=int, default=8080, help="HTTP port (default 8080)")
    g.add_argument("--interval", type=float, default=10.0, help="minimum seconds between round starts (default 10)")
    g.add_argument("--min-display", type=float, default=5.0,
                   help="minimum seconds a finished round stays the current one (default 5)")
    g.add_argument("--order", choices=["sequential", "weighted"], default="sequential",
                   help="sequential: Q01, Q02, ... in turn; weighted: random by concurrent weight")
    g.add_argument("--timeout", "-t", type=float, default=120.0, help="query timeout in seconds (default 120)")
    g.add_argument("--seed", type=int, default=None, help="random seed for parameter choices")
    g.add_argument("--history", type=int, default=25,
                   help="finished rounds kept for 'see more' and the SQL pages (default and minimum 25)")
    g.add_argument("--show-rows", type=int, default=5, help="result rows kept per target (default 5)")
    g.add_argument("--start-paused", action="store_true", help="wait for Resume in the browser")
    g.add_argument("--refresh", type=float, default=60,
                   help="seconds between re-reading each target's size and time range (default 60)")
    g.add_argument("--system-stats", choices=["auto", "off"], default="auto",
                   help="CPU and disk load of this host (auto: on Linux)")
    g.add_argument("--history-file", default=None, metavar="FILE",
                   help="JSON-lines file keeping one point per full pass over the queries, so the "
                        "long-term chart survives restarts (default: kept in memory only)")
    g.add_argument("--keep-hours", type=float, default=24, help="hours the long-term chart covers (default 24)")
    p.set_defaults(func=run_compare, mode=None,
                   defaults={a.dest: a.default for a in p._actions if a.dest != "help"})

    p = sub.add_parser("info", help="show what the benchmark discovers about the database")
    add_common(p)
    p.set_defaults(func=cmd_info, mode=None)

    p = sub.add_parser("list", help="list the queries, weights and engine support")
    add_common(p, need_db=False)
    p.set_defaults(func=cmd_list, mode=None)

    p = sub.add_parser("sql", help="print the SQL of queries with freshly generated parameters")
    p.add_argument("ids", nargs="*", help="query ids (default all)")
    add_common(p)
    p.set_defaults(func=cmd_sql, mode=None)
    return ap


def run_heartbeat(args):
    from .heartbeat import heartbeat
    return heartbeat(args)


def run_compare(args):
    from .compare import compare
    return compare(args)


def run_dashboard(ctx):
    from .dashboard import dashboard
    dashboard(ctx)


def split_ids(s):
    return [x for x in (s or "").split(",") if x.strip()] or None


def apply_weights(queries, overrides, mode):
    for o in overrides:
        try:
            qid, w = o.split("=", 1)
            qid, w = Q.normalize_id(qid), float(w)
        except ValueError:
            raise SystemExit("--set-weight expects QID=WEIGHT, got %r" % o)
        hit = [q for q in queries if q.id == qid]
        if not hit:
            raise SystemExit("--set-weight: unknown query %s" % qid)
        for m in ([mode] if mode else ["serial", "concurrent"]):
            hit[0].weight[m] = w


def cmd_list(args):
    engine = get_engine(args.engine, args)
    qs = Q.load(args.query_dir, split_ids(args.queries), split_ids(args.exclude))
    apply_weights(qs, args.set_weight, None)
    ws, wc = sum(q.weight["serial"] for q in qs), sum(q.weight["concurrent"] for q in qs)
    rows = []
    for q in qs:
        support = {e: ("yes" if q.supported(ENGINES[e](args)) else "NO") for e in sorted(ENGINES)}
        rows.append([q.id, q.name, q.complexity, ",".join(q.tables), "%g" % q.weight["serial"],
                     "%g" % q.weight["concurrent"], "%.1f%%" % (100 * q.weight["concurrent"] / wc)]
                    + [support[e] for e in sorted(ENGINES)])
    print(S.table(["id", "name", "cx", "tables", "w.serial", "w.conc", "conc%"] + sorted(ENGINES), rows,
                  align=["<", "<", ">", "<", ">", ">", ">"] + ["^"] * len(ENGINES)))
    print("\ncx = complexity (1 single table, 2 join, 3 multi-join / window functions, 4 heavy)")
    print("serial executions per base iteration: %g" % ws)


def connect(args):
    engine = get_engine(args.engine, args)
    try:
        session = engine.connect()
    except ImportError as e:
        raise SystemExit("missing driver for %s: %s (pip install -r requirements.txt)" % (engine.name, e))
    except Exception as e:
        raise SystemExit("cannot connect to %s: %s" % (engine.describe(), e))
    return engine, session


def prepare(args):
    qs = Q.load(args.query_dir, split_ids(args.queries), split_ids(args.exclude))
    apply_weights(qs, args.set_weight, getattr(args, "mode", None))
    engine, session = connect(args)
    print("tplytics %s  %s (%s)" % (__version__, engine.describe(), session.server_version), flush=True)
    state = DatabaseState(session)
    version = session.server_version
    session.close()
    print("database: " + state.summary(), flush=True)
    return Context(args, engine, qs, state, version)


def cmd_info(args):
    ctx = prepare(args)
    for k, v in ctx.state.as_dict().items():
        print("  %-24s %s" % (k, v))
    supported = [q.id for q in ctx.queries if q.supported(ctx.engine)]
    print("  %-24s %d of %d" % ("supported queries", len(supported), len(ctx.queries)))


def cmd_sql(args):
    import random
    args.queries = ",".join(args.ids) if args.ids else args.queries
    ctx = prepare(args)
    rng = random.Random(args.seed)
    for q in ctx.queries:
        print("-- %s %s" % (q.id, q.name))
        if not q.supported(ctx.engine):
            print("-- not supported by %s\n" % ctx.engine.name)
            continue
        print(q.render(ctx.engine, q.new_params(ctx.state, rng)) + ";\n")


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.func in (cmd_list, cmd_info, cmd_sql, run_heartbeat, run_compare):
        return args.func(args)
    if args.timeout <= 0:
        raise SystemExit("--timeout must be positive")
    ctx = prepare(args)
    try:
        args.func(ctx)
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        return 130
    return 0
