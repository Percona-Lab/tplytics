# tplytics: analytical queries on the HammerDB TPC-C schema

tplytics is an analytical (OLAP) benchmark that runs against the schema
[HammerDB](https://www.hammerdb.com/) builds for its TPROC-C (TPC-C derived)
workload. It is meant to run **at the same time as HammerDB**, so you can see
how an OLTP database copes when reporting queries hit the tables it is
writing to, and how much each workload slows the other down.

* 20 analytical queries with a business meaning, from single-table scans to
  multi-join, window-function and self-join queries.
* Every query has a **weight** for serial mode and one for concurrent mode, so
  cheap queries can run more often than expensive ones.
* Parameters are drawn **fresh for every execution** from the database itself:
  the number of warehouses, the item range and the time range of the stored
  transactions. The same query never runs with the same parameters twice in a
  row, and the benchmark works on any database size, whether HammerDB has been
  running for a minute or for a month.
* Failed queries (syntax not supported, other errors) and **timeouts** (300 s
  by default) are recorded and reported. They never stop the run.
* **Engine variants** in the style of ClickBench: a query can carry different
  SQL for different engines or be marked as not supported by an engine.
* Three modes: **demo**, **serial** and **concurrent**, plus a live **dashboard**
  that runs the queries in a loop in the browser.

Supported engines: MySQL / Percona Server (`mysql`), MariaDB (`mariadb`) and
PostgreSQL (`postgresql`). Other engines can be added easily (see
[Adding an engine](#adding-an-engine)).

## Quick start

```bash
git clone https://github.com/Percona-Lab/tplytics.git
cd tplytics
pip install -r requirements.txt      # PyMySQL, psycopg 3 (Python 3.8+; tomli on < 3.11)

./tplytics.py list                                         # the 20 queries and their weights
./tplytics.py info  --engine mysql --host db1 --user root -p secret
./tplytics.py demo  --engine mysql --host db1 --user root -p secret
./tplytics.py serial --engine postgresql --host db1 --user tpcc -p tpcc --json serial.json
./tplytics.py concurrent --engine mysql --host db1 -u root -p secret \
              --threads 8 --duration 600 --think 10 --json conc.json --raw conc.csv
```

On Ubuntu/Debian the drivers are also packaged: `apt install python3-pymysql python3-psycopg`.

Connection defaults follow HammerDB: database `tpcc`, user `root` for MySQL and
`tpcc` for PostgreSQL. `--defaults-file ~/.my.cnf` reads MySQL credentials from
an option file. `--socket` connects through a Unix socket (for PostgreSQL, give
the socket directory). Any connection option can also be set as an environment
variable: `TPLYTICS_ENGINE`, `TPLYTICS_HOST`, `TPLYTICS_PORT`, `TPLYTICS_USER`,
`TPLYTICS_PASSWORD`, `TPLYTICS_DATABASE` or `TPLYTICS_SOCKET`.

## Modes

### demo

For every query, demo mode prints what the query answers and how it works, the
generated parameters, and the full SQL statement. It then runs the statement and
shows the execution time, the number of rows and the first 5 rows of the result.
A summary table follows at the end.

```
./tplytics.py demo -q Q09,Q20 --pause          # selected queries, wait for Enter between them
./tplytics.py demo --variants 3 --show-rows 10 # three parameter variants per query, 10 rows each
```

```
====================================================================================================
Q19  Stock-out risk   [complexity 3, weight serial 1 / concurrent 2, tables: district, order_line, stock, item]
----------------------------------------------------------------------------------------------------
Which items will run out first? Measures recent demand per supplying
warehouse and item from the latest orders of a slice of warehouses, ...

Parameters: wh_lo=24, wh_hi=32, wh_count=9, last=1000, cover=3, top=20

WITH demand AS ( ... );

>>> OK: 7.39s, 20 rows
w_id   i_id  i_name                   i_price  on_hand  recent_demand  order_lines  cover_ratio  demand_value
----  -----  -----------------------  -------  -------  -------------  -----------  -----------  ------------
  24  11707  edrxY3oaQRmzbMuW           16.86       11             40            8         0.28        674.40
  31  11710  MlY0fVmiHWrx5WFHOPf18N     35.91       15             50           10         0.30       1795.50
...
```

### serial

Serial mode uses one connection. Each query runs `iterations × serial weight`
times (10 iterations by default), each time with new parameters. For every query
the report shows the runs, successes, errors, timeouts and success ratio, the
min / avg / p50 / p95 / p99 / max / stddev of the execution time, the average
number of rows returned, and the response time distribution. The run total, the
sum of the per-query averages and their geometric mean summarise the result.

```
./tplytics.py serial -n 10                     # default: 10 x weight runs of every query
./tplytics.py serial -n 3 --order shuffled     # random order instead of query by query
./tplytics.py serial --warmup-runs 1 -v        # one uncounted run per query; print every execution
```

`--order grouped` (the default) runs all executions of a query back to back.
`interleaved` cycles through the queries round robin, and `shuffled` runs the
executions in random order, which is closer to how caches behave in practice.

### concurrent

Concurrent mode opens `--threads` connections. Each one repeatedly picks a random
query, with the probability of each query proportional to its concurrent
weight, and runs it with fresh parameters until `--duration` seconds of
measurement have passed. An optional `--warmup` runs before measuring starts.
The report has the same per-query statistics plus each query's share of the
executions and its QPS, the response time distribution, the overall and
successful QPS, and a progress line every `--report-interval` seconds. These
progress lines also go into the JSON file as a timeline.

**Think time.** `--think T` makes each thread sleep after every query for
`T / weight` seconds (with ±50 % jitter, set by `--think-jitter`). A cheap
query of weight 10 is followed by a short pause and an expensive query of
weight 1 by a long one. The load can therefore stay below saturation with
realistic concurrency (many sessions, most of them idle at any moment), and
the expensive queries still don't dominate. `--think 0` (the default) runs
flat out.

While the benchmark runs, the database size and time range are read again
every `--refresh` seconds (60 by default). New HammerDB orders are therefore
included in the time windows. Statements still running when the time is up are
cancelled and left out of the results.

```
./tplytics.py concurrent --threads 16 --duration 1800 --warmup 60 --think 20
```

### dashboard (live demo in the browser)

The dashboard runs one query at a time in an endless loop, like demo mode, and
serves a live web page:

* **System load** at the top: CPU busy and I/O wait, disk throughput (read and
  write MB/s), disk IOPS and database transactions/s. Each tile has a sparkline
  of the last 3 minutes, with a hover tooltip. CPU and disk come from `/proc`
  of the machine the dashboard runs on, so run it on the database host to see
  them. Transactions/s comes from the engine (`Com_commit + Com_rollback` on
  MySQL, `pg_stat_database` on PostgreSQL) and includes HammerDB.
* **Current query**: the query's description, tables and parameters, and its
  state. While it is **running**, the page shows the SQL and a live timer. Once
  it finishes it shows **waiting**, with its response time, rows, status, the
  first 5 rows (or the error) and a countdown to the next query.
* **Recent queries** at the bottom: the last `--history` (10) finished queries,
  newest first. When a new query starts, the finished one slides in at the top
  and the oldest drops off the bottom. Click a row for its description,
  parameters, result rows and SQL.

At most one query starts every `--interval` seconds (5 by default), and every
result stays the current query for at least `--min-display` seconds, so people
have time to read it. **Pause** stops the loop after the current query, and
**Resume** continues it. `--order weighted` picks queries at random by their
concurrent weight instead of going through Q01..Q20 in turn.

```
./tplytics.py dashboard --defaults-file ~/.my.cnf                         # http://127.0.0.1:8080/
./tplytics.py dashboard --listen 0.0.0.0 --http-port 8080 --interval 8 --timeout 120
```

The page only shows what the query files and the engine layer provide. Adding
a query or an engine variant (see below) needs no change to the dashboard.
Engines can supply a transactions counter by setting `activity_sql` in
`engines.py`. The dashboard has no authentication. It listens on 127.0.0.1
unless you pass `--listen`, so put it behind a firewall or an SSH tunnel when
the database is not a test system.

### Other commands

| command | what it does |
|---|---|
| `list` | queries, complexity, tables, weights, concurrent probability, and support per engine |
| `info` | what the benchmark discovers about the database (size, time range) |
| `sql [ids]` | print queries with freshly generated parameters, without running them |

## Running next to HammerDB

tplytics only reads data, and the only thing it needs is a HammerDB TPROC-C
schema, so you can start it on any machine while HammerDB is running. For a
single command, `scripts/with-hammerdb.sh` starts a HammerDB timed run with the
settings in `scripts/hammerdb/*.tcl`, waits for the rampup to finish, and then
runs tplytics in concurrent mode for the rest of the HammerDB run. The logs of
both tools go into `results/<timestamp>/`.

```bash
HDB_DB=mysql HDB_USER=root HDB_PASS=secret HDB_VU=16 HDB_DURATION=30 \
  scripts/with-hammerdb.sh /opt/HammerDB-6.0 --engine mysql -u root -p secret --threads 4 --think 10
```

To measure the effect, run HammerDB once on its own and once with tplytics, and
compare NOPM. Then compare the tplytics latencies with those of a run on an
idle database.

Use `HammerDB … allwarehouse true` (the default in the helper scripts) or
enough virtual users that new orders reach every warehouse. Otherwise many
time windows will cover warehouses that HammerDB never touches.

## Queries

| id | name | cx | tables | serial w | conc w | question answered |
|---|---|---|---|---|---|---|
| Q01 | Warehouse leaderboard | 1 | district | 3 | 10 | Which warehouses take the most orders / payments? |
| Q02 | Catalog price bands | 1 | item | 3 | 10 | How is the catalog priced; share of brand items per band |
| Q03 | Stock health by warehouse | 1 | stock | 3 | 8 | Which warehouses have the most SKUs below a reorder point? |
| Q04 | Customer credit profile | 1 | customer | 3 | 8 | Good vs bad credit customers: discount, balances |
| Q05 | Payment activity over time | 1 | history | 2 | 4 | Payments per time bucket (full scan of history) |
| Q06 | Delivery backlog | 1 | new_order | 3 | 10 | Undelivered orders per warehouse |
| Q07 | Order line fulfilment status | 1 | order_line | 2 | 4 | Delivered vs pending value, remote supply share |
| Q08 | Sales trend | 2 | orders, order_line | 2 | 5 | Orders, revenue and order size per time bucket |
| Q09 | Best sellers | 2 | district, order_line, item | 2 | 5 | Top items in the latest orders |
| Q10 | Carrier delivery performance | 2 | orders, order_line | 2 | 4 | Delivery delay per carrier |
| Q11 | Payments by customer region | 2 | history, customer | 1 | 2 | Which customer states pay the most (whole database) |
| Q12 | Cross-warehouse supply routes | 2 | district, order_line, warehouse | 2 | 4 | Busiest home → supplying warehouse routes |
| Q13 | Brand vs generic sales | 2 | district, order_line, item, stock | 2 | 4 | Revenue split brand / generic (TPC-C ORIGINAL rule) |
| Q14 | Customer value segments | 3 | orders, order_line, customer | 1 | 2 | Customers per spend segment and credit rating |
| Q15 | Top customers per district | 3 | orders, order_line, customer | 1 | 2 | Top-K spenders per district (ROW_NUMBER) |
| Q16 | Customer churn | 3 | orders, customer | 1 | 2 | Who ordered in the first half of a window but not the second (NOT EXISTS) |
| Q17 | Order value deciles | 3 | orders, order_line | 1 | 2 | Order value distribution (NTILE) |
| Q18 | District benchmarking | 3 | orders, order_line | 1 | 2 | Districts deviating most from their warehouse average |
| Q19 | Stock-out risk | 3 | district, order_line, stock, item | 1 | 2 | Items whose stock covers less than recent demand |
| Q20 | Market basket analysis | 4 | district, order_line, item | 1 | 1 | Item pairs bought together: support, confidence, lift |

Complexity (cx): 1 = single table, 2 = join, 3 = CTEs / window functions /
correlated subqueries over joins, 4 = heavy self-join.

### How parameters adapt to the database

At start-up, and again during concurrent runs, tplytics reads the warehouse,
district, item and customer counts and the first and last order of a sample
of districts. It only uses primary-key lookups, so this is cheap even on a
very large database. Each query declares its parameters, and every execution
draws them from that state:

* **warehouse slices** are a random contiguous range covering a fraction of all
  warehouses (e.g. 5–25 %), with a minimum and, for expensive queries, a
  maximum. The work per query therefore grows with the database but stays
  bounded.
* **time windows** are a fraction of the time range actually stored. Half of
  them end at the newest order ("recent", like a dashboard) and the other half
  fall anywhere in the stored range. Their bucket size (minute, 5 minutes,
  hour, …) is chosen to give about 24 buckets.
* **"latest N orders per district"** is based on `d_next_o_id`, so it always
  looks at current activity.
* top-N, thresholds, sort columns and other choices are drawn from lists.

Two queries, Q05 and Q11, deliberately scan the whole `history` table, which has
no index on its date. Their cost grows with the size of the database and with
the length of the HammerDB run.

HammerDB specifics the queries respect: the loader writes `ol_amount = 0` for
the orders delivered at load time, and all remote supply lines come from the
benchmark run. HammerDB's MySQL stored procedures do not maintain
`c_ytd_payment`, `c_payment_cnt`, `c_delivery_cnt`, `s_ytd`, `s_order_cnt` or
`s_remote_cnt`, so demand and payments are taken from `order_line` and
`history` instead. On a freshly loaded database that HammerDB has not run
against yet, some queries legitimately return no rows.

## Timeouts and errors

`--timeout` (300 s by default) is enforced twice:

1. The server is asked to stop the statement: `max_execution_time` on MySQL,
   `max_statement_time` on MariaDB and `statement_timeout` on PostgreSQL.
2. A client-side watchdog cancels the statement 2 s later if it is still
   running (`KILL QUERY <id>` on a separate connection for MySQL, a cancel
   request for PostgreSQL). This also covers engines or statements the server
   timeout does not apply to.

Such an execution is reported as **timeout**. Any other database error is
reported as **error** and shown in the "Errors and timeouts" part of the
report. A broken connection is re-opened, and the run continues in all cases.

## Output

* Human readable report on stdout.
* `--json FILE`: everything in the report plus the settings, the server version,
  the discovered database state, a per-query response time distribution, error
  samples and (in concurrent mode) the timeline.
* `--raw FILE` (serial and concurrent): CSV with one row per execution, giving
  the query, thread, start time, elapsed time, status, rows, error and
  parameters.

## Query files and engine variants

Every query is a TOML file in [`tplytics/queries/`](tplytics/queries):

```toml
id = "Q10"
name = "Carrier delivery performance"
complexity = 2
tables = ["orders", "order_line"]
description = """What the query answers and how it works."""
weight = { serial = 2, concurrent = 4 }
unsupported = []                      # engines that cannot run it at all

[params]
wh  = { type = "warehouse_range", fraction = [0.05, 0.25], min = 1, max = 50 }
win = { type = "time_window", fraction = [0.05, 0.5], anchor = "mixed" }

[sql]
default    = """ ...portable SQL... """          # used when no engine-specific text exists
mysql      = """ ... TIMESTAMPDIFF(SECOND, a, b) ... """
postgresql = """ ... EXTRACT(EPOCH FROM (b - a)) ... """
# mariadb  = false                    # false (or "UNSUPPORTED") marks the query as not supported
```

The SQL for an engine is looked up along the engine's dialect chain. For
example, `mariadb` tries `[sql].mariadb`, then `[sql].mysql`, then
`[sql].default`. Unsupported queries are listed and skipped in every mode.
Placeholders such as `{wh_lo}` are replaced with literals of the right type for
the engine (timestamps become `TIMESTAMP '…'`).

Parameter types:

| type | placeholders | options |
|---|---|---|
| `warehouse_range` | `{x_lo}` `{x_hi}` `{x_count}` | `fraction = [lo, hi]`, `min`, `max` |
| `warehouse` | `{x}` | |
| `district` | `{x}` | |
| `item_range` | `{x_lo}` `{x_hi}` `{x_count}` | `fraction`, `min`, `max` |
| `time_window` | `{x_start}` `{x_mid}` `{x_end}` `{x_seconds}` `{x_bucket}` | `fraction`, `anchor` = recent / any / mixed, `min_seconds`, `buckets` |
| `int` | `{x}` | `range = [lo, hi]` |
| `choice` | `{x}` | `values = [...]`; `raw = true` inserts an identifier unquoted |

Use `--query-dir` to run your own set of query files. To change weights without
editing the files, use `--set-weight Q20=0 --set-weight Q01=20`.

## Adding an engine

Engines live in `tplytics/engines.py`. An engine that speaks the MySQL or
PostgreSQL protocol is usually a small subclass that sets its name, its dialect
chain and its timeout statement:

```python
class TiDBEngine(MySQLEngine):
    name = "tidb"
    dialects = ["tidb", "mysql", "default"]

ENGINES["tidb"] = TiDBEngine
```

Then add `[sql].tidb` variants, or `unsupported = ["tidb"]`, wherever the engine
needs them. A new protocol needs a `Session` that implements `open`,
`execute`, `cancel` and `set_timeout`.

## Validation run

Tested on a 4 GB / 2 vCPU Linode (Ubuntu 24.04), with HammerDB 6.0 running on the
same machine:

| | Percona Server 9.7, 40 warehouses | PostgreSQL 16, 20 warehouses |
|---|---|---|
| demo, all 20 queries | all OK, 10 ms – 50 s (Q20 before its slice was capped) | all OK, 5 ms – 2.7 s |
| serial `-n 3` with HammerDB 4 VU | 111/111 OK in 227 s, geomean of averages 0.84 s | |
| concurrent 6 threads, saturating, with HammerDB 4 VU | 1.75 QPS, p50 0.94 s, p95 13.7 s, 100 % OK | |
| concurrent 8 threads, `--think 20 --timeout 10` | 92.7 % OK, 16 timeouts (Q09, Q12, Q13, Q19, Q20) | |
| HammerDB 4 VU alone | | 42,662 NOPM |
| HammerDB 4 VU + tplytics 4 threads `--think 5` | | **21,311 NOPM (−50 %)**, tplytics 1.23 QPS, 565/565 OK |

## Tests

```bash
python3 -m unittest discover tests          # offline: all queries render for all engines
./tplytics.py serial --query-dir tests/fixtures/queries --timeout 5 -n 2 -v
                                             # live: error, timeout, unsupported and OK cases
```

## License

Apache License 2.0
