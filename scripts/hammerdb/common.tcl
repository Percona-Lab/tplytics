# Shared HammerDB CLI settings for the tplytics helper scripts.
# Everything comes from environment variables so one script works for
# MySQL-compatible databases and PostgreSQL:
#   HDB_DB          mysql | pg                  (default mysql)
#   HDB_HOST        server host                 (default localhost)
#   HDB_PORT        server port                 (default 3306 / 5432)
#   HDB_SOCKET      MySQL socket                (default /var/run/mysqld/mysqld.sock, "null" for TCP)
#   HDB_USER        benchmark user              (default root / tpcc)
#   HDB_PASS        benchmark password
#   HDB_DBASE       database                    (default tpcc)
#   HDB_SUPERUSER / HDB_SUPERPASS   PostgreSQL superuser used by the build (default postgres)
#   HDB_WAREHOUSES  warehouses to build         (default 10)
#   HDB_VU          virtual users               (default 4)
#   HDB_RAMPUP      rampup minutes              (default 1)
#   HDB_DURATION    timed run minutes           (default 10)
#   HDB_ALLWAREHOUSE  use all warehouses        (default true, so analytics see activity everywhere)

proc env { name {default ""} } {
    if { [info exists ::env($name)] && $::env($name) ne "" } { return $::env($name) }
    return $default
}

set db [env HDB_DB mysql]
dbset db $db
dbset bm TPC-C

if { $db eq "mysql" } {
    diset connection mysql_host   [env HDB_HOST localhost]
    diset connection mysql_port   [env HDB_PORT 3306]
    diset connection mysql_socket [env HDB_SOCKET /var/run/mysqld/mysqld.sock]
    diset tpcc mysql_user         [env HDB_USER root]
    diset tpcc mysql_pass         [env HDB_PASS]
    diset tpcc mysql_dbase        [env HDB_DBASE tpcc]
    set P mysql
} else {
    diset connection pg_host      [env HDB_HOST localhost]
    diset connection pg_port      [env HDB_PORT 5432]
    diset tpcc pg_superuser       [env HDB_SUPERUSER postgres]
    diset tpcc pg_superuserpass   [env HDB_SUPERPASS postgres]
    diset tpcc pg_defaultdbase    postgres
    diset tpcc pg_user            [env HDB_USER tpcc]
    diset tpcc pg_pass            [env HDB_PASS tpcc]
    diset tpcc pg_dbase           [env HDB_DBASE tpcc]
    set P pg
}
