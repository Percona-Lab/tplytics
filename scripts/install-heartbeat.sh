#!/usr/bin/env bash
# Install tplytics heartbeat as the systemd service tplytics-heartbeat@NAME.
#
# Usage: scripts/install-heartbeat.sh NAME update|monitor [tplytics heartbeat options ...]
#
#   scripts/install-heartbeat.sh primary update --defaults-file /etc/tplytics/primary.cnf
#   scripts/install-heartbeat.sh replica monitor --defaults-file /root/.my.cnf \
#       --socket /var/run/mysqld/mysqld.sock --label replica
#
# Copies this checkout to /opt/tplytics, writes /etc/tplytics/heartbeat-NAME.env
# and (re)starts the service. A monitor writes /run/tplytics/NAME.json unless
# --status-file is given. Follow it with: journalctl -fu tplytics-heartbeat@NAME
# Remove with: systemctl disable --now tplytics-heartbeat@NAME
set -euo pipefail
[[ $# -ge 2 ]] || { sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }
NAME=$1; ACTION=$2; shift 2
[[ $NAME =~ ^[A-Za-z0-9_-]+$ ]] || { echo "NAME must be letters, digits, _ or -" >&2; exit 1; }
[[ $ACTION == update || $ACTION == monitor ]] || { echo "action must be update or monitor" >&2; exit 1; }
for a in "$@"; do [[ $a != *" "* ]] || { echo "arguments cannot contain spaces: '$a'" >&2; exit 1; }; done
[[ $(id -u) == 0 ]] || { echo "run as root" >&2; exit 1; }
ROOT=$(cd "$(dirname "$0")/.." && pwd)

python3 -c "import pymysql" 2>/dev/null || python3 -c "import psycopg" 2>/dev/null \
    || echo "warning: neither PyMySQL nor psycopg is installed (apt install python3-pymysql python3-psycopg)" >&2
if [[ $ROOT != /opt/tplytics ]]; then
    rm -rf /opt/tplytics.new
    mkdir -p /opt/tplytics.new
    cp -a "$ROOT"/tplytics.py "$ROOT"/tplytics /opt/tplytics.new/
    rm -rf /opt/tplytics && mv /opt/tplytics.new /opt/tplytics
fi
install -m 0644 "$ROOT/scripts/systemd/tplytics-heartbeat@.service" /etc/systemd/system/

ARGS=("$ACTION" "$@")
if [[ $ACTION == monitor ]]; then
    [[ " $* " == *" --status-file "* ]] || ARGS+=(--status-file "/run/tplytics/$NAME.json")
    [[ " $* " == *" --label "* ]] || ARGS+=(--label "$NAME")
fi
install -d -m 0755 /etc/tplytics
umask 077
echo "HEARTBEAT_ARGS=${ARGS[*]}" > "/etc/tplytics/heartbeat-$NAME.env"
systemctl daemon-reload
systemctl enable -q "tplytics-heartbeat@$NAME"
systemctl restart "tplytics-heartbeat@$NAME"
sleep 3
systemctl --no-pager --lines 5 status "tplytics-heartbeat@$NAME" || true
