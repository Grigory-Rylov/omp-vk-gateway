#!/bin/sh
# One-shot cutover: hand the manually-started restarter over to systemd.
# Safe to run twice (idempotent).
DIR=/home/grishberg/projects/go/omp-vk-gateway
PIDFILE=$DIR/.cutover.pid
LOG=$DIR/debug/cutover.log

# If systemd is already running the restarter, nothing to do.
if systemctl is-active --quiet omp-vk-gateway-restarter; then
    echo "unit already active, nothing to do" >> "$LOG"
    exit 0
fi

# Find the manually-started restarter (not the one owned by systemd).
OLD=$(ps -eo pid,cmd | awk '/omp-agent-restarter/ && $0 !~ /awk/ {print $1}' | head -1)
if [ -n "$OLD" ]; then
    echo "stopping manual restarter pid=$OLD" >> "$LOG"
    kill -TERM "$OLD" 2>>"$LOG"
    # restarter SIGTERMs the gateway; gateway SIGTERMs agents; allow up to 15s
    for i in $(seq 1 15); do
        if [ "$(ps -o state= -p "$OLD" 2>/dev/null | tr -d ' ')" = "Z" ] || ! ps -p "$OLD" >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    # make sure no orphan gateway remains before the unit takes over
    for i in $(seq 1 10); do
        if ! pgrep -f "^$DIR/omp-agent" >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    pgrep -f "^$DIR/omp-agent" >/dev/null 2>&1 && kill -9 $(pgrep -f "^$DIR/omp-agent") 2>>"$LOG"
fi

# Remove the manual restarter if it lingers as a zombie-free survivor
ps -p "$OLD" >/dev/null 2>&1 && kill -9 "$OLD" 2>>"$LOG"

# Wait a beat so any in-flight state settles, then hand over to systemd.
sleep 2
if ! systemctl is-active --quiet omp-vk-gateway-restarter; then
    echo "starting systemd unit" >> "$LOG"
    echo '1' | sudo -S -k systemctl enable --now omp-vk-gateway-restarter 2>>"$LOG"
else
    echo "unit became active meanwhile" >> "$LOG"
fi
sleep 3
{
    echo "=== cutover finished $(date -u +%FT%TZ) ==="
    systemctl is-active omp-vk-gateway-restarter
    systemctl is-enabled omp-vk-gateway-restarter
    ps -eo pid,ppid,cmd | grep -E 'omp-agent' | grep -v grep
} >> "$LOG" 2>&1
rm -f "$PIDFILE"
