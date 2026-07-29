#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pid_file="$project_dir/server/jetty.pid"
log_file="$project_dir/server/jetty.log"
port="${JETTY_PORT:-8080}"

find_listener() {
  fuser -n tcp "$port" 2>/dev/null | awk '{print $1}' || true
}

if existing_pid="$(find_listener)"; [ -n "$existing_pid" ]; then
  echo "Port $port is already in use (PID $existing_pid)."
  exit 1
fi

rm -f "$pid_file"
cd "$project_dir"
nohup sbt -batch "runMain server.Server --port=$port" >"$log_file" 2>&1 &
launcher_pid=$!

for _ in {1..30}; do
  if server_pid="$(find_listener)"; [ -n "$server_pid" ]; then
    echo "$server_pid" >"$pid_file"
    echo "Jetty started on http://127.0.0.1:$port/ (PID $server_pid)."
    echo "Log: $log_file"
    exit 0
  fi
  if ! kill -0 "$launcher_pid" 2>/dev/null; then
    echo "Jetty failed to start. See $log_file"
    exit 1
  fi
  sleep 1
done

kill "$launcher_pid" 2>/dev/null || true
echo "Jetty did not start within 30 seconds. See $log_file"
exit 1
