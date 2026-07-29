#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pid_file="$project_dir/server/jetty.pid"

if [ ! -f "$pid_file" ]; then
  echo "Jetty is not running."
  exit 0
fi

pid="$(cat "$pid_file")"
process_args="$(ps -p "$pid" -o args= 2>/dev/null || true)"
if [[ "$process_args" != *"server.Server"* ]]; then
  rm -f "$pid_file"
  echo "Jetty is not running; removed stale PID file."
  exit 0
fi

kill "$pid"
for _ in {1..20}; do
  kill -0 "$pid" 2>/dev/null || break
  sleep 1
done

if kill -0 "$pid" 2>/dev/null; then
  kill -KILL "$pid"
fi

rm -f "$pid_file"
echo "Jetty stopped."
