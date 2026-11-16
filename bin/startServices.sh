#!/usr/bin/env bash
# Starts the Jetty server with the DeDup2 JSON services (server.Services).
#
# Environment variables:
#   SERVICES_PORT     HTTP port (default 7071)
#   SERVICES_TIMEOUT  seconds to wait for the server to start (default 120)
#   JAVA_HOME_25      Java 25 installation directory
set -euo pipefail

if [ -z "${JAVA_HOME_25:-}" ]; then
  JAVA_HOME_25="/home/users/operacao/.cache/coursier/arc/https/github.com/graalvm/graalvm-ce-builds/releases/download/jdk-25.0.1/graalvm-community-jdk-25.0.1_linux-x64_bin.tar.gz/graalvm-community-openjdk-25.0.1+8.1"
fi
export JAVA_HOME=$JAVA_HOME_25
export PATH=$JAVA_HOME/bin:$PATH

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pid_file="$project_dir/server/services.pid"
log_file="$project_dir/server/services.log"
port="${SERVICES_PORT:-7071}"
timeout="${SERVICES_TIMEOUT:-120}"

mkdir -p "$(dirname "$pid_file")"

find_listener() {
  fuser -n tcp "$port" 2>/dev/null | awk '{print $1}' || true
}

if existing_pid="$(find_listener)"; [ -n "$existing_pid" ]; then
  echo "Port $port is already in use (PID $existing_pid)."
  exit 1
fi

rm -f "$pid_file"
# The services resolve conf/ and indexes/ relative to the project directory.
cd "$project_dir"
nohup sbt -batch "runMain server.Services --port=$port" >"$log_file" 2>&1 &
launcher_pid=$!

for _ in $(seq 1 "$timeout"); do
  if server_pid="$(find_listener)"; [ -n "$server_pid" ]; then
    echo "$server_pid" >"$pid_file"
    echo "Services started on http://127.0.0.1:$port/DeDup2/services/ (PID $server_pid)."
    echo "Log: $log_file"
    exit 0
  fi
  if ! kill -0 "$launcher_pid" 2>/dev/null; then
    echo "Services failed to start. See $log_file"
    exit 1
  fi
  sleep 1
done

kill "$launcher_pid" 2>/dev/null || true
echo "Services did not start within $timeout seconds. See $log_file"
exit 1
