#!/usr/bin/env bash
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
PY="$(command -v python3 || command -v python)"
if [ -z "$PY" ]; then
  echo "python3 not found. Install Python 3 or adjust PY variable." >&2
  exit 1
fi

echo "Cleaning DB..."
rm -f "$DIR/queue.db" "$DIR/workers.pid"

echo "Enqueue jobs: one good, one failing"
"$PY" "$DIR/queuectl.py" enqueue '{"id":"good1","command":"echo ok"}'
"$PY" "$DIR/queuectl.py" enqueue '{"id":"bad1","command":"false","max_retries":2}'

echo "Start 2 workers"
"$PY" "$DIR/queuectl.py" worker start --count 2
sleep 1

echo "Wait for processing (8s)"
sleep 8

echo "Status and DLQ"
"$PY" "$DIR/queuectl.py" status
"$PY" "$DIR/queuectl.py" dlq list

echo "Stop workers"
"$PY" "$DIR/queuectl.py" worker stop
echo "done"
