# queuectl

Simple CLI job queue with workers, exponential backoff retries and a Dead Letter Queue (DLQ).

## Setup

Requirements
- Python 3.8+
- No external packages (uses stdlib + sqlite)

Install / run
1. Ensure files are in `queuectl`.
2. Make test script executable:
   chmod +x queuectl/test_flow.sh
3. Run commands with python3:
   /usr/bin/python3 queuectl/queuectl.py <command>
   or use the test script:
   ./test_flow.sh

## Usage examples

Enqueue a job:
  /usr/bin/python3 queuectl.py enqueue '{"id":"job1","command":"echo Hello; sleep 1"}'

Start workers (background processes):
  /usr/bin/python3 queuectl.py worker start --count 3

Stop workers (graceful):
  /usr/bin/python3 queuectl.py worker stop

Show summary of jobs and active workers:
  /usr/bin/python3 queuectl.py status

List jobs (by state):
  /usr/bin/python3 queuectl.py list --state pending
  /usr/bin/python3 queuectl.py list --state failed
  /usr/bin/python3 queuectl.py list --state dead

DLQ:
  /usr/bin/python3 queuectl.py dlq list
  /usr/bin/python3 queuectl.py dlq retry job1

Config (keys stored in DB):
  /usr/bin/python3 queuectl.py config set backoff_base 2
  /usr/bin/python3 queuectl.py config set max_retries 3
  /usr/bin/python3 queuectl.py config get max_retries

Inspect database:
  sqlite3 ./queue.db "SELECT * FROM jobs;"

## Architecture overview

- Persistence
  - SQLite database `queue.db` in project directory.
  - Tables: `jobs` and `config`.

- Job model (core fields)
  - id, command, state, attempts, max_retries, created_at, updated_at, last_error, next_run_at

- Worker model
  - Workers are separate Python processes (multiprocessing.Process).
  - Each worker selects one pending job in a transaction (BEGIN IMMEDIATE), updates state to `processing` (locking), executes command with shell, then updates job state on success/failure.
  - Worker PIDs written to `workers.pid` for stop signaling.

- Retry & backoff
  - On failure: attempts += 1.
  - If attempts > max_retries -> state `dead` (DLQ).
  - Else set state `failed`, compute next_run_at = now + base ** attempts seconds, where `base` is `backoff_base` config (default 2).
  - Jobs returned to `pending` are selected only when next_run_at is NULL or <= now.

## Job lifecycle

pending -> processing -> completed
        -> failed (retryable; scheduled via next_run_at)
        -> dead (moved to DLQ after exhausting retries)

## Assumptions & trade-offs

- Uses shell=True to execute commands for flexibility — do NOT enqueue untrusted commands.
- PID file is a lightweight approach; it may contain stale PIDs if workers die unexpectedly.
- Retry cutoff is currently "attempts > max_retries" (changeable to >= if desired).
- Simple SQLite locking (BEGIN IMMEDIATE + UPDATE) to avoid duplicate processing. Works for modest concurrency.
- No authentication, no priority, no visibility timeout beyond simple locking semantics.
- Logging is printed to the terminal where workers were started. Consider redirecting output to files in production.

## Testing / verification

A basic integration script `test_flow.sh` is provided.

1. Clean DB and run sample flow:
   ./test_flow.sh

2. Manual verification steps
   - Enqueue a successful job and a failing job:
     /usr/bin/python3 queuectl.py enqueue '{"id":"good1","command":"echo ok"}'
     /usr/bin/python3 queuectl.py enqueue '{"id":"bad1","command":"false","max_retries":2}'
   - Start a worker:
     /usr/bin/python3 queuectl.py worker start --count 1
   - Observe output: good1 should complete; bad1 should retry with delays 2s, 4s, ... then move to DLQ.
   - Check DLQ:
     /usr/bin/python3 queuectl.py dlq list
   - Retry DLQ job:
     /usr/bin/python3 queuectl.py dlq retry bad1

3. Persistence check
   - Stop all workers and process, then re-run status/list commands — `queue.db` persists job states.

## To finish / recommended improvements

- Change DLQ cutoff to attempts >= max_retries if preferred.
- Clear `last_error` and `next_run_at` when re-queuing from DLQ.
- Improve PID lifecycle (don't start when workers.pid present; detect running PIDs).
- Add file logging or run workers in foreground for easier debugging.
- Add unit/integration tests for concurrency, retry, DLQ.

---
