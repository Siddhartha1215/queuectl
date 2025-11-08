import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from multiprocessing import Process

DB_PATH = os.path.join(os.path.dirname(__file__), "queue.db")
PID_FILE = os.path.join(os.path.dirname(__file__), "workers.pid")
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE = 2

# --- DB helpers ---
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      command TEXT NOT NULL,
      state TEXT NOT NULL,
      attempts INTEGER NOT NULL,
      max_retries INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      last_error TEXT,
      next_run_at TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS config (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )""")
    # defaults
    cur.execute("INSERT OR IGNORE INTO config(key,value) VALUES(?,?)", ("backoff_base", str(DEFAULT_BACKOFF_BASE)))
    cur.execute("INSERT OR IGNORE INTO config(key,value) VALUES(?,?)", ("max_retries", str(DEFAULT_MAX_RETRIES)))
    conn.commit()
    conn.close()

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def enqueue_job(job_json):
    try:
        j = json.loads(job_json)
    except Exception as e:
        print("invalid json:", e, file=sys.stderr)
        return
    req = {
        "id": j.get("id"),
        "command": j.get("command"),
        "state": "pending",
        "attempts": int(j.get("attempts", 0)),
        "max_retries": int(j.get("max_retries", get_config_int("max_retries") or DEFAULT_MAX_RETRIES)),
        "created_at": j.get("created_at", now_iso()),
        "updated_at": j.get("updated_at", now_iso()),
    }
    if not req["id"] or not req["command"]:
        print("job must include id and command", file=sys.stderr)
        return
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""INSERT INTO jobs(id,command,state,attempts,max_retries,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?)""",
                    (req["id"], req["command"], req["state"], req["attempts"], req["max_retries"], req["created_at"], req["updated_at"]))
        conn.commit()
        print(f"enqueued {req['id']}")
    except sqlite3.IntegrityError:
        print("job id already exists", file=sys.stderr)
    finally:
        conn.close()

def list_jobs(state=None):
    conn = get_conn()
    cur = conn.cursor()
    if state:
        cur.execute("SELECT * FROM jobs WHERE state=? ORDER BY created_at", (state,))
    else:
        cur.execute("SELECT * FROM jobs ORDER BY created_at")
    rows = cur.fetchall()
    conn.close()
    for r in rows:
        print(dict(r))

def get_status():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state")
    rows = cur.fetchall()
    conn.close()
    print("jobs summary:")
    for r in rows:
        print(f"  {r['state']}: {r['cnt']}")
    pids = read_pids()
    print("active workers:", len(pids))

def dlq_list():
    list_jobs("dead")

def dlq_retry(job_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE id=? AND state='dead'", (job_id,))
    row = cur.fetchone()
    if not row:
        print("no such dead job")
        conn.close()
        return
    cur.execute("UPDATE jobs SET state='pending', attempts=0, updated_at=? WHERE id=?", (now_iso(), job_id))
    conn.commit()
    conn.close()
    print("requeued", job_id)

def get_config_int(key):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT value FROM config WHERE key=?", (key,))
    r = cur.fetchone()
    conn.close()
    try:
        return int(r["value"]) if r else None
    except Exception:
        return None

def set_config(key, value):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO config(key,value) VALUES(?,?)", (key, str(value)))
    conn.commit()
    conn.close()
    print("set", key, value)

# --- Worker logic ---
stop_event = threading.Event()

def run_worker_loop(worker_id):
    conn = get_conn()
    cur = conn.cursor()
    base = get_config_int("backoff_base") or DEFAULT_BACKOFF_BASE
    while not stop_event.is_set():
        try:
            # Begin transaction to select and lock a job
            cur.execute("BEGIN IMMEDIATE")
            now = datetime.now(timezone.utc).isoformat()
            cur.execute("""
              SELECT * FROM jobs
              WHERE state='pending' AND (next_run_at IS NULL OR next_run_at<=?)
              ORDER BY created_at LIMIT 1
            """, (now,))
            job = cur.fetchone()
            if not job:
                conn.commit()
                # nothing to do
                time.sleep(1)
                continue
            # lock it by updating state to processing only if still pending
            cur.execute("UPDATE jobs SET state='processing', updated_at=? WHERE id=? AND state='pending'", (now, job["id"]))
            if cur.rowcount == 0:
                conn.commit()
                continue
            conn.commit()
            # execute
            print(f"[worker {worker_id}] running {job['id']} -> {job['command']}")
            proc = subprocess.run(job["command"], shell=True)
            rc = proc.returncode
            updated_at = now_iso()
            if rc == 0:
                cur.execute("UPDATE jobs SET state='completed', updated_at=? WHERE id=?", (updated_at, job["id"]))
                conn.commit()
                print(f"[worker {worker_id}] completed {job['id']}")
            else:
                attempts = int(job["attempts"] or 0) + 1
                if attempts > int(job["max_retries"]):
                    cur.execute("UPDATE jobs SET state='dead', attempts=?, last_error=?, updated_at=? WHERE id=?",
                                (attempts, f"exit:{rc}", updated_at, job["id"]))
                    conn.commit()
                    print(f"[worker {worker_id}] moved to DLQ {job['id']}")
                else:
                    delay = (base ** attempts)
                    next_run = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat()
                    cur.execute("""UPDATE jobs SET state='failed', attempts=?, last_error=?, next_run_at=?, updated_at=? WHERE id=?""",
                                (attempts, f"exit:{rc}", next_run, updated_at, job["id"]))
                    conn.commit()
                    print(f"[worker {worker_id}] failed {job['id']} attempts={attempts} retrying after {delay}s")
        except sqlite3.OperationalError as e:
            # DB locked or other transient error
            try:
                conn.rollback()
            except Exception:
                pass
            time.sleep(0.5)
        except Exception as e:
            print("worker error:", e)
            time.sleep(1)
    conn.close()
    print(f"[worker {worker_id}] exiting gracefully")

def worker_process_main(worker_id):
    # each worker process must have its own stop_event; set handler for SIGTERM
    def handle_sigterm(signum, frame):
        print(f"worker {worker_id} got term")
        stop_event.set()
    signal.signal(signal.SIGTERM, handle_sigterm)
    run_worker_loop(worker_id)

def start_workers(count):
    procs = []
    for i in range(count):
        p = Process(target=worker_process_main, args=(i+1,))
        p.start()
        procs.append(p.pid)
    write_pids(procs)
    print("started workers:", procs)
    # processes left running in background

def stop_workers():
    pids = read_pids()
    if not pids:
        print("no workers running")
        return
    import signal as _sig
    for pid in pids:
        try:
            os.kill(pid, _sig.SIGTERM)
            print("signaled", pid)
        except ProcessLookupError:
            print("no such pid", pid)
        except PermissionError:
            print("no permission to signal pid", pid)
    # remove pid file
    try:
        os.remove(PID_FILE)
    except OSError:
        pass

def write_pids(pids):
    with open(PID_FILE, "w") as f:
        for p in pids:
            f.write(str(p) + "\n")

def read_pids():
    if not os.path.exists(PID_FILE):
        return []
    with open(PID_FILE, "r") as f:
        lines = f.read().splitlines()
        return [int(l) for l in lines if l.strip().isdigit()]

# --- CLI wiring ---
def main():
    init_db()
    parser = argparse.ArgumentParser(prog="queuectl")
    sub = parser.add_subparsers(dest="cmd")

    # enqueue
    e = sub.add_parser("enqueue")
    e.add_argument("job_json")

    # worker
    w = sub.add_parser("worker")
    wsub = w.add_subparsers(dest="action")
    ws = wsub.add_parser("start")
    ws.add_argument("--count", type=int, default=1)
    wsub.add_parser("stop")

    # status
    sub.add_parser("status")

    # list
    l = sub.add_parser("list")
    l.add_argument("--state", choices=["pending","processing","completed","failed","dead"], default=None)

    # dlq
    dlq = sub.add_parser("dlq")
    dlqsub = dlq.add_subparsers(dest="action")
    dlqsub.add_parser("list")
    retry = dlqsub.add_parser("retry")
    retry.add_argument("job_id")

    # config
    cfg = sub.add_parser("config")
    cfgsub = cfg.add_subparsers(dest="action")
    cfgset = cfgsub.add_parser("set")
    cfgset.add_argument("key")
    cfgset.add_argument("value")
    cfgsub.add_parser("get").add_argument("key")

    args = parser.parse_args()

    if args.cmd == "enqueue":
        enqueue_job(args.job_json)
    elif args.cmd == "worker":
        if args.action == "start":
            start_workers(args.count)
        elif args.action == "stop":
            stop_workers()
        else:
            parser.print_help()
    elif args.cmd == "status":
        get_status()
    elif args.cmd == "list":
        list_jobs(args.state)
    elif args.cmd == "dlq":
        if args.action == "list":
            dlq_list()
        elif args.action == "retry":
            dlq_retry(args.job_id)
        else:
            parser.print_help()
    elif args.cmd == "config":
        if args.action == "set":
            set_config(args.key, args.value)
        elif args.action == "get":
            v = get_config_int(args.key)
            print(v)
        else:
            parser.print_help()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()