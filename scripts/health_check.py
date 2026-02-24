#!/usr/bin/env python3
"""
hl-predictor health check script.
Outputs a JSON report of data freshness and collector process state.
"""
import json
import subprocess
import sys
from datetime import datetime, timezone

try:
    import psycopg2
except ImportError:
    subprocess.run([sys.executable, "-m", "pip", "install", "psycopg2-binary", "-q"])
    import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "hldata",
    "user": "hl",
    "password": "hlpassword",
}

# Thresholds: how old (minutes) before a table is considered stale
STALE_THRESHOLDS = {
    "trades": 5,
    "oi_snapshots": 10,
    "orderbook_imbalance": 10,
    "liquidations": 60,   # liquidations are sparse, allow up to 1h gap
    "candles": 5,         # candles are collected live
}


def now_utc():
    return datetime.now(timezone.utc)


def check_tables():
    results = {}
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        for table, threshold_min in STALE_THRESHOLDS.items():
            try:
                cur.execute(f"SELECT MAX(time) FROM {table}")
                row = cur.fetchone()
                last_ts = row[0] if row else None

                if last_ts is None:
                    results[table] = {
                        "status": "EMPTY",
                        "last_row": None,
                        "age_minutes": None,
                        "threshold_minutes": threshold_min,
                    }
                else:
                    if last_ts.tzinfo is None:
                        last_ts = last_ts.replace(tzinfo=timezone.utc)
                    age_min = (now_utc() - last_ts).total_seconds() / 60
                    status = "OK" if age_min <= threshold_min else "STALE"
                    results[table] = {
                        "status": status,
                        "last_row": last_ts.isoformat(),
                        "age_minutes": round(age_min, 1),
                        "threshold_minutes": threshold_min,
                    }
            except Exception as e:
                results[table] = {"status": "ERROR", "error": str(e)}

        cur.close()
        conn.close()
    except Exception as e:
        return {"db_error": str(e)}
    return results


def check_collector_process():
    try:
        result = subprocess.run(
            ["pgrep", "-f", "run_collectors"],
            capture_output=True, text=True
        )
        pids = result.stdout.strip().split() if result.stdout.strip() else []
        return {"running": len(pids) > 0, "pids": pids}
    except Exception as e:
        return {"running": False, "error": str(e)}


def check_docker():
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Status}}", "hl_tsdb"],
            capture_output=True, text=True
        )
        return {"hl_tsdb": result.stdout.strip()}
    except Exception as e:
        return {"error": str(e)}


def restart_collector():
    """Try to restart the collector process."""
    try:
        # Kill existing
        subprocess.run(["pkill", "-f", "run_collectors"], capture_output=True)
        import time; time.sleep(2)
        # Restart in background
        proc = subprocess.Popen(
            ["/home/jwlee/code/hl-predictor/.venv/bin/python", "-m", "run_collectors"],
            cwd="/home/jwlee/code/hl-predictor",
            stdout=open("/home/jwlee/code/hl-predictor/data/collectors.log", "a"),
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        return {"restarted": True, "pid": proc.pid}
    except Exception as e:
        return {"restarted": False, "error": str(e)}


if __name__ == "__main__":
    report = {
        "checked_at": now_utc().isoformat(),
        "tables": check_tables(),
        "collector": check_collector_process(),
        "docker": check_docker(),
    }

    # Auto-restart if collector is dead
    if not report["collector"].get("running"):
        report["restart_attempt"] = restart_collector()

    print(json.dumps(report, indent=2))
