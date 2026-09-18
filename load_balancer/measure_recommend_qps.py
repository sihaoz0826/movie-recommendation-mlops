#!/usr/bin/env python3
"""One-shot QPS from existing GET /recommend traffic. Does not generate load.

Each balancer /recommend is INSERTed into Azure SQL `routing_decisions` when
ENABLE_DB_LOGGING=true (set on the Kubernetes load-balancer Deployment).
This script COUNTs those rows over a window and prints QPS.

  python measure_recommend_qps.py                 # last 5 minutes
  python measure_recommend_qps.py --minutes 10
  python measure_recommend_qps.py --seconds 60    # wait 60s, count that window

Needs AZURE_SERVER / AZURE_DATABASE / AZURE_USERNAME / AZURE_PASSWORD (same
as the balancer). If ENABLE_DB_LOGGING is off, the table will not get new rows.
Not a load test — does not call GET /recommend.
"""

import argparse
import os
import sys
import time

try:
    import pyodbc
except ImportError:
    print("pyodbc is required. pip install pyodbc", file=sys.stderr)
    sys.exit(1)

# Same env as load_balancer.py / dashboard_api (local .env, or already in env).
if os.environ.get("KUBERNETES_SERVICE_HOST") is None:
    try:
        from dotenv import load_dotenv

        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
        if os.path.exists(env_path):
            load_dotenv(dotenv_path=env_path)
    except ImportError:
        pass

DRIVERS = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "ODBC Driver 13 for SQL Server",
    "SQL Server Native Client 11.0",
    "SQL Server",
]


def connect():
    server = os.environ.get("AZURE_SERVER")
    database = os.environ.get("AZURE_DATABASE")
    username = os.environ.get("AZURE_USERNAME")
    password = os.environ.get("AZURE_PASSWORD")
    if not all([server, database, username, password]):
        print(
            "Azure SQL is not configured. Set AZURE_SERVER, AZURE_DATABASE, "
            "AZURE_USERNAME, AZURE_PASSWORD (load_balancer/.env or the environment).\n"
            "The balancer only writes /recommend rows when ENABLE_DB_LOGGING=true.",
            file=sys.stderr,
        )
        sys.exit(1)

    last_err = None
    for driver in DRIVERS:
        try:
            return pyodbc.connect(
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=30;"
            )
        except pyodbc.Error as e:
            last_err = e
            err = str(e).lower()
            if "im002" in err or "driver" in err or "data source name not found" in err:
                continue
            print(f"Azure SQL connection failed: {e}", file=sys.stderr)
            sys.exit(1)

    print(
        f"Azure SQL connection failed (no ODBC driver). Last error: {last_err}",
        file=sys.stderr,
    )
    sys.exit(1)


def count_since(cursor, start_utc):
    cursor.execute(
        "SELECT COUNT(*) FROM routing_decisions WHERE [timestamp] >= ?",
        start_utc,
    )
    return int(cursor.fetchone()[0])


def print_result(count, seconds, window_label):
    qps = count / seconds if seconds > 0 else 0.0
    print(f"Window: {window_label}")
    print(f"GET /recommend rows (routing_decisions): {count}")
    print(f"QPS: {qps:.2f}")
    if count == 0:
        print(
            "No rows in this window. ENABLE_DB_LOGGING must be true on the load "
            "balancer, and there must already be GET /recommend traffic. "
            "This script does not generate load."
        )


def main():
    parser = argparse.ArgumentParser(
        description="Measure QPS from existing balancer GET /recommend rows in Azure SQL. Does not generate load."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--minutes",
        type=float,
        default=None,
        help="COUNT rows in the last N minutes (default: 5)",
    )
    group.add_argument(
        "--seconds",
        type=float,
        default=None,
        help="Wait N seconds of existing traffic, then COUNT that window",
    )
    args = parser.parse_args()

    conn = connect()
    try:
        cursor = conn.cursor()
        if args.seconds is not None:
            if args.seconds <= 0:
                print("--seconds must be > 0", file=sys.stderr)
                sys.exit(1)
            cursor.execute("SELECT GETUTCDATE()")
            start = cursor.fetchone()[0]
            print(
                f"Measuring existing GET /recommend traffic for {args.seconds:g}s "
                "(not generating load)..."
            )
            time.sleep(args.seconds)
            count = count_since(cursor, start)
            print_result(count, args.seconds, f"{args.seconds:g}s live window")
        else:
            minutes = 5.0 if args.minutes is None else args.minutes
            if minutes <= 0:
                print("--minutes must be > 0", file=sys.stderr)
                sys.exit(1)
            seconds = minutes * 60.0
            cursor.execute("SELECT DATEADD(second, ?, GETUTCDATE())", -int(seconds))
            start = cursor.fetchone()[0]
            count = count_since(cursor, start)
            print_result(count, seconds, f"last {minutes:g} minutes ({seconds:g}s)")
    except pyodbc.Error as e:
        print(
            f"Query failed: {e}\n"
            "Need table routing_decisions (balancer INSERT when ENABLE_DB_LOGGING=true).",
            file=sys.stderr,
        )
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
