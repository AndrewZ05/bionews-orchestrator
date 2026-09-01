#!/usr/bin/env python3
"""Raw-PyMySQL probe: isolate SQLAlchemy as a variable.

The existing probe_wp_mysql.py uses SQLAlchemy. The orchestrator's
extractor uses SQLAlchemy. If SQLAlchemy's pool / pre-ping / retry
behavior is interacting badly with the SSH tunnel and WP Engine's MySQL,
we need to know. This probe uses raw PyMySQL (no pool, no pre-ping, no
abstractions) so we can compare apples-to-apples.

Per iteration:
  1. Open SSH tunnel  (same as existing probe)
  2. Open RAW PyMySQL connection (no SQLAlchemy)
  3. Run a few queries
  4. Hold the connection idle for some seconds (optional)
  5. Run a final query to test idle survival
  6. Close everything cleanly

Compare results against `probe_wp_mysql.py` against the same sites to
see if behavior differs between the two MySQL access paths.

Usage:
  python scripts/probe_wp_raw.py bnphforumprd          # 5 iterations
  python scripts/probe_wp_raw.py bnphforumprd -n 10
  python scripts/probe_wp_raw.py bnahusprd             # try a known-failing site
  python scripts/probe_wp_raw.py bnphforumprd --idle 60
                                                # hold idle 60s between queries

Read-only.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

logging.getLogger("paramiko").setLevel(logging.ERROR)
logging.getLogger("invoke").setLevel(logging.ERROR)
logging.getLogger("fabric").setLevel(logging.ERROR)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import pymysql

from plugins.wordpress_extractor import (
    get_wp_credentials,
    find_free_port,
    start_ssh_tunnel,
    stop_ssh_tunnel,
    get_effective_table_prefix,
)
from shared.config_loader import load_config


def fmt_s(seconds):
    return f"{seconds:>6.2f}s"


def probe_raw_once(cfg, site, ssh_key_path, idle_s=0):
    """Run one full SSH->tunnel->raw-PyMySQL->query cycle. Returns dict of timings."""
    result = {
        "t_creds": None,
        "t_tunnel": None,
        "t_mysql_conn": None,
        "t_query1": None,
        "t_query2": None,
        "t_idle_query": None,
        "rows": None,
        "status": "",
        "error": "",
    }
    t_start = time.time()
    conn = None
    tunnel = None
    try:
        # Step 1: cached wp-config
        t0 = time.time()
        db_config = get_wp_credentials(site, cfg)
        result["t_creds"] = time.time() - t0

        # Step 2: open SSH tunnel
        host = cfg["connection_patterns"]["wpengine"]["ssh_host_pattern"].format(site_name=site)
        user = cfg["connection_patterns"]["wpengine"]["ssh_user_pattern"].format(site_name=site)
        local_port, port_sock = find_free_port()
        t0 = time.time()
        tunnel = start_ssh_tunnel(
            host, user, ssh_key_path,
            db_config["host"], db_config["port"], local_port,
            port_socket=port_sock,
        )
        result["t_tunnel"] = time.time() - t0

        # Step 3: raw PyMySQL connect (NO SQLAlchemy)
        t0 = time.time()
        conn = pymysql.connect(
            host="127.0.0.1",
            port=local_port,
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["name"],
            charset="utf8mb4",
            connect_timeout=120,
            read_timeout=300,
            write_timeout=300,
        )
        result["t_mysql_conn"] = time.time() - t0

        # Set the same session timeout the extractor sets
        with conn.cursor() as cur:
            try:
                cur.execute("SET SESSION max_execution_time = 300000")
            except Exception:
                pass

        # Step 4: first query (the handshake-confirming SELECT 1)
        prefix = db_config["table_prefix"]
        full_table = f"`{get_effective_table_prefix(prefix, 'options')}options`"

        t0 = time.time()
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {full_table}")
            row = cur.fetchone()
            result["rows"] = int(row[0]) if row else None
        result["t_query1"] = time.time() - t0

        # Step 5: second query - tests connection reuse
        t0 = time.time()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        result["t_query2"] = time.time() - t0

        # Step 6 (optional): idle hold + query
        if idle_s > 0:
            time.sleep(idle_s)
            t0 = time.time()
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {full_table}")
                cur.fetchone()
            result["t_idle_query"] = time.time() - t0

        result["status"] = "OK"
    except Exception as e:
        result["status"] = "ERR"
        result["error"] = f"{type(e).__name__}: {str(e)[:200]}"
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
        try:
            if tunnel is not None:
                stop_ssh_tunnel(tunnel)
        except Exception:
            pass
    result["total"] = time.time() - t_start
    return result


def main():
    ap = argparse.ArgumentParser(description="Raw-PyMySQL probe (no SQLAlchemy)")
    ap.add_argument("site")
    ap.add_argument("-n", "--iterations", type=int, default=5)
    ap.add_argument("--idle", type=int, default=0,
                    help="Hold connection idle N seconds before final query")
    ap.add_argument("--key", default=None)
    args = ap.parse_args()

    ssh_key_path = args.key or os.environ.get("WP_SSH_KEY_PATH")
    if not ssh_key_path:
        print("ERROR: SSH key not set. Set WP_SSH_KEY_PATH or pass --key", file=sys.stderr)
        sys.exit(2)
    ssh_key_path = os.path.expanduser(ssh_key_path.replace("\\", "/"))

    cfg = load_config("wordpress")

    print(f"Raw PyMySQL probe: site={args.site!r} iterations={args.iterations} idle={args.idle}s")
    print(f"{'iter':>4s} {'t_tunnel':>9s} {'t_conn':>9s} {'t_q1':>9s} {'t_q2':>9s} {'t_idle_q':>9s} {'rows':>9s} {'total':>9s}  status")
    print("-" * 100)

    n_ok = n_err = 0
    err_signatures = {}
    for i in range(1, args.iterations + 1):
        r = probe_raw_once(cfg, args.site, ssh_key_path, idle_s=args.idle)
        marker = "OK " if r["status"] == "OK" else "ERR"
        rows_s = str(r["rows"]) if r["rows"] is not None else "-"
        idle_q_s = fmt_s(r["t_idle_query"]) if r["t_idle_query"] is not None else "    -    "
        print(
            f"{i:>4d}"
            f" {fmt_s(r['t_tunnel'] or 0)}"
            f" {fmt_s(r['t_mysql_conn'] or 0)}"
            f" {fmt_s(r['t_query1'] or 0)}"
            f" {fmt_s(r['t_query2'] or 0)}"
            f" {idle_q_s}"
            f" {rows_s:>9s}"
            f" {fmt_s(r['total'])}"
            f"  {marker}"
            + (f" -- {r['error']}" if r["status"] == "ERR" else "")
        )
        if r["status"] == "OK":
            n_ok += 1
        else:
            n_err += 1
            sig = r["error"][:60]
            err_signatures[sig] = err_signatures.get(sig, 0) + 1
        time.sleep(1.0)

    print()
    print("=" * 100)
    print(f"SUMMARY  {n_ok}/{args.iterations} OK, {n_err} ERR  (raw PyMySQL - no SQLAlchemy)")
    print("=" * 100)
    if n_err:
        for sig, n in sorted(err_signatures.items(), key=lambda x: -x[1]):
            print(f"  ({n}x) {sig}")
    print()
    print("Compare these results with scripts/probe_wp_mysql.py against the same")
    print("sites. If the SQLAlchemy probe fails but this one succeeds, SQLAlchemy")
    print("is the variable to fix. If both fail equally, the issue is WP Engine.")

    sys.exit(0 if n_err == 0 else 1)


if __name__ == "__main__":
    main()
