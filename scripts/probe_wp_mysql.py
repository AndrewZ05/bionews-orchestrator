#!/usr/bin/env python3
"""Single-site SSH+MySQL health probe — isolates where "Lost connection during
query" actually comes from when an ETL run cascades.

The orchestrator's failure path is: open SSH -> open SSH tunnel for port
forwarding -> open MySQL connection via the tunnel -> SELECT FROM table.
"Lost connection during query" can come from any of those. This script
breaks each layer apart so we can see which one is actually failing.

Output per iteration:
  iter | t_ssh_open | t_tunnel_up | t_mysql_conn | t_query | total | status
       | (seconds)  | (seconds)   | (seconds)    | (seconds)| (sec) | OK / ERR
       | (- pre-query rows from a tiny COUNT(*) FROM wp_options)

Usage:
  python scripts/probe_wp_mysql.py bnphforumprd          # 20 iterations
  python scripts/probe_wp_mysql.py bnphforumprd -n 50    # custom count
  python scripts/probe_wp_mysql.py bnphforumprd --bigger # use COUNT against
                                                          # bigger table (postmeta)
  python scripts/probe_wp_mysql.py bnphforumprd --hold 30
                                                          # hold the MySQL
                                                          # connection idle 30s
                                                          # between queries to
                                                          # detect wait_timeout

Read-only. Never writes to MySQL. Cancellable with Ctrl-C.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

# Make repo root importable
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# Quiet noisy libs
logging.getLogger("paramiko").setLevel(logging.ERROR)
logging.getLogger("invoke").setLevel(logging.ERROR)
logging.getLogger("fabric").setLevel(logging.ERROR)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from fabric import Connection
from sqlalchemy import create_engine, text
import socket

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


def probe_once(cfg, site, ssh_key_path, target_table="options", hold_idle_s=0):
    """Run one full SSH->tunnel->MySQL->query cycle. Return a dict of timings
    and the resulting status string."""
    result = {
        "t_creds": None,
        "t_tunnel": None,
        "t_mysql_conn": None,
        "t_query": None,
        "t_hold": None,
        "t_query2": None,
        "rows": None,
        "status": "",
        "error": "",
    }
    t_start = time.time()
    engine = None
    tunnel = None
    try:
        # Step 1: get cached wp-config (should be near-instant)
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

        # Step 3: open MySQL through the tunnel
        t0 = time.time()
        conn_str = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
            f"@127.0.0.1:{local_port}/{db_config['name']}"
            f"?charset=utf8mb4&connect_timeout=30&read_timeout=120&write_timeout=120"
        )
        engine = create_engine(conn_str, pool_pre_ping=True, echo=False)
        mysql_conn = engine.connect()
        result["t_mysql_conn"] = time.time() - t0

        try:
            mysql_conn.execute(text("SET SESSION max_execution_time = 300000"))
        except Exception:
            pass

        # Step 4: trivial COUNT against the chosen table
        prefix = db_config["table_prefix"]
        effective_prefix = get_effective_table_prefix(prefix, target_table)
        full_table = f"`{effective_prefix}{target_table}`"
        t0 = time.time()
        rows = mysql_conn.execute(text(f"SELECT COUNT(*) AS c FROM {full_table}")).fetchall()
        result["t_query"] = time.time() - t0
        result["rows"] = int(rows[0][0]) if rows else None

        # Step 5 (optional): hold the connection idle, then run another query.
        # Detects whether wait_timeout is being hit on slow runs.
        if hold_idle_s > 0:
            t0 = time.time()
            time.sleep(hold_idle_s)
            result["t_hold"] = time.time() - t0
            t0 = time.time()
            mysql_conn.execute(text(f"SELECT 1")).fetchall()
            result["t_query2"] = time.time() - t0

        mysql_conn.close()
        result["status"] = "OK"
    except Exception as e:
        result["status"] = "ERR"
        result["error"] = f"{type(e).__name__}: {str(e)[:200]}"
    finally:
        try:
            if engine is not None:
                engine.dispose()
        except Exception:
            pass
        try:
            if tunnel is not None:
                stop_ssh_tunnel(tunnel)
        except Exception:
            pass
    result["total"] = time.time() - t_start
    return result


def probe_reuse(cfg, site, ssh_key_path, target_table, iterations,
                idle_between_s=0):
    """Open SSH+MySQL ONCE and run `iterations` queries against the same
    long-lived connection. Tests whether connection-reuse-across-tables
    would actually work in practice, or whether long-lived MySQL connections
    through the SSH tunnel degrade/drop over time.

    Prints one line per query showing how the per-query latency evolves.
    """
    print(f"\n[REUSE MODE] One SSH+MySQL connection, {iterations} queries")
    print(f"  site={site!r} table={target_table!r} idle_between={idle_between_s}s")
    print(f"  {'iter':>4s} {'t_query':>8s} {'idle':>6s} {'rows':>9s}  status")
    print("  " + "-" * 60)

    db_config = get_wp_credentials(site, cfg)
    host = cfg["connection_patterns"]["wpengine"]["ssh_host_pattern"].format(site_name=site)
    user = cfg["connection_patterns"]["wpengine"]["ssh_user_pattern"].format(site_name=site)
    local_port, port_sock = find_free_port()

    t0_total = time.time()
    tunnel = None
    engine = None
    mysql_conn = None
    n_ok = n_err = 0
    try:
        t0 = time.time()
        tunnel = start_ssh_tunnel(
            host, user, ssh_key_path,
            db_config["host"], db_config["port"], local_port,
            port_socket=port_sock,
        )
        print(f"  Tunnel up in {time.time()-t0:.2f}s")

        t0 = time.time()
        conn_str = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
            f"@127.0.0.1:{local_port}/{db_config['name']}"
            f"?charset=utf8mb4&connect_timeout=120&read_timeout=300&write_timeout=300"
        )
        engine = create_engine(conn_str, pool_pre_ping=True, pool_recycle=-1, echo=False)
        mysql_conn = engine.connect()
        try:
            mysql_conn.execute(text("SET SESSION max_execution_time = 300000"))
        except Exception:
            pass
        print(f"  MySQL connected in {time.time()-t0:.2f}s")
        print()

        prefix = db_config["table_prefix"]
        effective_prefix = get_effective_table_prefix(prefix, target_table)
        full_table = f"`{effective_prefix}{target_table}`"

        for i in range(1, iterations + 1):
            t0 = time.time()
            try:
                rows = mysql_conn.execute(text(f"SELECT COUNT(*) AS c FROM {full_table}")).fetchall()
                elapsed = time.time() - t0
                row_count = int(rows[0][0]) if rows else 0
                print(f"  {i:>4d} {elapsed:>6.2f}s {idle_between_s:>5d}s {row_count:>9d}  OK")
                n_ok += 1
            except Exception as e:
                elapsed = time.time() - t0
                print(f"  {i:>4d} {elapsed:>6.2f}s {idle_between_s:>5d}s {'-':>9s}  ERR -- {type(e).__name__}: {str(e)[:80]}")
                n_err += 1
                if isinstance(e, Exception) and "lost connection" in str(e).lower():
                    print(f"        ^^ connection died at query {i}/{iterations}, ending run")
                    break

            if i < iterations and idle_between_s > 0:
                time.sleep(idle_between_s)

    finally:
        try:
            if mysql_conn is not None:
                mysql_conn.close()
        except Exception:
            pass
        try:
            if engine is not None:
                engine.dispose()
        except Exception:
            pass
        try:
            if tunnel is not None:
                stop_ssh_tunnel(tunnel)
        except Exception:
            pass

    total = time.time() - t0_total
    print()
    print(f"  REUSE SUMMARY  {n_ok}/{iterations} OK, {n_err} ERR, total {total:.1f}s")
    if n_ok > 0:
        avg_query = (total - 1 - (iterations - 1) * idle_between_s) / max(1, n_ok)
        print(f"  Average per-query (excluding setup + idle waits): ~{avg_query:.2f}s")
    return n_err == 0


def main():
    ap = argparse.ArgumentParser(description="Single-site SSH+MySQL probe")
    ap.add_argument("site", help="Site name (e.g. bnphforumprd)")
    ap.add_argument("-n", "--iterations", type=int, default=20)
    ap.add_argument("--bigger", action="store_true",
                    help="Probe postmeta instead of options (larger COUNT)")
    ap.add_argument("--hold", type=int, default=0,
                    help="Hold MySQL connection idle N seconds between queries")
    ap.add_argument("--reuse", action="store_true",
                    help="Reuse a single SSH+MySQL connection for all iterations "
                         "(tests connection lifetime under repeated queries)")
    ap.add_argument("--reuse-idle", type=int, default=0,
                    help="In --reuse mode, sleep N seconds between queries to "
                         "simulate inter-table extraction delay")
    ap.add_argument("--key", default=None,
                    help="SSH private key path; default = $WP_SSH_KEY_PATH")
    args = ap.parse_args()

    ssh_key_path = args.key or os.environ.get("WP_SSH_KEY_PATH")
    if not ssh_key_path:
        print("ERROR: SSH key not set. Set WP_SSH_KEY_PATH or pass --key", file=sys.stderr)
        sys.exit(2)
    ssh_key_path = os.path.expanduser(ssh_key_path.replace("\\", "/"))

    cfg = load_config("wordpress")
    target_table = "postmeta" if args.bigger else "options"

    # --reuse mode: open ONCE, run many queries against the same connection.
    # Use this to verify whether connection-reuse-across-tables would work,
    # which is exactly the architecture the pre-warm plan depends on.
    if args.reuse:
        ok = probe_reuse(cfg, args.site, ssh_key_path,
                         target_table=target_table,
                         iterations=args.iterations,
                         idle_between_s=args.reuse_idle)
        sys.exit(0 if ok else 1)

    print(f"Probing site={args.site!r} table={target_table!r} iterations={args.iterations} hold_idle={args.hold}s")
    print(f"{'iter':>4s} {'t_creds':>9s} {'t_tunnel':>9s} {'t_mysql':>9s} {'t_query':>9s} {'rows':>9s} {'total':>9s}  status")
    print("-" * 95)

    n_ok = 0
    n_err = 0
    err_signatures = {}
    for i in range(1, args.iterations + 1):
        r = probe_once(cfg, args.site, ssh_key_path,
                       target_table=target_table, hold_idle_s=args.hold)
        marker = "OK " if r["status"] == "OK" else "ERR"
        rows_s = str(r["rows"]) if r["rows"] is not None else "-"
        print(
            f"{i:>4d}"
            f" {fmt_s(r['t_creds'] or 0)}"
            f" {fmt_s(r['t_tunnel'] or 0)}"
            f" {fmt_s(r['t_mysql_conn'] or 0)}"
            f" {fmt_s(r['t_query'] or 0)}"
            f" {rows_s:>9s}"
            f" {fmt_s(r['total'])}"
            f"  {marker}"
            + (f" -- {r['error']}" if r["status"] == "ERR" else "")
        )
        if r["status"] == "OK":
            n_ok += 1
        else:
            n_err += 1
            # Bucket by first 60 chars of error
            sig = r["error"][:60]
            err_signatures[sig] = err_signatures.get(sig, 0) + 1
        # Brief pause so we don't trigger any rate limit during the probe
        time.sleep(1.0)

    print()
    print("=" * 80)
    print(f"SUMMARY  {n_ok}/{args.iterations} OK, {n_err} ERR")
    print("=" * 80)
    if n_err:
        print("Error signatures:")
        for sig, n in sorted(err_signatures.items(), key=lambda x: -x[1]):
            print(f"  ({n:>3d}x) {sig}")

    sys.exit(0 if n_err == 0 else 1)


if __name__ == "__main__":
    main()
