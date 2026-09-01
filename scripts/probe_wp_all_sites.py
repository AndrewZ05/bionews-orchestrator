#!/usr/bin/env python3
"""Raw-PyMySQL probe across ALL ~131 sites sequentially.

The single-site raw probe (probe_wp_raw.py) was 18/18 OK on three sites,
which proved SQLAlchemy is sensitive to something the raw client isn't.
But the orchestrator cascade pattern usually starts at site 30-50 - some
state ACCUMULATES across the run. This probe rules out (or confirms)
whether the cascade is also visible in raw PyMySQL when you actually do
all 131 sites in a row.

For each site, sequentially:
  1. Open SSH tunnel
  2. Open RAW PyMySQL connection (no SQLAlchemy)
  3. Run SELECT 1 against wp_options table
  4. Close MySQL + tunnel cleanly
  5. Move on to next site

The connection close after each site mirrors what the orchestrator's
extract_site_data does between table-loops. The accumulating cost (open
file descriptors, ssh subprocess churn, WP Engine's connection-count
throttle) is whatever we measure here.

Output per site: t_tunnel, t_mysql_conn, t_query, status. Summary at end
with stats on where failures start.

Usage:
  python scripts/probe_wp_all_sites.py                    # all sites
  python scripts/probe_wp_all_sites.py -n 60              # first 60 only
  python scripts/probe_wp_all_sites.py --sleep 2          # 2s between sites

Read-only.
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from collections import Counter
from datetime import datetime
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
    get_available_sites,
)
from shared.config_loader import load_config


def probe_one_site(cfg, site, ssh_key_path):
    """Open SSH tunnel + raw PyMySQL + SELECT 1, then close.
    Returns timings + status."""
    result = {
        "site": site,
        "t_tunnel": None,
        "t_mysql_conn": None,
        "t_query": None,
        "rows": None,
        "status": "ERR",
        "error": "",
    }
    t_start = time.time()
    conn = None
    tunnel = None
    try:
        db_config = get_wp_credentials(site, cfg)
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

        prefix = db_config["table_prefix"]
        full_table = f"`{get_effective_table_prefix(prefix, 'options')}options`"

        t0 = time.time()
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {full_table}")
            row = cur.fetchone()
            result["rows"] = int(row[0]) if row else None
        result["t_query"] = time.time() - t0
        result["status"] = "OK"
    except Exception as e:
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
    ap = argparse.ArgumentParser(description="Raw-PyMySQL probe across all WordPress sites")
    ap.add_argument("-n", "--limit", type=int, default=None,
                    help="Only probe the first N sites (default: all)")
    ap.add_argument("--sleep", type=float, default=1.0,
                    help="Seconds between sites (default 1.0)")
    ap.add_argument("--key", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    ssh_key_path = args.key or os.environ.get("WP_SSH_KEY_PATH")
    if not ssh_key_path:
        print("ERROR: SSH key not set. Set WP_SSH_KEY_PATH or pass --key", file=sys.stderr)
        sys.exit(2)
    ssh_key_path = os.path.expanduser(ssh_key_path.replace("\\", "/"))

    cfg = load_config("wordpress")
    sites = get_available_sites(cfg)
    if not sites:
        print("ERROR: no sites discovered from API", file=sys.stderr)
        sys.exit(2)

    if args.limit:
        sites = sites[: args.limit]

    print(f"Raw-PyMySQL probe across {len(sites)} sites (sleep {args.sleep}s between)")
    print(f"{'#':>4s} {'site':<22s} {'t_tunnel':>9s} {'t_conn':>9s} {'t_query':>9s} {'rows':>8s} {'total':>9s}  status")
    print("-" * 100)

    # Output CSV
    if args.out:
        out_path = Path(args.out)
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = Path("docs/wordpress_audit") / f"raw_probe_all_sites_{stamp}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    results = []
    n_ok = 0
    n_err = 0
    err_counter = Counter()
    first_err_idx = None
    consec_ok = 0
    longest_consec_ok = 0

    t_run_start = time.time()
    for i, site in enumerate(sites, 1):
        r = probe_one_site(cfg, site, ssh_key_path)
        results.append(r)
        marker = "OK " if r["status"] == "OK" else "ERR"
        rows_s = str(r["rows"]) if r["rows"] is not None else "-"
        t_tunnel_s = f"{r['t_tunnel']:>6.2f}s" if r["t_tunnel"] is not None else "    -    "
        t_conn_s = f"{r['t_mysql_conn']:>6.2f}s" if r["t_mysql_conn"] is not None else "    -    "
        t_query_s = f"{r['t_query']:>6.2f}s" if r["t_query"] is not None else "    -    "
        line = (
            f"{i:>4d} {site:<22s} {t_tunnel_s} {t_conn_s} {t_query_s} "
            f"{rows_s:>8s} {r['total']:>6.2f}s  {marker}"
        )
        if r["status"] == "ERR":
            line += f" -- {r['error']}"
            n_err += 1
            err_counter[r["error"][:80]] += 1
            if first_err_idx is None:
                first_err_idx = i
            consec_ok = 0
        else:
            n_ok += 1
            consec_ok += 1
            longest_consec_ok = max(longest_consec_ok, consec_ok)
        print(line, flush=True)

        if args.sleep > 0 and i < len(sites):
            time.sleep(args.sleep)

    total_runtime = time.time() - t_run_start

    # CSV
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "site", "status", "t_tunnel", "t_mysql_conn", "t_query",
            "rows", "total", "error",
        ])
        w.writeheader()
        for r in results:
            w.writerow(r)

    print()
    print("=" * 100)
    print(f"SUMMARY  {n_ok}/{len(sites)} OK, {n_err} ERR  (raw PyMySQL, sequential, no SQLAlchemy)")
    print(f"Total runtime: {total_runtime:.0f}s")
    print(f"First failure: " + (f"site #{first_err_idx}" if first_err_idx else "(none)"))
    print(f"Longest run of consecutive OK: {longest_consec_ok}")
    print("=" * 100)
    if err_counter:
        print("Error signatures:")
        for sig, n in err_counter.most_common():
            print(f"  ({n}x) {sig}")
    print()
    print(f"CSV written: {out_path}")
    print()
    print("Interpretation guide:")
    print("  If 131/131 OK -> the SQLAlchemy refactor IS the right fix; raw client")
    print("                   handles the full production load with no problem.")
    print("  If failures start at site ~30-50 like the orchestrator did -> the cause")
    print("                   is NOT just SQLAlchemy; cumulative effect (per-IP")
    print("                   throttle, local FD exhaustion, etc.) is the real driver.")
    print("                   Refactor alone will not fix it.")
    print("  If random failures throughout -> mixed picture; SQLAlchemy contributes")
    print("                   but isn't the only cause.")

    sys.exit(0 if n_err == 0 else 1)


if __name__ == "__main__":
    main()
