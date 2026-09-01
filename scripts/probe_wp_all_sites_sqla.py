#!/usr/bin/env python3
"""SQLAlchemy probe across ALL ~131 sites sequentially.

The counterpart to probe_wp_all_sites.py (which uses raw PyMySQL).
This one uses SQLAlchemy with the exact same engine settings the
orchestrator uses: pool_pre_ping=True, pool_recycle=-1, the same
connect_timeout/read_timeout/write_timeout values.

Run BOTH probes back-to-back to get a head-to-head comparison:

  python scripts/probe_wp_all_sites.py       # raw PyMySQL, all 131 sites
  python scripts/probe_wp_all_sites_sqla.py  # SQLAlchemy, all 131 sites

If raw is 131/131 and SQLAlchemy is 27/131, the refactor is the right fix.
If both fail at similar rates, SQLAlchemy is not the (primary) cause.

For each site, sequentially:
  1. Open SSH tunnel
  2. Open SQLAlchemy engine with the orchestrator's exact pool settings
  3. engine.connect() + SELECT 1 (forces handshake + verifies)
  4. Close MySQL + tunnel
  5. Move on to next site

Same output format as probe_wp_all_sites.py so the two CSVs can be diffed.

Usage:
  python scripts/probe_wp_all_sites_sqla.py                # all sites
  python scripts/probe_wp_all_sites_sqla.py -n 60          # first 60 only
  python scripts/probe_wp_all_sites_sqla.py --sleep 2      # 2s between sites

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
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from sqlalchemy import create_engine, text

from plugins.wordpress_extractor import (
    get_wp_credentials,
    find_free_port,
    start_ssh_tunnel,
    stop_ssh_tunnel,
    get_effective_table_prefix,
    get_available_sites,
    # Pull in the same constants the extractor uses so this test exercises
    # the same engine settings as production.
    WP_MYSQL_CONNECT_TIMEOUT_S,
    WP_MYSQL_READ_TIMEOUT_S,
    WP_MYSQL_WRITE_TIMEOUT_S,
    WP_MYSQL_POOL_RECYCLE_S,
    WP_MYSQL_MAX_EXEC_TIME_MS,
)
from shared.config_loader import load_config


def probe_one_site(cfg, site, ssh_key_path):
    """Open SSH tunnel + SQLAlchemy engine + SELECT 1, then close.

    Uses the orchestrator's exact engine settings (pool_pre_ping=True,
    pool_recycle=WP_MYSQL_POOL_RECYCLE_S, etc.) so this probe reproduces
    the orchestrator's MySQL access pattern exactly. The ONLY difference
    from probe_wp_all_sites.py is the MySQL client library.
    """
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
    engine = None
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

        # Mirror plugins/wordpress_extractor.py:ssh_connect engine settings
        conn_str = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
            f"@127.0.0.1:{local_port}/{db_config['name']}"
            f"?charset=utf8mb4"
            f"&connect_timeout={WP_MYSQL_CONNECT_TIMEOUT_S}"
            f"&read_timeout={WP_MYSQL_READ_TIMEOUT_S}"
            f"&write_timeout={WP_MYSQL_WRITE_TIMEOUT_S}"
        )
        t0 = time.time()
        engine = create_engine(
            conn_str,
            pool_pre_ping=True,
            pool_recycle=WP_MYSQL_POOL_RECYCLE_S,
            echo=False,
            connect_args={
                "read_timeout": WP_MYSQL_READ_TIMEOUT_S,
                "write_timeout": WP_MYSQL_WRITE_TIMEOUT_S,
            },
        )
        # engine.connect() is what forces the real handshake (lazy until used)
        sa_conn = engine.connect()
        result["t_mysql_conn"] = time.time() - t0

        try:
            sa_conn.execute(text(f"SET SESSION max_execution_time = {WP_MYSQL_MAX_EXEC_TIME_MS}"))
        except Exception:
            pass

        prefix = db_config["table_prefix"]
        full_table = f"`{get_effective_table_prefix(prefix, 'options')}options`"

        t0 = time.time()
        row = sa_conn.execute(text(f"SELECT COUNT(*) FROM {full_table}")).fetchone()
        result["rows"] = int(row[0]) if row else None
        result["t_query"] = time.time() - t0

        sa_conn.close()
        result["status"] = "OK"
    except Exception as e:
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


def main():
    ap = argparse.ArgumentParser(description="SQLAlchemy probe across all WordPress sites")
    ap.add_argument("-n", "--limit", type=int, default=None)
    ap.add_argument("--sleep", type=float, default=1.0)
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

    print(f"SQLAlchemy probe across {len(sites)} sites (sleep {args.sleep}s between)")
    print(f"Using orchestrator's engine settings: pool_pre_ping=True, "
          f"pool_recycle={WP_MYSQL_POOL_RECYCLE_S}, "
          f"connect_timeout={WP_MYSQL_CONNECT_TIMEOUT_S}s")
    print(f"{'#':>4s} {'site':<22s} {'t_tunnel':>9s} {'t_conn':>9s} {'t_query':>9s} {'rows':>8s} {'total':>9s}  status")
    print("-" * 100)

    if args.out:
        out_path = Path(args.out)
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = Path("docs/wordpress_audit") / f"sqla_probe_all_sites_{stamp}.csv"
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
    print(f"SUMMARY  {n_ok}/{len(sites)} OK, {n_err} ERR  (SQLAlchemy + pool_pre_ping)")
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
    print("Compare with the raw PyMySQL probe:")
    print("  python scripts/probe_wp_all_sites.py")
    print()
    print("If raw PyMySQL succeeds 131/131 AND this SQLAlchemy probe fails")
    print("substantially on the SAME sites at similar indices -> SQLAlchemy")
    print("is the cause, refactor is the right fix.")
    print("If both fail at similar rates and locations -> the cause is")
    print("cumulative (WP Engine throttle, FD exhaustion) and switching")
    print("clients will not fix it.")

    sys.exit(0 if n_err == 0 else 1)


if __name__ == "__main__":
    main()
