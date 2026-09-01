#!/usr/bin/env python3
"""Reproduce the orchestrator's exact concurrency pattern in isolation.

The serial probes (probe_wp_all_sites.py / _sqla.py) both succeed 131/131,
which means SSH+MySQL+SQLAlchemy work fine sequentially. But the
orchestrator fails. What's left? CONCURRENCY: the orchestrator runs
multiple worker threads, a heartbeat thread, and parallel pre-warm.

This probe uses the actual orchestrator code paths (get_pooled_connection
+ execute_query) but runs them under the orchestrator's concurrency
pattern in isolation:

  Phase 1: Pre-warm 131 sites with 8 parallel workers (orchestrator default)
  Phase 2: Start the heartbeat thread (orchestrator default)
  Phase 3: Run a fan-out of N worker threads. Each thread takes sites
           round-robin and runs M queries against each (simulating the
           multi-table loop). Threads share the CONNECTION_POOL.

If THIS fails like the orchestrator does -> concurrency is the cause.
If THIS succeeds -> the bug is somewhere else (e.g. row-streaming with
large meta tables, SQLAlchemy text() with parameterized queries, the
chunked-OFFSET pattern, etc.) and we need to look there.

Usage:
  python scripts/probe_wp_concurrent.py                  # default: 8 prewarm, 2 workers, 9 queries per site
  python scripts/probe_wp_concurrent.py --workers 4      # try 4 workers (faster pace)
  python scripts/probe_wp_concurrent.py --no-heartbeat   # disable heartbeat thread to isolate
  python scripts/probe_wp_concurrent.py --no-prewarm     # skip pre-warm phase
  python scripts/probe_wp_concurrent.py -n 30            # first 30 sites only
  python scripts/probe_wp_concurrent.py --queries 18     # 18 queries/site (heavy test)

Read-only.
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logging.getLogger("paramiko").setLevel(logging.ERROR)
logging.getLogger("invoke").setLevel(logging.ERROR)
logging.getLogger("fabric").setLevel(logging.ERROR)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from plugins.wordpress_extractor import (
    get_pooled_connection,
    execute_query,
    cleanup_pooled_connection,
    cleanup_all_connections,
    prewarm_site_connections,
    start_pool_heartbeat,
    stop_pool_heartbeat,
    get_effective_table_prefix,
    get_available_sites,
    CONNECTION_POOL,
)
from shared.config_loader import load_config


def do_site_work(cfg, site, queries_per_site):
    """Simulate one site's full extraction: get the pooled connection,
    run N queries against it sequentially (mimicking the table loop).

    Returns (site, status, n_ok, n_err, first_error, elapsed).
    """
    t_start = time.time()
    n_ok = 0
    n_err = 0
    first_error = ""
    try:
        # 1) get a pooled connection (same call the table loop makes)
        conn_info = get_pooled_connection(site, cfg)
        prefix = conn_info["table_prefix"]
        full_table = f"`{get_effective_table_prefix(prefix, 'options')}options`"

        # 2) run N queries against that connection (mimicking N tables)
        for q in range(queries_per_site):
            try:
                rows = execute_query(conn_info, f"SELECT COUNT(*) as cnt FROM {full_table}")
                if rows:
                    n_ok += 1
                else:
                    n_err += 1
                    if not first_error:
                        first_error = "empty result"
            except Exception as e:
                n_err += 1
                if not first_error:
                    first_error = f"{type(e).__name__}: {str(e)[:120]}"
    except Exception as e:
        n_err += queries_per_site - (n_ok + n_err)  # remaining count as errors
        if not first_error:
            first_error = f"get_pooled_connection: {type(e).__name__}: {str(e)[:120]}"

    elapsed = time.time() - t_start
    status = "OK" if n_err == 0 else ("PARTIAL" if n_ok > 0 else "ERR")
    return site, status, n_ok, n_err, first_error, elapsed


def main():
    ap = argparse.ArgumentParser(description="Concurrent orchestrator-pattern probe")
    ap.add_argument("-n", "--limit", type=int, default=None)
    ap.add_argument("--workers", type=int, default=2,
                    help="Number of concurrent table-loop workers (default 2)")
    ap.add_argument("--queries", type=int, default=9,
                    help="Queries per site - mimics N tables (default 9 = core group)")
    ap.add_argument("--prewarm-workers", type=int, default=8,
                    help="Pre-warm parallelism (default 8)")
    ap.add_argument("--no-prewarm", action="store_true",
                    help="Skip the pre-warm phase (connections form on-demand)")
    ap.add_argument("--no-heartbeat", action="store_true",
                    help="Skip the heartbeat thread (isolates whether HB is the cause)")
    ap.add_argument("--key", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    ssh_key_path = args.key or os.environ.get("WP_SSH_KEY_PATH")
    if not ssh_key_path:
        print("ERROR: SSH key not set. Set WP_SSH_KEY_PATH or pass --key", file=sys.stderr)
        sys.exit(2)

    cfg = load_config("wordpress")
    sites = get_available_sites(cfg)
    if not sites:
        print("ERROR: no sites discovered from API", file=sys.stderr)
        sys.exit(2)
    if args.limit:
        sites = sites[: args.limit]

    print(f"Concurrent orchestrator-pattern probe")
    print(f"  sites             = {len(sites)}")
    print(f"  workers           = {args.workers}  (table-loop concurrency)")
    print(f"  queries per site  = {args.queries}  (mimics N tables in core group)")
    print(f"  pre-warm          = {'yes' if not args.no_prewarm else 'NO'}  "
          f"(workers={args.prewarm_workers})")
    print(f"  heartbeat thread  = {'yes' if not args.no_heartbeat else 'NO'}")
    print()

    t_overall = time.time()
    prewarm_failures = {}

    # PHASE 1: pre-warm (matches orchestrator behavior)
    if not args.no_prewarm:
        print(f"Phase 1: pre-warm {len(sites)} sites with {args.prewarm_workers} parallel workers...")
        t0 = time.time()
        prewarm_failures = prewarm_site_connections(
            sites, cfg, max_workers=args.prewarm_workers,
        )
        print(f"Phase 1 complete: {len(sites) - len(prewarm_failures)}/{len(sites)} "
              f"warmed in {time.time()-t0:.1f}s ({len(prewarm_failures)} failed)")
        print()

    # PHASE 2: start heartbeat (matches orchestrator)
    if not args.no_heartbeat and not args.no_prewarm:
        start_pool_heartbeat()

    # Sites we'll actually probe: skip pre-warm failures
    sites_to_test = [s for s in sites if s not in prewarm_failures]

    print(f"Phase 3: fan out {args.workers} concurrent workers, each runs "
          f"{args.queries} queries per site against {len(sites_to_test)} sites...")
    print(f"{'#':>4s} {'site':<22s} {'status':<8s} {'ok/total':>10s} {'elapsed':>8s}  detail")
    print("-" * 100)

    if args.out:
        out_path = Path(args.out)
    else:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = Path("docs/wordpress_audit") / f"concurrent_probe_{stamp}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    results = []
    n_ok = n_partial = n_err = 0
    err_counter = Counter()
    completed_count = 0
    completed_lock = threading.Lock()

    def submit_and_print(executor):
        nonlocal n_ok, n_partial, n_err, completed_count
        futures = {
            executor.submit(do_site_work, cfg, s, args.queries): s
            for s in sites_to_test
        }
        for fut in as_completed(futures):
            site, status, ok, err, err_msg, elapsed = fut.result()
            with completed_lock:
                completed_count += 1
                idx = completed_count
            results.append({
                "site": site, "status": status,
                "n_ok": ok, "n_err": err,
                "elapsed": round(elapsed, 2),
                "error": err_msg,
            })
            if status == "OK":
                n_ok += 1
            elif status == "PARTIAL":
                n_partial += 1
            else:
                n_err += 1
            if err_msg:
                err_counter[err_msg[:80]] += 1
            tot = ok + err
            print(
                f"{idx:>4d} {site:<22s} {status:<8s} {ok}/{tot:<8s} "
                f"{elapsed:>6.2f}s  {err_msg[:60] if err_msg else ''}",
                flush=True,
            )

    try:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            submit_and_print(executor)
    finally:
        if not args.no_heartbeat and not args.no_prewarm:
            stop_pool_heartbeat()
        cleanup_all_connections()

    total_runtime = time.time() - t_overall

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "site", "status", "n_ok", "n_err", "elapsed", "error",
        ])
        w.writeheader()
        for r in results:
            w.writerow(r)

    print()
    print("=" * 100)
    print(f"CONCURRENT PROBE SUMMARY")
    print("=" * 100)
    print(f"  Sites probed:     {len(sites_to_test)}")
    print(f"  Pre-warm failed:  {len(prewarm_failures)}")
    print(f"  OK (all queries): {n_ok}")
    print(f"  PARTIAL:          {n_partial}")
    print(f"  ERR (all failed): {n_err}")
    print(f"  Total runtime:    {total_runtime:.0f}s")
    print()
    if err_counter:
        print("Error signatures (top 5):")
        for sig, n in err_counter.most_common(5):
            print(f"  ({n}x) {sig}")
        print()
    print(f"CSV: {out_path}")
    print()
    print("Interpretation:")
    print("  All 131 sites OK -> concurrency is NOT the problem; bug is elsewhere")
    print("                    (chunked OFFSET? text() vs raw queries? large rows?)")
    print("  Partial/Err in pattern matching orchestrator's failures -> CONCURRENCY")
    print("                    is the variable. The fix is reducing thread sharing")
    print("                    on engines, not switching MySQL clients.")
    print()
    print("To isolate WHICH concurrency feature triggers the bug, re-run with:")
    print("  --no-heartbeat   (rule out heartbeat-thread races)")
    print("  --no-prewarm     (rule out pre-warm contention)")
    print("  --workers 1      (rule out worker-pool races on shared engines)")

    sys.exit(0 if (n_err == 0 and n_partial == 0) else 1)


if __name__ == "__main__":
    main()
