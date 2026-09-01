#!/usr/bin/env python3
"""Pre-warm the wp-config credentials cache for all WordPress sites.

Reads the site list from the discovery API (same as the orchestrator),
SSHes to each site with retry, and persists credentials to
.wp_credentials.json.

After this completes, daily orchestrator runs use the cache and don't
SSH for wp-config at all. Re-run once per month (cache TTL is 30 days)
or whenever a site is added.

Usage:
  python scripts/warmup_wp_cache.py                  # all sites
  python scripts/warmup_wp_cache.py site1 site2      # specific sites
  python scripts/warmup_wp_cache.py --force          # ignore TTL, refresh all
  python scripts/warmup_wp_cache.py --workers 4      # tune concurrency
"""
import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Make repo root importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("paramiko").setLevel(logging.ERROR)
logging.getLogger("invoke").setLevel(logging.ERROR)

from shared.config_loader import load_config
from plugins.wordpress_extractor import (
    get_wp_credentials,
    invalidate_wp_creds,
    get_available_sites,
    WP_CREDS_CACHE_PATH,
)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sites", nargs="*", help="Site names; omit for all-sites discovery")
    ap.add_argument("--force", action="store_true", help="Invalidate existing cache entries before probing")
    ap.add_argument("--workers", type=int, default=6, help="Concurrent SSH probes (default 6)")
    args = ap.parse_args()

    cfg = load_config("wordpress")

    sites = args.sites or get_available_sites(cfg)
    if not sites:
        print("No sites discovered. Specify sites on the command line.", file=sys.stderr)
        sys.exit(2)

    print(f"Pre-warming wp-config cache for {len(sites)} sites (workers={args.workers}, force={args.force})")
    print(f"Cache file: {WP_CREDS_CACHE_PATH}")
    print()

    def probe(site):
        if args.force:
            invalidate_wp_creds(site)
        t0 = time.time()
        try:
            creds = get_wp_credentials(site, cfg)
            return site, True, time.time() - t0, f"db={creds['name']} prefix={creds['table_prefix']}"
        except Exception as e:
            return site, False, time.time() - t0, f"{type(e).__name__}: {str(e)[:200]}"

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(probe, s): s for s in sites}
        for i, fut in enumerate(as_completed(futures), 1):
            site, ok, elapsed, detail = fut.result()
            results.append((site, ok, elapsed, detail))
            marker = "OK  " if ok else "FAIL"
            print(f"  [{i:>3d}/{len(sites)}] {site:22s} [{elapsed:>5.1f}s] {marker} {detail}")

    n_ok = sum(1 for r in results if r[1])
    n_fail = len(results) - n_ok
    print()
    print("=" * 70)
    print(f"SUMMARY")
    print("=" * 70)
    print(f"  Successful: {n_ok}/{len(sites)}")
    print(f"  Failed:     {n_fail}/{len(sites)}")
    if n_fail:
        print()
        print("Failed sites (will need investigation):")
        for site, ok, _t, detail in results:
            if not ok:
                print(f"  {site}: {detail}")
        sys.exit(1)


if __name__ == "__main__":
    main()
