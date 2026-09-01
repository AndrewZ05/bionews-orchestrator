#!/usr/bin/env python3
"""
Probe Facebook Graph API v25.0 to discover which metrics are valid
at the /{post_id}/insights endpoint.

Tests candidate metric names one at a time and reports which ones
Facebook accepts vs. rejects.

Usage:
    python ops/probe_facebook_post_metrics.py
"""

import os
import sys
import time
from pathlib import Path

# -- locate repo root and load .env ------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

env_file = REPO_ROOT / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

POST_ID = "1546946778911219_1505188621619019"  # live post from 2026-06-22 run
PAGE_ID = POST_ID.split("_")[0]
ACCESS_TOKEN = os.environ.get("FACEBOOK_ACCESS_TOKEN", "")
API_VERSION = "v25.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"

# -- candidate metrics to probe ----------------------------------------------
CANDIDATES = {
    "Link Clicks (the goal)": [
        "post_link_clicks",
        "post_clicks_by_type",
        "post_clicks",
        "inline_link_clicks",
        "outbound_clicks",
        "link_clicks",
    ],
    "Views / Impressions (new v25 names)": [
        "post_media_view",
        "post_impressions",
        "post_impressions_unique",
        "post_impressions_organic",
        "post_impressions_organic_unique",
        "post_impressions_paid",
        "post_impressions_paid_unique",
        "post_impressions_viral",
        "post_impressions_viral_unique",
        "post_impressions_fan_unique",
    ],
    "Engagement": [
        "post_engaged_users",
        "post_negative_feedback",
        "post_negative_feedback_unique",
        "post_reactions_by_type_total",
        "post_reactions_like_total",
        "post_reactions_love_total",
        "post_reactions_wow_total",
        "post_reactions_haha_total",
        "post_reactions_sorry_total",
        "post_reactions_anger_total",
    ],
    "Video": [
        "post_video_views",
        "post_video_views_organic",
        "post_video_views_paid",
        "post_video_complete_views_30s",
        "post_video_avg_time_watched",
    ],
}

PASS = "\033[32mACCEPTED\033[0m"
FAIL = "\033[31mREJECTED\033[0m"
WARN = "\033[33mWARNING\033[0m"


def get_page_token(session, user_token, page_id):
    """Exchange a system user token for a Page Access Token."""
    r = session.get(
        f"{BASE_URL}/{page_id}",
        params={"fields": "access_token,name", "access_token": user_token},
        timeout=15,
    ).json()
    if "error" in r:
        return user_token, page_id
    return r.get("access_token", user_token), r.get("name", page_id)


def probe_metric(session, metric_name, token, object_id=None, period=None):
    """
    Call /{object_id}/insights?metric={metric_name} and return the raw result.
    object_id defaults to the post id (post-level). Pass PAGE_ID + period='day'
    to probe page-level metrics.
    """
    oid = object_id or POST_ID
    url = f"{BASE_URL}/{oid}/insights"
    params = {"metric": metric_name, "access_token": token}
    if period:
        params["period"] = period
    try:
        r = session.get(url, params=params, timeout=15)
        return {"status_code": r.status_code, "body": r.json()}
    except Exception as exc:
        return {"status_code": -1, "body": {"error": str(exc)}}


# Page-level candidate metrics (mirrors configs/facebook.yaml page_insights).
# Probe with: python ops/probe_facebook_post_metrics.py --page
PAGE_CANDIDATES = {
    "Page Impressions / Views": [
        "page_media_view",
        "page_impressions",
        "page_impressions_unique",
        "page_impressions_paid",
        "page_impressions_paid_unique",
        "page_impressions_organic",
        "page_impressions_organic_unique",
        "page_impressions_viral",
        "page_impressions_viral_unique",
    ],
    "Page Post Impressions": [
        "page_posts_impressions",
        "page_posts_impressions_unique",
        "page_posts_impressions_paid",
        "page_posts_impressions_paid_unique",
        "page_posts_impressions_organic",
        "page_posts_impressions_organic_unique",
        "page_posts_impressions_viral",
        "page_posts_impressions_viral_unique",
    ],
    "Page Engagement / Actions": [
        "page_post_engagements",
        "page_total_actions",
        "page_actions_post_reactions_like_total",
        "page_actions_post_reactions_love_total",
        "page_actions_post_reactions_wow_total",
        "page_actions_post_reactions_haha_total",
        "page_actions_post_reactions_sorry_total",
        "page_actions_post_reactions_anger_total",
        "page_actions_post_reactions_total",
    ],
    "Page Follows / Fans": [
        "page_follows",
        "page_daily_follows",
        "page_daily_follows_unique",
        "page_daily_unfollows",
        "page_daily_unfollows_unique",
        "page_fans",
        "page_fan_adds",
        "page_fan_removes",
    ],
    "Page Views": [
        "page_views_total",
    ],
    "Page Video": [
        "page_video_views",
        "page_video_views_paid",
        "page_video_views_organic",
        "page_video_complete_views_30s",
    ],
}


def interpret(result, metric_name):
    """Return (accepted, detail_string)."""
    body = result["body"]
    if "error" in body:
        code = body["error"].get("code", "?")
        subcode = body["error"].get("error_subcode", "")
        msg = body["error"].get("message", "")
        detail = f"code={code}" + (f"/{subcode}" if subcode else "") + f": {msg}"
        return False, detail
    data = body.get("data", [])
    if not data:
        return True, "(accepted but returned empty data)"
    values = data[0].get("values", [])
    name = data[0].get("name", metric_name)
    total = sum(
        v.get("value", 0) for v in values if isinstance(v.get("value"), (int, float))
    )
    return True, f"name={name}, {len(values)} period(s), total={total}"


def main():
    page_mode = "--page" in sys.argv or "--level=page" in sys.argv

    if not ACCESS_TOKEN:
        print("[ERROR] FACEBOOK_ACCESS_TOKEN not set in .env or environment.")
        sys.exit(1)

    try:
        import requests
    except ImportError:
        print("[ERROR] requests library not installed. Run: pip install requests")
        sys.exit(1)

    session = requests.Session()

    level = "PAGE" if page_mode else "POST"
    candidates = PAGE_CANDIDATES if page_mode else CANDIDATES
    # Page metrics require period=day; post metrics do not.
    period = "day" if page_mode else None
    object_id = PAGE_ID if page_mode else POST_ID

    print(f"\nFacebook Insights Metrics Probe ({level}-level)")
    print(f"  API version : {API_VERSION}")
    print(f"  Post ID     : {POST_ID}")
    print(f"  Page ID     : {PAGE_ID}")
    print(
        f"  Probing     : /{object_id}/insights" + (" (period=day)" if period else "")
    )
    print(f"  User token  : {ACCESS_TOKEN[:12]}...{ACCESS_TOKEN[-6:]}\n")

    # Exchange system user token -> Page Access Token
    page_token, page_name = get_page_token(session, ACCESS_TOKEN, PAGE_ID)
    print(f"  Page name   : {page_name}")
    if page_token != ACCESS_TOKEN:
        print(f"  Page token  : {page_token[:12]}...{page_token[-6:]} (exchanged OK)\n")
    else:
        print(f"  Page token  : same as user token (no access_token field returned)\n")

    accepted = []
    rejected = []

    for category, metrics in candidates.items():
        print(f"-- {category} {'-' * max(0, 60 - len(category))}")
        for metric_name in metrics:
            result = probe_metric(
                session, metric_name, page_token, object_id=object_id, period=period
            )
            ok, detail = interpret(result, metric_name)
            status = PASS if ok else FAIL
            print(f"  [{status}] {metric_name:<45} {detail}")
            if ok:
                accepted.append(metric_name)
            else:
                rejected.append(metric_name)
            time.sleep(0.25)
        print()

    print("=" * 70)
    print(f"  Accepted : {len(accepted)}")
    for m in accepted:
        print(f"    + {m}")
    print(f"  Rejected : {len(rejected)}")
    print()

    if not page_mode:
        if "post_link_clicks" in accepted:
            print(
                "  NOTE: post_link_clicks IS accepted - consider re-adding to pipeline."
            )
        else:
            print(
                "  NOTE: post_link_clicks is NOT available at the post insights level."
            )
            link_alts = [
                m for m in accepted if "click" in m.lower() or "link" in m.lower()
            ]
            if link_alts:
                print("  Link-click alternatives that ARE accepted:")
                for m in link_alts:
                    print(f"    + {m}")
            else:
                print("  No link-click related metrics were accepted at this endpoint.")


if __name__ == "__main__":
    main()
