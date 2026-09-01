#!/usr/bin/env python3
"""Derive 25 interesting facts from the profile database.

This is an ad hoc operational utility, not part of the profile runtime.
Results are written to a repo-root text file for quick review and sharing.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from google.cloud import bigquery

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_PATH = REPO_ROOT / 'profile_facts.txt'
DEFAULT_PROJECT_ID = 'bi-data-391216'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Write 25 profile database factoids to a text file.')
    parser.add_argument('--project', default=DEFAULT_PROJECT_ID, help='BigQuery project id')
    parser.add_argument('--output', default=str(DEFAULT_OUTPUT_PATH), help='Output file path')
    parser.add_argument('--credentials', default=None, help='Optional GOOGLE_APPLICATION_CREDENTIALS path override')
    return parser.parse_args()


args = parse_args()
OUTPUT_PATH = Path(args.output)

if args.credentials:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.credentials

c = bigquery.Client(project=args.project)
f = OUTPUT_PATH.open('w', encoding='utf-8')

def q(n, label, sql):
    f.write(f'\n{n}. {label}\n')
    try:
        for row in c.query(sql).result():
            d = dict(row)
            for k, v in d.items():
                if isinstance(v, float):
                    f.write(f'   {k}: {v:,.1f}\n')
                elif isinstance(v, int):
                    f.write(f'   {k}: {v:,}\n')
                else:
                    f.write(f'   {k}: {v}\n')
    except Exception as e:
        f.write(f'   ERROR: {str(e)[:200]}\n')
    f.flush()

q(1, 'Total universe size', "SELECT COUNT(*) AS total_profiles, COUNTIF(email IS NOT NULL) AS known_with_email, COUNTIF(email IS NULL) AS anonymous FROM profile_data.profile_core")

q(2, 'How many people can we reach on each ad platform?', """
    SELECT
        COUNTIF(identifier_type='fbp') AS meta_pixel,
        COUNTIF(identifier_type='gcl_au') AS google_ads,
        COUNTIF(identifier_type='fbc') AS facebook_click,
        COUNTIF(identifier_type='dmd_tag') AS dmd_hcp,
        COUNTIF(identifier_type='aim_tag_id') AS aim_hcp
    FROM (SELECT DISTINCT bn_id, identifier_type FROM identity_hub_data.bn_id_xref
          WHERE identifier_type IN ('fbp','gcl_au','fbc','dmd_tag','aim_tag_id'))
""")

q(3, 'Top 10 conditions by audience size', """
    SELECT preferred_condition.label AS condition, COUNT(*) AS profiles
    FROM profile_data.profile_core WHERE preferred_condition IS NOT NULL
    GROUP BY condition ORDER BY profiles DESC LIMIT 10
""")

q(4, 'HCP specialty breakdown (top 10)', """
    SELECT specialty.label AS specialty, COUNT(*) AS hcps
    FROM profile_data.profile_core
    WHERE is_hcp = TRUE AND specialty IS NOT NULL
    GROUP BY specialty ORDER BY hcps DESC LIMIT 10
""")

q(5, 'What percentage of visitors become known?', """
    SELECT
        COUNT(*) AS total,
        COUNTIF(email IS NOT NULL) AS known,
        ROUND(100.0 * COUNTIF(email IS NOT NULL) / COUNT(*), 1) AS conversion_rate_pct
    FROM profile_data.profile_core
""")

q(6, 'Average identifiers per person (graph richness)', """
    SELECT
        ROUND(AVG(id_count), 1) AS avg_identifiers,
        MAX(id_count) AS max_identifiers,
        APPROX_QUANTILES(id_count, 100)[OFFSET(50)] AS median_identifiers
    FROM (SELECT bn_id, COUNT(*) AS id_count FROM identity_hub_data.bn_id_xref GROUP BY bn_id)
""")

q(7, 'Email engagement: what fraction of email recipients are active on the web?', """
    SELECT
        COUNT(*) AS total_with_email,
        COUNTIF(pe.total_sessions > 0) AS also_visit_web,
        ROUND(100.0 * COUNTIF(pe.total_sessions > 0) / COUNT(*), 1) AS web_crossover_pct
    FROM profile_data.profile_core pc
    JOIN profile_data.profile_engagement pe ON pc.bn_id = pe.bn_id
    WHERE pc.email IS NOT NULL
""")

q(8, 'Engagement tier by condition (which conditions have the most engaged audiences?)', """
    SELECT
        pc.preferred_condition.label AS condition,
        COUNTIF(pe.engagement_tier = 'high') AS high,
        COUNTIF(pe.engagement_tier = 'medium') AS medium,
        ROUND(100.0 * COUNTIF(pe.engagement_tier IN ('high','medium')) / COUNT(*), 1) AS engaged_pct
    FROM profile_data.profile_core pc
    JOIN profile_data.profile_engagement pe ON pc.bn_id = pe.bn_id
    WHERE pc.preferred_condition IS NOT NULL
    GROUP BY condition
    HAVING COUNT(*) > 5000
    ORDER BY engaged_pct DESC LIMIT 10
""")

q(9, 'How many HCPs can we reach via web ads vs email only?', """
    SELECT
        COUNT(*) AS total_hcps,
        COUNTIF(pc.email IS NOT NULL) AS hcps_with_email,
        COUNTIF(pe.total_sessions > 0) AS hcps_with_web_sessions,
        COUNTIF(pc.bn_id IN (SELECT bn_id FROM identity_hub_data.bn_id_xref WHERE identifier_type = 'fbp')) AS hcps_on_meta,
        COUNTIF(pc.bn_id IN (SELECT bn_id FROM identity_hub_data.bn_id_xref WHERE identifier_type = 'gcl_au')) AS hcps_on_google
    FROM profile_data.profile_core pc
    JOIN profile_data.profile_engagement pe ON pc.bn_id = pe.bn_id
    WHERE pc.is_hcp = TRUE
""")

q(10, 'HCP practice state distribution (top 10)', """
    SELECT practice_state, COUNT(*) AS hcps
    FROM profile_data.profile_core
    WHERE is_hcp = TRUE AND practice_state IS NOT NULL
    GROUP BY practice_state ORDER BY hcps DESC LIMIT 10
""")

q(11, 'Most common first-touch acquisition channels', """
    SELECT COALESCE(acquisition_source, '(unknown)') AS channel, COUNT(*) AS profiles
    FROM profile_data.profile_core
    WHERE acquisition_source IS NOT NULL
    GROUP BY channel ORDER BY profiles DESC LIMIT 10
""")

q(12, 'Content affinity: which condition sites get the most cross-condition visitors?', """
    SELECT content_condition, COUNT(DISTINCT bn_id) AS unique_visitors,
           ROUND(AVG(affinity_score), 2) AS avg_affinity
    FROM profile_data.profile_content_affinity
    GROUP BY content_condition ORDER BY unique_visitors DESC LIMIT 10
""")

q(13, 'What devices do our audience use?', """
    SELECT COALESCE(primary_device, '(unknown)') AS device, COUNT(*) AS profiles
    FROM profile_data.profile_engagement
    WHERE primary_device IS NOT NULL
    GROUP BY device ORDER BY profiles DESC
""")

q(14, 'Best time to send emails (preferred hour distribution)', """
    SELECT preferred_email_hour AS hour_utc, COUNT(*) AS profiles
    FROM profile_data.profile_engagement
    WHERE preferred_email_hour IS NOT NULL
    GROUP BY hour_utc ORDER BY hour_utc
""")

q(15, 'Anonymous visitors with high engagement (conversion opportunities)', """
    SELECT COUNT(*) AS anon_high_engagement
    FROM profile_data.profile_core pc
    JOIN profile_data.profile_engagement pe ON pc.bn_id = pe.bn_id
    WHERE pc.email IS NULL
      AND pe.engagement_tier IN ('high', 'medium')
""")

q(16, 'How many profiles have multi-condition interest?', """
    SELECT multi_conditions, COUNT(*) AS profiles FROM (
        SELECT bn_id, COUNT(DISTINCT content_condition) AS multi_conditions
        FROM profile_data.profile_content_affinity
        GROUP BY bn_id
    )
    WHERE multi_conditions > 1
    GROUP BY multi_conditions ORDER BY multi_conditions LIMIT 10
""")

q(17, 'Repeat engagers vs one-time visitors', """
    SELECT
        COUNTIF(pe.total_sessions >= 3 AND pe.email_open_count >= 2) AS repeat_engagers,
        COUNTIF(pe.total_sessions = 1 AND pe.email_open_count <= 1) AS one_timers,
        COUNT(*) AS total
    FROM profile_data.profile_engagement pe
    WHERE pe.total_sessions > 0
""")

q(18, 'Geographic distribution of known profiles (top countries)', """
    SELECT country, COUNT(*) AS profiles
    FROM profile_data.profile_core
    WHERE country IS NOT NULL AND email IS NOT NULL
    GROUP BY country ORDER BY profiles DESC LIMIT 10
""")

q(19, 'Consent status breakdown', """
    SELECT
        COUNTIF(consent_status = 'granted') AS granted,
        COUNTIF(consent_status = 'denied') AS denied,
        COUNTIF(consent_status = 'pending' OR consent_status IS NULL) AS pending_or_unknown,
        COUNT(*) AS total
    FROM profile_data.profile_core
""")

q(20, 'How many days do retained users stay active?', """
    SELECT
        ROUND(AVG(days_retained), 0) AS avg_days_retained,
        APPROX_QUANTILES(days_retained, 100)[OFFSET(50)] AS median_days,
        MAX(days_retained) AS max_days
    FROM profile_data.profile_engagement
    WHERE days_retained > 0
""")

q(21, 'Newsletter subscription rates by condition site', """
    SELECT pref.site_domain, COUNT(DISTINCT pp.bn_id) AS subscribers
    FROM profile_data.profile_preferences pp,
         UNNEST(pp.newsletter_preferences) AS pref
    WHERE pref.is_subscribed = TRUE
    GROUP BY pref.site_domain ORDER BY subscribers DESC LIMIT 10
""")

q(22, 'BuddyPress community engagement (forum power users)', """
    SELECT
        COUNTIF(bp_activity_count > 0) AS forum_active,
        COUNTIF(bp_activity_count >= 10) AS power_users,
        COUNTIF(bp_is_group_admin = TRUE) AS group_admins,
        COUNTIF(has_paid_subscription = TRUE) AS paid_members
    FROM profile_data.profile_engagement
""")

q(23, 'Identity resolution quality: how many profiles have full identity chains?', """
    SELECT
        COUNTIF(email IS NOT NULL AND pc.bn_id IN (SELECT bn_id FROM identity_hub_data.bn_id_xref WHERE identifier_type = 'bnfpvid')) AS email_plus_browser,
        COUNTIF(is_hcp = TRUE AND pc.bn_id IN (SELECT bn_id FROM identity_hub_data.bn_id_xref WHERE identifier_type = 'bnfpvid')) AS hcp_plus_browser,
        COUNT(*) AS total
    FROM profile_data.profile_core pc
""")

q(24, 'Data exceptions: top quality issues to fix', """
    SELECT exception_type, COUNT(*) AS n
    FROM profile_data.profile_exceptions
    GROUP BY exception_type ORDER BY n DESC
""")

q(25, 'Growth potential: anonymous profiles with condition data (ready for personalization)', """
    SELECT
        COUNTIF(email IS NULL AND preferred_condition IS NOT NULL) AS anon_with_condition,
        COUNTIF(email IS NULL AND preferred_condition IS NOT NULL AND hcp_status = TRUE) AS anon_hcps_with_condition,
        COUNTIF(email IS NULL) AS total_anonymous
    FROM profile_data.profile_core
""")

f.write('\nDONE\n')
f.close()
print(f'Results written to {OUTPUT_PATH}')
