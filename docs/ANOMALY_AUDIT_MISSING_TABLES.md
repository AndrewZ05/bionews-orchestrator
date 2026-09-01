# Audits -- Missing Tables (config drift)

Tables declared `active: true` in their source YAML but ABSENT from BigQuery as of
the 2026-06-10 audit run. BOTH audits (freshness and anomaly) now SKIP these (no
row written, count surfaced in the email/summary as "N known-missing tables
skipped"). They are listed here so the drift is visible and can be reconciled:
either mark the resource `audit: false` (the per-resource audit block in the
source YAML), or deactivate truly-dead resources in the source ETL config -- a
WordPress-ETL-owner decision, not the audit's.

All 32 are in `wordpress_data` (WordPress plugin tables: symposium, rtmedia, pms,
blc, elementor `e_submissions`, etc. -- groups: symposium, rtmedia, pms, others).

| # | dataset | table |
|---|---|---|
| 1 | wordpress_data | wordpress_ai_statistics |
| 2 | wordpress_data | wordpress_bionews_ab_experiments |
| 3 | wordpress_data | wordpress_blc_links |
| 4 | wordpress_data | wordpress_blc_synch |
| 5 | wordpress_data | wordpress_db7_forms |
| 6 | wordpress_data | wordpress_e_submissions |
| 7 | wordpress_data | wordpress_e_submissions_actions_log |
| 8 | wordpress_data | wordpress_email_log |
| 9 | wordpress_data | wordpress_eo_events |
| 10 | wordpress_data | wordpress_hdflvvideoshare |
| 11 | wordpress_data | wordpress_lgp_posts |
| 12 | wordpress_data | wordpress_ml_adverts_clicks |
| 13 | wordpress_data | wordpress_mlw_quizzes |
| 14 | wordpress_data | wordpress_modal_survey_participants_details |
| 15 | wordpress_data | wordpress_pms_paymentmeta |
| 16 | wordpress_data | wordpress_pms_payments |
| 17 | wordpress_data | wordpress_pmxe_exports |
| 18 | wordpress_data | wordpress_rank_math_analytics_objects |
| 19 | wordpress_data | wordpress_rmp_analytics |
| 20 | wordpress_data | wordpress_rt_rtm_activity |
| 21 | wordpress_data | wordpress_rt_rtm_media |
| 22 | wordpress_data | wordpress_rt_rtm_media_interaction |
| 23 | wordpress_data | wordpress_rt_rtm_media_meta |
| 24 | wordpress_data | wordpress_symposium_cats |
| 25 | wordpress_data | wordpress_symposium_comments |
| 26 | wordpress_data | wordpress_symposium_friends |
| 27 | wordpress_data | wordpress_symposium_mail |
| 28 | wordpress_data | wordpress_symposium_styles |
| 29 | wordpress_data | wordpress_symposium_subs |
| 30 | wordpress_data | wordpress_symposium_topics |
| 31 | wordpress_data | wordpress_tec_series_relationships |
| 32 | wordpress_data | wordpress_yuzoviews |

## Per-resource audit flag (IMPLEMENTED)

The requested per-table audit flag now exists. On any resource in a source YAML:

```yaml
resources:
  some_table:
    audit: false                 # exclude from BOTH audits
  other_table:
    audit:
      enabled: true              # default
      date_column: last_seen     # override the freshness/anomaly date column
```

Resolved by `shared/freshness_audit/yaml_target_loader.py` (shared by both
audits). To permanently silence a table in this list, mark it `audit: false` in
`configs/wordpress.yaml` -- safer than changing `active`, which affects the ETL.
The skip-if-missing behavior remains for tables not yet flagged.
