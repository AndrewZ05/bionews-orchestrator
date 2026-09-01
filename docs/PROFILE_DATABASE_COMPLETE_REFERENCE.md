# Profile Database — Complete Reference

**Project:** `bi-data-391216` · **Dataset:** `profile_data`
**Row counts verified against production 2026-08-18.** Schema updated to
**v6.7** on 2026-08-18; the v6.7 acquisition-date columns are documented below
but their fill rates have not been re-measured against production.

This is the single entry point. It consolidates what was previously spread across
~20 documents, several with stale figures (the prior inventory reported 3,460,572
`profile_core` rows against a live 5,437,115).

**Companion documents — still current, not duplicated here:**

| Document | Use it for |
|---|---|
| [PROFILE_DATABASE_BUSINESS_QUERIES.md](PROFILE_DATABASE_BUSINESS_QUERIES.md) | The 25 business queries. All 25 re-validated 2026-07-30. |
| [PROFILE_DB_EXPLAINED.md](PROFILE_DB_EXPLAINED.md) | Build modes, empty shells, condition provenance |
| [PROFILE_DATABASE_DATA_DICTIONARY.md](PROFILE_DATABASE_DATA_DICTIONARY.md) | **Every table, every column, every definition, and which build step writes it. Auto-generated.** |
| [PROFILE_DATABASE_EXPLAINED.md](PROFILE_DATABASE_EXPLAINED.md) | **How the data behaves: tier1 vs tier2, multi-valued roles, date grades, bot opens.** Read before querying. |
| [../sql/profile_database_query_cookbook.sql](../sql/profile_database_query_cookbook.sql) | Runnable queries, validated against production. |
| [PROFILE_DATABASE_SCHEMA_CONTRACT.md](PROFILE_DATABASE_SCHEMA_CONTRACT.md) | Table-level contract and build steps. Auto-generated. |
| [PROFILE_DATABASE_LINEAGE.md](PROFILE_DATABASE_LINEAGE.md) | Which SQL step writes which table. Auto-generated. |
| [PROFILE_DATABASE_OPERATOR_GUIDE.md](PROFILE_DATABASE_OPERATOR_GUIDE.md) | Running builds, recovery |
| [PROFILE_CREATED_AT_RUNBOOK.md](PROFILE_CREATED_AT_RUNBOOK.md) | Which build modes are safe |

---

# PART 1 — Where `can_personalize` lives

**`profile_data.profile_contactability`** — a view, defined at
`sql/profile_database_views.sql:965`. The flag itself is at **line 995**:

```sql
CREATE OR REPLACE VIEW {view_dataset}.profile_contactability AS
SELECT
    ...
    (pc.email IS NOT NULL
        AND pc.communication_opt_in = TRUE
        AND pc.consent_status NOT IN ('withdrawn','suppressed','denied')) AS can_email_market,
    (pc.preferred_condition IS NOT NULL
        AND pc.consent_status NOT IN ('withdrawn','suppressed','denied')) AS can_personalize,
    COALESCE(pe.consent_analytics, pc.tracking_consent, FALSE) AS can_analytics_track,
    COALESCE(pe.consent_advertising, FALSE)                   AS can_advertise,
```

The four capability flags all live on this one view:

| Flag | Requires |
|---|---|
| `can_email_market` | email + `communication_opt_in` + not denied |
| `can_personalize` | a condition + not denied |
| `can_analytics_track` | analytics consent or `tracking_consent` |
| `can_advertise` | advertising consent |

`can_personalize` is also re-exposed (not redefined) at lines 1054 and 1164 in
downstream views.

## ⚠️ Read this before using `can_personalize`

**The flag is currently unreliable, and the reason is a live defect.**

It tests `NOT IN ('withdrawn','suppressed','denied')`. The value `'pending'` is
not in that list, so `'pending'` passes as consented. Measured:

```
consent_status   rows        can_personalize
pending          4,144,768         3,138,549
granted            998,643             4,479
denied             293,704                 0
                              ----------------
                 5,437,115         3,143,028
```

**3,138,549 of the 3,143,028 profiles marked `can_personalize` qualify only
because `'pending'` is absent from the denial list. Just 4,479 qualify on an
actual `'granted'` value.**

Root cause: the rebuild path seeds `'pending'`
(`sql/populate_identity_core.sql:472`) while refresh and reconcile seed
`'denied'`. Separately, OneTrust enrichment only matches `= 'denied'`
(`sql/enrich_mailchimp_ga4.sql:444`), so all 4,144,768 `'pending'` rows are
permanently unreachable by consent enrichment.

**Until this is fixed, treat `can_personalize` as "not explicitly denied" — not
as "consented."** For a defensible consented population, filter explicitly:

```sql
-- Trustworthy today. Requires a real granted value.
SELECT COUNT(*)
FROM `bi-data-391216.profile_data.profile_core`
WHERE consent_status = 'granted'
  AND preferred_condition IS NOT NULL;
```

Full analysis and remediation options are in the plan file; this is item A1.

---

# PART 2 — Complete table inventory

## Base tables (16)

| Table | Rows | Cols | Grain | What it is |
|---|---:|---:|---|---|
| `site_events` | 64,601,371 | 21 | N per `bn_id` | Event-level web timeline |
| `profile_identifiers` | 29,275,664 | 9 | N per `bn_id` | Every identifier mapped to a person |
| `profile_ad_attribution` | 6,088,824 | 7 | N per `bn_id` | Ad click IDs by platform |
| **`profile_core`** | **6,083,585** | **131** | **1 per `bn_id`** | **The canonical person row** |
| `profile_engagement` | 6,051,103 | 72 | 1 per `bn_id` | Engagement rollup |
| `profile_content_affinity` | 2,883,933 | 8 | N per `bn_id` | Condition affinity from reading |
| `profile_survey_data` | 207,214 | 12 | N per `bn_id` | Normalized survey answers |
| `profile_segment_tags` | 127,821 | 6 | N per `bn_id` | Governed tags |
| `profile_zero_party` | 11,760 | 10 | N per `bn_id` | Self-declared answers |
| `profile_preferences` | 5,454 | 4 | 1 per `bn_id` | Newsletter + forum settings |
| `profile_lookup` | 291 | 8 | — | Config/lookup values |
| `conditions_dict` | 88 | 12 | — | Condition vocabulary (MeSH) |
| `subtypes_dict` | 24 | 11 | — | Subtype vocabulary |
| `symptoms_dict` | 23 | 13 | — | Symptom vocabulary (HPO) |
| `treatments_dict` | 17 | 13 | — | Treatment vocabulary (RxNorm) |
| `dictionary_meta` | 4 | 8 | — | Dictionary provenance |

## Views (22)

**Governed analytic surfaces — prefer these:**

| View | Cols | PII | Use for |
|---|---:|---|---|
| `profile_current_safe` | 76 | Masked | **Default analytic surface** |
| `profile_contactability` | 31 | Email | Consent + reachability (see Part 1 caveat) |
| `profile_marketing_audience` | 36 | Name + email | Campaign lists |
| `profile_analytics_audience` | 49 | Masked | Analytics/BI |
| `profile_ops_audience` | 29 | Masked | Operational reporting |
| `profile_current` | 167 | **Full PII** | Privileged only |

**Pre-filtered persona audiences (76 cols each):**
`profile_audience_hcp` · `profile_audience_patients_confirmed` ·
`profile_audience_caregivers` · `profile_audience_high_engagement`

**Operational / QA:**
`profile_coverage` (5) · `profile_exceptions` (3) · `profile_explain` (73) ·
`profile_release_status` (33) · `profile_build_performance` (37) ·
`profile_signals` (17) · `profile_roles` (5) · `profile_events` (10) ·
`profile_consent` (9) · `profile_engagement_monthly` (5) ·
`profile_newsletter_preferences` (9) · `profile_forum_settings` (12)

---

# PART 3 — Column reference

## `profile_core` — 131 columns

The canonical row. One per `bn_id`. Grouped by purpose below; for the
exhaustive column-by-column list with definitions and the build step that
populates each table, see
[PROFILE_DATABASE_DATA_DICTIONARY.md](PROFILE_DATABASE_DATA_DICTIONARY.md).

### v6.7 — acquisition dates (`source_created_at`)

Three columns added 2026-08. They exist because `created_at` is **the date our
pipeline first observed the profile, not the date the person arrived**, and
charting it produces growth curves that reflect pipeline history rather than
audience history.

| Column | Meaning |
|---|---|
| `source_created_at` | Best-known true acquisition date, recovered from the source system. Write-once. |
| `source_created_basis` | What kind of date it is. This is the important one. |
| `source_created_set_at` | When the build wrote it. Audit only. |

`source_created_basis` grades the date, and the grades are not interchangeable:

| Grade | Values | Safe for growth reporting? |
|---|---|---|
| Registration | `mailchimp_signup`, `mailchimp_warehouse`, `mailchimp_archived`, `wordpress_registered`, `survey_response`, `npi_enumeration`, `hub:*` | **Yes** — a real signup event |
| First sighting | `cookie_creation` | **No** — a browser's first appearance, severely survivorship-biased |
| Upper bound | `email_activity_upper_bound` | **No** — the real date is some unknown earlier date |

**Use `profile_growth_known`, not raw dates.** Cookie survivorship makes naive
cookie-birth charts report roughly a 396x rise in anonymous audience over a span
when real GA4 traffic grew ~45%. The view enforces the correct filters
structurally; see [../sql/profile_growth_views_v2.sql](../sql/profile_growth_views_v2.sql).

**Identity**

| Column | Type | Notes |
|---|---|---|
| `bn_id` | STRING | Primary key. `BN_` + base64url(SHA256)[:16] |
| `bionews_uk` | STRING | SSO user key. Immutable |
| `email`, `email_hash` | STRING | Email + hash |
| `npi_number` | STRING | Verified clinician ID. Immutable |
| `ga_user_id` | STRING | GA user ID |
| `cluster_tier` | STRING | `tier1` (strong) / `tier2` (cookie) |
| `cluster_size` | INT64 | Identifiers in this cluster |
| `identity_confidence_score` | INT64 | tier1=90, tier2=50, else 10 |
| `is_shared_workstation`, `is_suspicious` | BOOL | Quality flags |

**Health condition** — see Part 4 for the critical provenance caveat

| Column | Type | Filled |
|---|---|---:|
| `preferred_condition` | STRUCT | 3,145,389 |
| `preferred_condition_normalized` | STRUCT | 3,144,790 |
| `preferred_condition_source` | STRING | — |
| `preferred_condition_confidence` | FLOAT64 | — |
| `condition_focus` | ARRAY&lt;STRUCT&gt; | All conditions |
| `condition_subtype` | STRING | 15,432 |
| `diagnosis_stage` | STRING | 17,328 |
| `caregiver_condition`, `family_condition` | STRUCT | — |
| `follow_conditions` | ARRAY&lt;STRUCT&gt; | — |

STRUCT fields: `condition_key`, `label`, `mesh_id`.

**Roles (v6.5)** — five independent booleans, each with a lineage triple:

`is_patient` · `is_hcp` · `is_caregiver` · `is_family_or_friend` · `is_other`,
each with `_source`, `_confidence`, `_updated_at`.

Source-tier confidence: npi/survey/confirmed = 1.0 · buddypress = 0.8 ·
mailchimp/gravity = 0.7 · inferred = 0.5. `is_other` stays NULL until a positive
signal.

**Consent + contact**

| Column | Type | Notes |
|---|---|---|
| `consent_status` | STRING | `granted`/`denied`/`pending` — **see Part 1** |
| `communication_opt_in` | BOOL | Email opt-in |
| `tracking_consent` | BOOL | Analytics consent |
| `country` | STRING | ISO-2. Use this, not `address_country` |
| `address_country` | STRING | Holds `'United States'`, not `'US'` |

**Lifecycle**

| Column | Type | Notes |
|---|---|---|
| `created_at` | TIMESTAMP | True genesis. 4,459 distinct dates from 2013-01-06 |
| `last_active_at` | TIMESTAMP | Most recent activity |
| `profile_updated_at` | TIMESTAMP | Last pipeline write |
| `profile_stage` | STRING | Lifecycle stage |

## Satellite tables

**`profile_identifiers`** (26.0M) — `bn_id`, `identifier_type`,
`identifier_value`, `source_system`, `confidence`, `first_seen`, `last_seen`,
`is_primary`, `loaded_at`

> `first_seen` here is the **only** rebuild-surviving genesis source and is what
> repaired `profile_core.created_at`.

**`profile_engagement`** (5.4M, 71 cols) — `mailchimp_status`,
`mailchimp_member_rating`, `mailchimp_vip`, `email_open_count`,
`email_click_count`, `last_email_open`, `last_email_click`, `first_seen_web`,
`last_seen_web`, `total_sessions`, `total_pageviews`, `total_form_submissions`,
`total_ad_impressions`, `total_ad_clicks`, `total_video_views`,
`total_file_downloads`, `total_comments`, `total_forum_interactions`,
`avg_scroll_depth`, `avg_session_duration_sec`, `content_clusters`

**`profile_content_affinity`** (2.9M) — `content_condition`, `pageview_count`,
`active_days`, `first_viewed`, `last_viewed`, `affinity_score`

> Presence here yields a condition **100% of the time**. See Part 4.

**`site_events`** (15.3M) — `event_id`, `bn_id`, `bnfpvid`, `client_id`,
`event_name`, `event_category`, `event_timestamp`, `session_id`, `site_domain`,
`page_path`, `page_title`, `event_label`, `event_value`, `event_detail`,
`traffic_source`, `traffic_medium`, `traffic_campaign`, `device_category`,
`user_agent`, `consent_status`, `loaded_at`

**`profile_zero_party`** (11K) — `interaction_type`, `interaction_id`,
`question_text`, `answer_value`, `answer_score`, `site_domain`, `page_path`,
`responded_at`. Highest-trust self-declared data.

**`profile_survey_data`** (209K) — `survey_id`, `response_id`, `cde_id`,
`field_name`, `value_text`, `value_numeric`, `value_code`, `value_array`,
`response_submitted_at`, `mapping_confidence`

**`profile_ad_attribution`** (6.1M) — `platform`, `click_id_type`, `click_id`,
`first_seen`, `last_seen`

**`profile_segment_tags`** (126K) — `tag_category`, `tag_value`, `source`,
`first_seen`

**`profile_preferences`** (5.5K) — `newsletter_preferences`
ARRAY&lt;STRUCT&gt;, `forum_settings` STRUCT

---

# PART 4 — How to use this data

## The tiering that matters most

The headline "5.4M profiles" is accurate but does not mean 5.4M customers:

```
TOTAL ............................... 5,437,115   100.0%
  condition signal .................. 3,145,389    57.9%
    ├─ self-declared ................   399,185     7.3%   conf 0.7-0.89
    └─ inferred from reading ........ 2,746,204    50.5%   conf 0.5
  contactable (email) ...............   785,429    14.4%
  verified clinicians (NPI) .........   360,156     6.6%
  EMPTY SHELLS ...................... 1,908,096    35.1%
```

**Describe it as:** "5.4M tracked identities — 785K contactable, 360K verified
clinicians, 3.1M with a condition signal (~400K self-declared)."
**Not** "5.4M customer profiles."

## Four things that will bite you

**1. Conditions are 87.3% inferred, not declared.**

```
content_affinity  2,746,204 (87.3%)  conf 0.5   <- inferred from browsing
mailchimp_*         399,185 (12.7%)  conf 0.89  <- actually told us
```

Inference cannot distinguish a patient from a caregiver, nurse, or researcher.
Say "condition interest," not "patients." To restrict to declared only:

```sql
SELECT COUNT(*) AS declared_condition_profiles
FROM `bi-data-391216.profile_data.profile_core`
WHERE preferred_condition_source LIKE 'mailchimp%';
-- 399,185 as of 2026-07-30
```

**2. Conditions are STRUCTs.** `CAST(preferred_condition AS STRING)` fails. Use
`.label`, `.condition_key`, or `.mesh_id`. Prefer
`preferred_condition_normalized.condition_key` — the raw `label` carries both
`'ms'` and `'Multiple Sclerosis'` for the same condition, fragmenting audiences.

**3. Empty shells are live audience, not junk.** 916,057 of the 1.9M were active
in the last 30 days, and all but 17 are stitched across multiple identifiers.
They have zero `profile_content_affinity` rows — that is exactly why they have no
condition. Since affinity yields a condition 100% of the time (2,849,624 →
2,849,624), shells are a **content-attribution gap, not an identity failure**.

**4. Column gotchas.**

| Wrong | Right |
|---|---|
| `address_country = 'US'` (0 rows) | `country = 'US'` |
| `can_advertise` on `profile_marketing_audience` | it is on `profile_contactability` |
| `acquisition_source` on a safe view | only on `profile_core` / `profile_current` |
| `TIMESTAMP_SUB(..., INTERVAL 6 MONTH)` | `DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)` |
| `unsubscribed_at` | never populated (0 of 10,490) |

## Choosing a surface

```
Analytics / BI ............ profile_current_safe        (masked)
Campaign lists ............ profile_marketing_audience  (name + email)
Consent decisions ......... profile_contactability      (see Part 1)
Persona targeting ......... profile_audience_*          (pre-filtered)
Privileged / PII .......... profile_core, profile_current
```

Default to `profile_current_safe`. Escalate only when the task needs PII.

## Joining

Everything joins on `bn_id`.

```sql
SELECT p.bn_id, p.preferred_condition_normalized.label AS condition,
       e.total_pageviews, c.affinity_score
FROM `bi-data-391216.profile_data.profile_current_safe` p
LEFT JOIN `bi-data-391216.profile_data.profile_engagement` e USING (bn_id)
LEFT JOIN `bi-data-391216.profile_data.profile_content_affinity` c USING (bn_id)
WHERE p.preferred_condition_normalized.condition_key = 'multiple_sclerosis';
```

Satellites are N-per-`bn_id` — aggregate before joining, or rows fan out.

---

# PART 5 — Business queries

**25 production-verified queries:
[PROFILE_DATABASE_BUSINESS_QUERIES.md](PROFILE_DATABASE_BUSINESS_QUERIES.md)**

All 25 were re-validated against production on 2026-07-30: **25/25 parse and
execute**. They use governed views only, and cover:

| Section | Queries |
|---|---|
| Audience sizing (Marketing) | Q1–Q5 |
| Condition + persona targeting | Q6–Q10 |
| Engagement + retention | Q11–Q15 |
| Content + editorial | Q16–Q20 |
| Acquisition + ops | Q21–Q25 |

Additional catalogs:

- [Identity_Profile_Query_Showcase.md](Identity_Profile_Query_Showcase.md) —
  28 cross-platform identity + profile queries
- `sql/showcase/identity_profile_business_queries.sql` — runnable versions

Three quick starters, verified today:

```sql
-- Top conditions by audience size
SELECT preferred_condition_normalized.label AS condition, COUNT(*) AS profiles
FROM `bi-data-391216.profile_data.profile_core`
WHERE preferred_condition_normalized IS NOT NULL
GROUP BY condition ORDER BY profiles DESC LIMIT 10;

-- Genuinely contactable, genuinely consented
SELECT COUNT(*) AS emailable
FROM `bi-data-391216.profile_data.profile_core`
WHERE email IS NOT NULL AND communication_opt_in = TRUE
  AND consent_status = 'granted';

-- Signup cohorts by year (restored history)
SELECT EXTRACT(YEAR FROM created_at) AS yr, COUNT(*) AS profiles
FROM `bi-data-391216.profile_data.profile_core`
GROUP BY yr ORDER BY yr;
```

---

# PART 6 — Known issues

## Forum activity is not usable for an "Active Member" metric

**Verified against production 2026-08-18.** `profile_preferences.last_forum_activity`
is populated for **56 profiles**. Filtering on it yields `active_member = 4`.
The real number is **5,885** (90-day) / **2,915** (30-day) across **34,023**
forum members. Two defects in `sql/populate_newsletter_forum.sql` cause this:

1. **The activity date is not an activity date.** The CTE reads WordPress
   usermeta key `bp_latest_update`, but that value is a PHP-serialized
   `{id, content}` blob with no timestamp in it. The SQL therefore selects
   `MAX(wu.user_registered)` -- the user's *registration* date -- and labels it
   `last_forum_activity`.
2. **An SSO pre-filter empties the table.** The CTE ends with
   `WHERE pc.bionews_uk IS NOT NULL` ("only users with SSO keys have forum
   access"). Only **1,698 of 6,083,585** profiles carry an SSO key today, which
   is why `profile_preferences` holds just 5,454 rows. This is also why the
   metric cannot currently be used to measure SSO rollout: it presupposes the
   rollout it is meant to measure.

**Correct source:** `wordpress_data.wordpress_bp_activity` (198,372 rows,
14,059 distinct users, `date_recorded` current to yesterday). Note that
`user_id`, `ID` and `is_spam` are STRING in the warehouse copy, so the joins
need explicit casts.

**Status: fixed in code, not yet applied to data.**

- [../sql/populate_newsletter_forum.sql](../sql/populate_newsletter_forum.sql)
  now reads `bp_activity`, drops the SSO filter, and fills
  `subscribed_group_ids` from `wordpress_bp_groups_members` (previously
  hardcoded NULL). This only takes effect on a `rebuild`, because that is the
  only mode the step runs in.
- [../sql/backfill_forum_activity.sql](../sql/backfill_forum_activity.sql) applies
  the same correction without a rebuild. Idempotent, safe to re-run. Validated
  against production: it takes `profile_preferences` from 5,454 rows to ~34,024
  and fills 5,473 group memberships.

**Participation vs presence.** Not every activity row is engagement.
`members/new_member` is a registration event and `members/last_activity` is a
session heartbeat. Measured 2026-08-18:

| Definition | Members | Active 30d | Active 90d |
|---|---:|---:|---:|
| Presence (any activity row) | 34,023 | 2,915 | 5,885 |
| Participation (posts/replies/comments) | 22,597 | 790 | 1,695 |

`last_forum_activity` stores **participation**, since that is what the column
name claims and what "active member" should mean for growth reporting. Presence
remains derivable from `wordpress_bp_activity` directly.



Open defects that affect interpretation. Detail in
[PROFILE_DB_REVIEW_TRIAGE.md](PROFILE_DB_REVIEW_TRIAGE.md).

| # | Issue | Impact | Status |
|---|---|---|---|
| 1 | `'pending'` passes consent predicates | 3.1M wrongly `can_personalize` | **Open — needs a decision** |
| 2 | Merges delete history, not forward it | 61,431 identities lost | **Open — irreversible** |
| 3 | Shared-workstation contract split | 2,732 rows oscillate | Open |
| 4 | `refresh_safety_check` samples 200, fail-open | Cannot see population drift | Open |
| 5 | `MERGED` → `MERGE` vocabulary split | Filters on `'MERGED'` go stale | Open (hub PR merged) |

**Fixed and verified this week:** `created_at` genesis across all three write
paths (`6ed6e04`, `76f7e3a`, `3b4e298`) and the `last_active_at` NULL wipe
(`8b511ad`).

## Trust summary

| Field | Trust | Note |
|---|---|---|
| `bn_id`, `bionews_uk`, `npi_number` | **High** | Immutable, verified |
| `email`, `communication_opt_in` | **High** | Directly sourced |
| `created_at` | **High** | Repaired; 4,459 dates from 2013 |
| `preferred_condition` (declared) | **High** | 399,185 rows, conf 0.7–0.89 |
| `preferred_condition` (inferred) | **Medium** | 2,746,204 rows, conf 0.5 |
| `last_active_at` | **Medium** | Fix shipped; ~284K legacy NULLs remain |
| `consent_status` | **Low** | `'pending'` ambiguous — Part 1 |
| `can_personalize` | **Low** | Do not use for consent decisions yet |
