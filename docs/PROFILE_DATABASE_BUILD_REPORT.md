# Profile Database Build Report

**Version:** `v6.4` (runtime-shape counts kept current; see note below)
**Datasets:** `profile_data`, `profile_data_candidate`, `profile_ops`, `profile_staging`
**Last full rebuild snapshot:** `2026-04-25`
**Build id:** `7a94edbc` (resume_rebuild that completed the first-ever v6.4 publish)
**Profiles in production:** `3,460,572` (live count, post-publish)
**Latest refresh:** `ce9e25bf` (2026-04-25, 11/11 hard assertions passed, 132,392 profiles touched, 1,113s)

---

## What shipped in the current runtime

- split-module SQL runtime is the active build path
- `profile_current_safe` is the default query surface
- `refresh` and `rebuild` are aligned more closely than earlier versions
- snapshot/restore is persistence-aware
- build logging is split into `profile_ops.profile_build_runs` and `profile_ops.profile_build_steps`
- rebuild publish now emits per-table promotion telemetry to `profile_ops.profile_publish_manifest`
- `profile_preferences` consolidates newsletter + forum preferences
- `profile_signals` is now a unified consumer signal view over affinity, tags, and attribution
- `profile_explain` is now a one-row explainability surface over sources, confidence, and supporting signals
- `profile_events` is now a unified consumer event view over `site_events` and `profile_zero_party`
- `profile_contactability` is now a one-row consent and reachability surface
- `profile_build_performance` is now an ops-facing build and refresh performance view over `profile_ops` logs
- `profile_release_status` is now a one-row current release and publish-health view
- marketing and ops audience logic now reuses `profile_contactability` instead of re-implementing suppression rules
- consumer views now build in `profile_staging` first and only publish to `profile_data` after gates pass
- `refresh` now hard-fails early when scope accidentally expands toward rebuild territory
- `refresh` now short-circuits as a no-op when the lookback resolves to zero changed profiles
- soft performance checks now watch build-duration regression and refresh-scope efficiency
- soft performance checks now also flag step-level hot-spot regressions against recent same-mode runs
- build logs now capture step-level bytes processed, bytes billed, and slot-millis
- build logs now also capture a runtime fingerprint and refresh-scope source breakdown for reproducibility and tuning
- `rebuild` now stages physical consumer tables in `profile_data_candidate`, releases through validated candidate-backed production views, then promotes physical tables into `profile_data`
- bot / shared-workstation bn_ids are filtered out of satellite writes (engagement, ad_attribution, content_affinity, segment_tags) at source, not just in the assertion gate
- the `orphan_satellite` post-build assertion now uses a graduated 0.1% per-table threshold and excludes populate-suppressed bn_ids via an `_excluded_bn_ids` CTE
- `profile_field_changes` is now scoped explicitly as a narrow audit log for the four persona-classification fields (account_type, preferred_condition, condition_subtype, diagnosis_stage); the soft assertion no longer fires on `resume_rebuild` where lineage is not expected
- `profile_consent` carries an explicit "compatibility view, not authoritative" header banning marketing-enforcement / compliance / legal use, deferring those to the canonical consent fact roadmap
- failure paths now propagate `statements_executed` and `failed_statement_index` so post-mortems can identify exactly which statement failed inside a multi-statement step
- `_log_build_run_end` is now invoked from every exit path (success, gate failure, step failure, publish failure, snapshot_core failure) plus a final `try/finally` catch-all so run rows always reach a terminal state
- `_sync_profile_core_runtime_schema` now hard-fails with a clear error when `profile_core` is missing from `consumer_dataset`, instead of issuing an `ALTER TABLE` against a non-existent table
- `restore_app_fields.sql` and `refresh.sql` defensively `CREATE TABLE IF NOT EXISTS` their staging dependencies so a wiped `profile_staging` self-heals on the next run
- `profile_engagement_monthly`, `profile_newsletter_preferences`,
  `profile_forum_settings`, and `profile_consent` are now
  compatibility views rather than first-class runtime tables

---

## Runtime shape

| Metric | Value |
|--------|-------|
| Consumer/reference tables (`profile_data`) | 16 |
| Ops/history tables (`profile_ops`) | 8 |
| Staging helpers (`profile_staging`) | 6 |
| Views | 22 |
| Primary consumer table set | 7 core + 3 derived signal tables |
| Internal runtime tables | 8 ops + 6 staging helpers |

---

## Live production snapshot (post-rebuild)

### Scale

| Surface | Rows |
|---|---:|
| `profile_core` | 3,460,572 |
| `profile_engagement` | 3,464,071 |
| `profile_identifiers` | 18,161,373 |
| `profile_preferences` | 4,837 |
| `profile_survey_data` | 210,486 |
| `site_events` | 15,736,146 |
| `profile_zero_party` | 7,253 |
| `profile_segment_tags` | 120,119 |
| `profile_content_affinity` | 2,352,718 |
| `profile_ad_attribution` | 6,437,821 |

### Persona mix

> **Stale (v6.4 snapshot, 2026-04-25).** `account_type` was dropped in v6.5 and
> replaced by five independent BOOL role flags, so these buckets no longer
> exist as mutually exclusive categories -- a profile can now be both HCP and
> patient. Row counts elsewhere in this file are also a 2026-04-25 snapshot
> (`profile_core` has since grown from 3.46M to 5.44M). Use
> [PROFILE_DATABASE_DATA_DICTIONARY.md](PROFILE_DATABASE_DATA_DICTIONARY.md)
> for schema and query production for live counts.

| Account type (v6.4, dropped) | Profiles | Share |
|---|---:|---:|
| other (unclassified / anonymous-known) | 3,100,190 | 89.6% |
| hcp | 340,914 | 9.85% |
| patient | 17,649 | 0.51% |
| caregiver | 1,241 | 0.04% |
| family_or_friend | 578 | 0.02% |

### Engagement tier

| Tier | Profiles | Share |
|---|---:|---:|
| low | 2,428,406 | 70.1% |
| inactive | 595,798 | 17.2% |
| medium | 246,701 | 7.1% |
| high | 193,166 | 5.6% |

### Activation reach

| Channel | Reachable bn_ids | Notes |
|---|---:|---|
| Email opt-in (marketable) | 411,916 | `communication_opt_in = TRUE AND email IS NOT NULL` |
| Email known | 759,169 | identifier coverage |
| Meta Pixel (`fbp`) | 2,663,842 | Custom Audience eligible |
| Google Ads (`gcl_au`) | 2,590,103 | Customer Match eligible |
| Facebook Click (`fbc`) | 546,878 | post-click attribution |
| HCP NPI | 333,755 | NPI-anchored profiles |
| WordPress account | 10,753 | logged-in users |
| SSO `bionews_uk` | 311 | early-stage |

### Audience views

| View | Rows |
|---|---:|
| `profile_audience_hcp` | 305,941 |
| `profile_audience_patients_confirmed` | 17,649 |
| `profile_audience_caregivers` | 1,241 |
| `profile_audience_high_engagement` | 192,783 |
| `profile_marketing_audience` | 411,916 |
| `profile_analytics_audience` | 3,460,572 |
| `profile_ops_audience` | 3,460,572 |

### Top conditions (preferred_condition)

| Condition | Profiles |
|---|---:|
| Multiple Sclerosis (`ms`) | 404,688 |
| Myasthenia Gravis (`mya`) | 238,282 |
| Parkinson's (`par`) | 184,635 |
| ALS | 178,217 |
| Hemophilia (`hem`) | 174,847 |
| Autoimmune Encephalitis (`aed`) | 92,968 |
| Amyloidosis (`amy`) | 92,365 |
| Renal Cell Carcinoma (`rc`) | 79,547 |
| Muscular Dystrophy (`md`) | 75,991 |
| SMA | 73,017 |

### Signal coverage on `profile_core`

| Field | Fill rate |
|---|---:|
| `country` | 100.0% |
| `consent_status` | 100.0% |
| `profile_completeness` | 100.0% |
| `preferred_condition` | 75.7% |
| `persona_source` | 73.9% |
| `email` | 21.9% |
| `first_name` | 20.9% |
| `gender` | 8.6% |
| `signup_source` | 4.4% |
| `diagnosis_stage` | 0.5% |
| `condition_subtype` | 0.4% |

---

## Current build path

Active runtime:

- `plugins/profile_database_extractor.py`
- `sql/populate_*.sql`
- `sql/fill_gaps_*.sql`
- `sql/enrich_*.sql`
- `sql/personas_*.sql`

Legacy monolithic files are now archived under `sql/legacy/profile_database/`.
They are not the active build path.

---

## Validation status

Current simplification pass validates with:

```bash
python scripts/profile_release_check.py
```

Latest result:

- `327 passed`
- `0 failed`

Additional safety validation:

- `scope_predicate_audit.py` reports `0 unscoped writes`
- `profile_core_view_coverage_check.py` reports `0 missing columns` across all five satellite tables

Live refresh post-build assertions (build `ce9e25bf`, 2026-04-25):

- 11 of 11 hard assertions PASS
- `orphan_satellite [soft]: PASS-with-residual` — worst table at 0.012% (well below 0.1% threshold)
- `profile_field_changes_populated [soft]: PASS` — 985 lineage rows across 3 of 4 tracked fields
- `refresh_safety_check [hard]: PASS` — no immutable-field mutations detected
- soft performance checks: PASS (no regressions vs same-mode baseline)

---

## Known limitations

1. `rebuild` now uses `profile_data_candidate` for blue/green physical-table promotion, but `refresh` and `reenrich` still update live production tables in place.
2. Rebuild publish is operationally safe but not a single atomic BigQuery swap; production views bridge the validated candidate dataset during table promotion.
3. `refresh` now short-circuits empty scopes, but true sampled shadow parity still remains roadmap work.
4. `profile_consent` is now a derived compatibility surface, not a full canonical consent fact.
5. `profile_field_changes` is a narrow audit log for the four persona fields, not a generic field-level lineage surface; do not infer broader provenance from its presence.

See [PROFILE_DATABASE_ROADMAP.md](PROFILE_DATABASE_ROADMAP.md) for the next
architecture steps.
