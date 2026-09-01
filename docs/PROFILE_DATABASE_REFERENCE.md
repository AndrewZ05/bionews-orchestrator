# Profile Database Reference

**Version:** `v6.7` (acquisition dates shipped 2026-08-18)
**Owned datasets:** `profile_data`, `profile_data_candidate`, `profile_ops`, `profile_staging`
**Primary key:** `bn_id`
**Profiles in production:** 6,083,585 (verified 2026-08-18)

This document is intentionally short. It is the quick reference for what to
query, not a duplicate of the full schema contract.

For every table, every column with its definition, and the build steps that
populate it, see
[PROFILE_DATABASE_DATA_DICTIONARY.md](PROFILE_DATABASE_DATA_DICTIONARY.md).

The canonical runtime contract for build modes, active SQL modules, and the
public profile shape lives in `shared/profile_database_manifest.py`.

---

## Start here

For most work, query one of these:

| Surface | Live size | Use it for |
|---------|---:|------------|
| `profile_current_safe` | 3,727,916 | Default one-row-per-profile querying (PII redacted) |
| `profile_current` | 3,727,916 | Approved sensitive queries (full PII) |
| `profile_signals` | 3,727,916 | Unified signal lookup over affinity, tags, and ad attribution |
| `profile_explain` | 3,727,916 | One-row explainability and debugging lookup |
| `profile_events` | 15.7M rows | Unified event timeline querying |
| `profile_contactability` | 3,727,916 | One-row consent and reachability lookup |
| `profile_marketing_audience` | 411,916 | Marketing-safe activation (consent + email) |
| `profile_audience_hcp` | 305,941 | HCP targeting (NPI-anchored, verified) |
| `profile_audience_patients_confirmed` | 17,649 | Confirmed patients |
| `profile_audience_caregivers` | 1,241 | Caregivers |
| `profile_audience_high_engagement` | 192,783 | engagement_tier='high' (top 5.6%) |
| `profile_analytics_audience` | 3,727,916 | Aggregate-safe BI (identifiers redacted) |
| `profile_ops_audience` | 3,727,916 | Support / CX lookup (identifiers + flags) |
| `profile_build_performance` | â€“ | Build and refresh speed / cost diagnostics |
| `profile_release_status` | 1 row | Current release / publish health lookup |

If you are reaching for raw tables first, make sure you actually need them.

---

## Core physical tables

| Table | What it represents |
|-------|--------------------|
| `profile_core` | Canonical resolved profile attributes |
| `profile_identifiers` | All known identifiers by `bn_id` |
| `profile_engagement` | Rolled-up engagement metrics |
| `profile_preferences` | Newsletter + forum preferences |
| `profile_survey_data` | Normalized LimeSurvey answers |
| `site_events` | Current event timeline surface |
| `profile_zero_party` | Identity-linked zero-party answers |

## Supporting signal tables

| Table | What it represents |
|-------|--------------------|
| `profile_content_affinity` | Condition/site browsing affinity |
| `profile_ad_attribution` | Ad click / attribution facts |
| `profile_segment_tags` | Governed segmentation tags |

## Internal runtime tables (`profile_ops`)

These are valid warehouse objects, but not the first consumer mental model:

- `profile_build_runs` â€” run-level observability
- `profile_build_steps` â€” step-level performance metrics
- `profile_publish_manifest` â€” per-table promotion records (rebuild blue/green)
- `profile_core_snapshot` â€” point-in-time snapshot of profile_core (rebuild only)
- `profile_field_changes` -- narrow audit log for eight tracked fields: the five v6.5 role flags (is_patient, is_hcp, is_caregiver, is_family_or_friend, is_other), preferred_condition, condition_subtype, diagnosis_stage; not a generic field-level lineage surface
- `profile_restore_unmapped` â€” diagnostic rows for snapshot rows that could not be remapped
- `profile_dataset_leases` â€” mutual-exclusion leases for rebuild publish (infra; managed by `shared/profile_rebuild_hardening.py`; listed in `OPS_TABLE_GROUPS` in `shared/profile_database_manifest.py`)

## Staging helpers

These sit outside the consumer dataset:

- `profile_staging.zero_party_staging` â€” anonymous zero-party answers awaiting identity promotion
- `profile_staging.profile_core_app_snapshot` â€” pre-rebuild capture of app-authored fields used by restore
- `profile_staging.refresh_scope_bn_ids` â€” changed-profile subset for refresh / reenrich scoping
- `profile_staging.identity_xref_snapshot` â€” build-local pin of `identity_hub_data.bn_id_xref`
- `profile_staging.identity_hub_snapshot` â€” build-local pin of `identity_hub_data.bn_id_hub`
- `profile_staging.identity_persistence_snapshot` â€” build-local pin of `identity_hub_data.bn_id_persistence`

---

## Compatibility surfaces

These names still resolve, but they are compatibility layers:

| Surface | Current implementation |
|---------|------------------------|
| `profile_engagement_monthly` | View |
| `profile_newsletter_preferences` | View over `profile_preferences` |
| `profile_forum_settings` | View over `profile_preferences` |
| `profile_consent` | Derived compatibility view |

---

## Common query patterns

### Default profile lookup

```sql
SELECT *
FROM profile_data.profile_current_safe
WHERE bn_id = 'BN_...';
```

### Resolve a person from an identifier

```sql
SELECT pc.*
FROM profile_data.profile_identifiers pi
JOIN profile_data.profile_current_safe pc USING (bn_id)
WHERE pi.identifier_type = 'email'
  AND LOWER(pi.identifier_value) = LOWER('jdoe@example.com');
```

### Marketing-safe audience

```sql
SELECT *
FROM profile_data.profile_marketing_audience
WHERE preferred_condition.label = 'multiple sclerosis';
```

### Ops lookup

```sql
SELECT *
FROM profile_data.profile_ops_audience
WHERE email = 'jdoe@example.com';
```

### Derived signal summary

```sql
SELECT bn_id, top_content_condition, segment_categories, ad_platforms
FROM profile_data.profile_signals
WHERE bn_id = 'BN_...';
```

### Explainability lookup

```sql
SELECT bn_id, is_patient_source, is_hcp_source, preferred_condition_source, has_condition_signal_conflict
FROM profile_data.profile_explain
WHERE bn_id = 'BN_...';
```

### Contactability summary

```sql
SELECT bn_id, contactability_status, marketing_status_reason, can_email_market
FROM profile_data.profile_contactability
WHERE bn_id = 'BN_...';
```

### Build performance summary

```sql
SELECT build_id, mode, runtime_fingerprint, refresh_scope_n, step_name,
       duration_seconds, step_total_bytes_processed, step_total_slot_millis
FROM profile_data.profile_build_performance
WHERE is_latest_for_mode
ORDER BY build_started_at DESC, duration_seconds DESC;
```

The runtime also emits a soft `step_performance_regression` check when a step
is materially slower or more expensive than recent same-mode history.

### Release status summary

```sql
SELECT *
FROM profile_data.profile_release_status;
```

### High-level event activity

```sql
SELECT bn_id, event_name, event_timestamp, properties
FROM profile_data.profile_events
WHERE bn_id = 'BN_...'
ORDER BY event_timestamp DESC
LIMIT 100;
```

### Zero-party answers

```sql
SELECT bn_id, event_name, event_timestamp, properties
FROM profile_data.profile_events
WHERE bn_id = 'BN_...'
  AND event_name LIKE 'zero_party_%'
ORDER BY event_timestamp DESC;
```

---

## Build/runtime pointers

- Active runtime uses `populate_*`, `fill_gaps_*`, `enrich_*`, and `personas_*`.
- The canonical runtime contract lives in `shared/profile_database_manifest.py`.
- `rebuild` now stages physical consumer tables in `profile_data_candidate` and releases through validated candidate-backed production views before finalizing back onto `profile_data`.
- `refresh` is protected by a scope guard before the expensive MERGE runs.
- Empty refresh scopes now short-circuit as a logged no-op instead of paying for a fake incremental run.
- Old monolithic SQL files are archived under `sql/legacy/profile_database/`.
  They are not the active build path and should not drive normal edits.
- For runtime behavior, see [PROFILE_DATABASE_GUIDE.md](PROFILE_DATABASE_GUIDE.md).
- For physical schema, write ownership, and compatibility rules, see
  [PROFILE_DATABASE_SCHEMA_CONTRACT.md](PROFILE_DATABASE_SCHEMA_CONTRACT.md).
