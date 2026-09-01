# Profile Database - Canonical Version

**Current version:** v6.7
**Effective date:** 2026-08-18
**Status:** v6.7 (acquisition dates) published 2026-08-18. Adds `source_created_at`, `source_created_basis`, and `source_created_set_at` to `profile_core` and `profile_core_snapshot`. `created_at` records when the pipeline first observed a profile, which is not when the person arrived, so it cannot be used for growth reporting. `source_created_at` carries the true acquisition date recovered from the registering source system, written once and never overwritten, and `source_created_basis` grades it as registration / first-sighting / upper-bound so consumers can tell a real signup from a cookie sighting. New build step `enrich_source_created_at.sql`; new reporting surfaces `profile_growth_known` and `profile_growth_anonymous` (sql/profile_growth_views_v2.sql) enforce the safe filters structurally. Tier1 coverage 99.6%, tier2 99.0%. v6.5 multi-role and v6.6 condition normalization unchanged.

---

## How to use this file

This is the single source of truth for the profile database version string.
Every other doc, extractor module, and SQL comment that references "the
version" should link here rather than repeat the string.

If the version changes, update it here first. Then update these enforcement
points in a single PR:

1. `shared/profile_database_manifest.py` - the canonical runtime version constant
2. `docs/PROFILE_DATABASE_SCHEMA_CONTRACT.md` + `PROFILE_DATABASE_DATA_DICTIONARY.md` + `PROFILE_DATABASE_LINEAGE.md` - all three are generated; run `python scripts/generate_schema_contract.py`
3. `docs/PROFILE_DATABASE_REFERENCE.md` - the top-of-file header

Anything else that appears version-stamped (`BUILD_REPORT`, `GUIDE`,
`INVENTORY`) should reference this file and inherit the version.

---

## Version history

| Version | Date | Highlights |
|---------|------|------------|
| v6 | 2026-04-08 | Consolidated schema: persona sub-tables merged into `profile_core`, ref tables collapsed into `profile_lookup`. |
| v6.1 | 2026-04-15 | LimeSurvey rewire, zero-party resolution, condition confidence, app-field preservation bridge, GA4 `site_events` backfill. |
| v6.2 | 2026-04-21 | MVP-gap closure, caregiver E5 fix, `interest_tags` and `follow_conditions` enrichment, consent `pending` seed. |
| v6.3 | 2026-04-22 | Demographics, contact, extended names, NPI metadata, cross-persona interest flags, and `profile_current_safe`. |
| v6.4 | 2026-04-22 | Pre-rebuild hardening: `incremental` retired, refresh/rebuild parity, persistence-aware snapshot/restore, identity-hub preflight, release assertions, scope audit, and version drift cleanup. |
| v6.5 | 2026-05-19 | Multi-role persona migration. Dropped single `account_type` STRING + `account_type_source/_confidence/_updated_at` + `persona_source` (5 cols). Added 5 boolean role flags (`is_patient`, `is_hcp`, `is_caregiver`, `is_family_or_friend`, `is_other`) plus 15 lineage columns and the new `profile_roles` view. 879 multi-role users surfaced in production; 474 HCP+patient overlap that was previously squashed by single-value `account_type` now correctly represented. Source-tier confidence enforced everywhere (npi/survey/confirmed=1.0, buddypress=0.8, mailchimp/gravity=0.7, inferred=0.5). `is_other` stays NULL until positive signal. 8 fields tracked in `profile_field_changes` (the 5 flags + preferred_condition + condition_subtype + diagnosis_stage). |
| v6.6 | 2026-05-26 | Condition normalization. New `preferred_condition_normalized STRUCT<condition_key, label, mesh_id>` column on `profile_core` + `profile_core_snapshot` (4 cols incl. lineage triple). New `enrich_condition_normalization.sql` build step resolves `preferred_condition.label` against `conditions_dict` via key/alias/mesh/label match priority. Surfaces unmatched values via `profile_exceptions.unnormalized_condition` for curator follow-up. Refresh gate extended with `bidirectional_no_lineage` block for the new column. `profile_field_changes` now tracks 9 fields (the 5 v6.5 role flags + 4 condition/diagnosis fields). |
| v6.7 | 2026-08-18 | Acquisition dates. Added `source_created_at`, `source_created_basis`, `source_created_set_at` to `profile_core` + `profile_core_snapshot` (3 cols each), declared in DDL so a rebuild CTAS preserves them. New write-once build step `enrich_source_created_at.sql` recovers the real signup date from Mailchimp, WordPress, LimeSurvey, NPI and identity-hub edges, grading each date by provenance. New `profile_growth_known` / `profile_growth_anonymous` views hard-filter unsafe dates rather than relying on analyst discipline: raw cookie-birth charts overstated anonymous growth ~396x against ~45% real GA4 traffic growth. Also added `profile_ops.profile_evictions` to the documented ops surface, and a generated `PROFILE_DATABASE_DATA_DICTIONARY.md` covering every table, column and writer. |

---

## Build acceptance checks (v6.4) — STATUS

The first v6.4 rebuild (`7a94edbc`, 2026-04-25) passed all six checks and
promoted to production. The latest refresh (`ce9e25bf`, 2026-04-25) passed
11 of 11 hard assertions across 132,392 changed profiles.

The six acceptance checks (now permanent gates):

1. Refresh vs rebuild parity on sampled `bn_id`s and key aggregate counts
2. Zero unexpected orphan satellite rows in `profile_engagement`, `profile_identifiers`, `profile_segment_tags`, `profile_content_affinity`, and `profile_ad_attribution`
3. Restore coverage >= 99% of snapshot rows remapped to a current `bn_id`
4. Identity-hub manifest ID logged into `profile_ops.profile_build_runs.identity_hub_manifest_id`
5. `profile_current_safe` documented as the default consumer surface in the contract docs
6. `persona_snapshot_check.py` passes with expected non-zero counts for Sarah, David, Dr. Chen, and Lisa personas

These checks now gate publication of the consumer view layer. Candidate views
are built in `profile_staging` first and only published to `profile_data`
after the checks pass. `rebuild` now also stages physical consumer tables in
`profile_data_candidate`, publishes production views against that validated
candidate dataset, then promotes physical tables into `profile_data` and
repoints the views back to production.

`refresh` also now has a scope guard before the expensive MERGE runs:
rebuild sentinel rows fail immediately, scopes above `25%` of `profile_core`
hard-fail unless `--force`, and scopes above `10%` warn.

If the scope resolves to zero changed profiles, `refresh` now short-circuits as
`completed_no_changes` instead of paying for a no-op incremental run.

Soft performance checks now cover whole-run duration, refresh-scope size, and
step-level regressions against recent same-mode history.

Each run also now records a `runtime_fingerprint` and refresh-scope source
summary in `profile_ops.profile_build_runs` so rebuilds and refreshes are
traceable to the exact on-disk runtime that produced them.

---

## Default consumer surface

Use `profile_data.profile_current_safe` for general-purpose queries. It redacts
sensitive fields such as `ethnicity`, `veteran`, `age_exact`, `phone`,
`address_postal_code`, `symptom_tags`, `diagnosis_stage`,
`diagnosis_timing_band`, `treatments_current`, `treatments_of_interest`, and
`treatments_discussed`.

`profile_data.profile_current` exposes those fields for documented, approved use
cases such as consent-reviewed research, DSR, and internal HCP tooling. Most
consumers should not query it directly.

See `docs/PROFILE_DATABASE_GUIDE.md` section 9 for the sensitive-field
governance policy.
