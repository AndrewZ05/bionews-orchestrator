"""
Shared manifest for the live profile database runtime.

This file is the single source of truth for:
  - schema version
  - build steps and modes
  - active SQL module lists
  - public runtime layer metadata used by docs/tooling

Keep this module free of BigQuery or repo-specific side effects so it can be
imported safely by the extractor, validators, and doc generators.

Build modes may include orchestration-only entries (empty step tuple), e.g.
``resume_publish`` — implemented in ``plugins/profile_database_extractor.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class BuildStep:
    name: str
    sql_file: str
    description: str


SCHEMA_VERSION: Final[str] = "v6.7"
PRODUCTION_DATASET: Final[str] = "profile_data"
REBUILD_CANDIDATE_DATASET: Final[str] = "profile_data_candidate"
OPS_DATASET: Final[str] = "profile_ops"
STAGING_DATASET: Final[str] = "profile_staging"
VIEW_CANDIDATE_DATASET: Final[str] = STAGING_DATASET
DEFAULT_CONSUMER_VIEW: Final[str] = "profile_current_safe"

# Lineage / explainability coverage
# --------------------------------
# The only canonical field-level change lineage currently tracked is the narrow
# audit log in profile_ops.profile_field_changes. This is intentionally limited
# to high-risk persona classification fields.
# v6.5 (Phase 2 complete): account_type was dropped; the 5 per-role flags
# (is_patient / is_hcp / is_caregiver / is_family_or_friend / is_other) are the
# canonical persona surface.
FIELD_CHANGES_TRACKED_FIELDS: Final[tuple[str, ...]] = (
    "preferred_condition",
    "preferred_condition_normalized",  # v6.6 normalized canonical condition
    "condition_subtype",
    "diagnosis_stage",
    # v6.5 multi-role flags
    "is_patient",
    "is_hcp",
    "is_caregiver",
    "is_family_or_friend",
    "is_other",
)

BUILD_STEPS: Final[tuple[BuildStep, ...]] = (
    BuildStep(
        "snapshot",
        "sql/profile_database_snapshot_app_fields.sql",
        "Snapshot app-written fields before rebuild",
    ),
    BuildStep(
        "rebuild_scope",
        "sql/profile_database_rebuild_scope_all.sql",
        "Write rebuild scope sentinel (all bn_ids in scope)",
    ),
    BuildStep(
        "ddl",
        "sql/profile_database_ddl.sql",
        "Drop deprecated tables, create v6 schema across consumer, ops, and staging datasets",
    ),
    BuildStep(
        "maintenance",
        "sql/profile_database_maintenance.sql",
        "Seed reference tables + dictionaries",
    ),
    BuildStep(
        "populate_identity_core",
        "sql/populate_identity_core.sql",
        "Populate: identifiers + core profile CTAS + genesis lineage",
    ),
    BuildStep(
        "populate_engagement",
        "sql/populate_engagement.sql",
        "Populate: engagement summary table",
    ),
    BuildStep(
        "populate_survey",
        "sql/populate_survey.sql",
        "Populate: survey data (LimeSurvey S5)",
    ),
    BuildStep(
        "populate_newsletter_forum",
        "sql/populate_newsletter_forum.sql",
        "Populate: newsletter preferences + forum settings",
    ),
    BuildStep(
        "populate_finalize",
        "sql/populate_finalize.sql",
        "Populate: backfill last_active_at from engagement",
    ),
    BuildStep(
        "fill_gaps_condition_site",
        "sql/fill_gaps_condition_site.sql",
        "Fill gaps: preferred condition + site domain (Parts 1-2)",
    ),
    BuildStep(
        "fill_gaps_site_domain",
        "sql/fill_gaps_site_domain.sql",
        "Fill gaps: site_domain tenant key from wp_user_id / Mailchimp / acceptor / AIM / site_events / affinity",
    ),
    BuildStep(
        "fill_gaps_ga4_engagement",
        "sql/fill_gaps_ga4_engagement.sql",
        "Fill gaps: GA4 + forms + survey + engagement tier (Parts 3-8)",
    ),
    BuildStep(
        "fill_gaps_aim_attribution",
        "sql/fill_gaps_aim_attribution.sql",
        "Fill gaps: AIM + dictionaries + ad attribution + content affinity (Parts 9-13)",
    ),
    BuildStep(
        "fill_gaps_forum_infer",
        "sql/fill_gaps_forum_infer.sql",
        "Fill gaps: BuddyPress forum interactions + engagement tier (Parts 14, 16)",
    ),
    # Runs in BOTH rebuild and refresh. Condition inference used to live inside
    # fill_gaps_forum_infer, which is rebuild-only -- so a person who registered
    # today waited for the next full rebuild to get a condition. All three parts
    # are forward-only (fill NULL, never overwrite), so nightly execution is safe.
    BuildStep(
        "enrich_condition_inference",
        "sql/enrich_condition_inference.sql",
        "Enrich: preferred_condition inference (content affinity, MC cleaned lists, registration site)",
    ),
    BuildStep(
        "backfill_site_events",
        "sql/backfill_site_events_ga4.sql",
        "Backfill site_events from GA4 (365d retention; rolling reload)",
    ),
    BuildStep(
        "enrich_subtypes_diagnosis",
        "sql/enrich_subtypes_diagnosis.sql",
        "Enrich: Parts 1-2 (condition subtype + diagnosis stage from MMERGE4)",
    ),
    BuildStep(
        "enrich_dictionaries_zeroparty",
        "sql/enrich_dictionaries_zeroparty.sql",
        "Enrich: Parts 3-4 (dictionaries + zero-party GA4 events)",
    ),
    BuildStep(
        "enrich_segments_signup",
        "sql/enrich_segments_signup.sql",
        "Enrich: Parts 5-6 (segment tags + signup source)",
    ),
    BuildStep(
        "enrich_completeness_limesurvey",
        "sql/enrich_completeness_limesurvey.sql",
        "Enrich: Parts 7-11 (profile completeness + LimeSurvey survey data)",
    ),
    BuildStep(
        "enrich_hcp_buddypress",
        "sql/enrich_hcp_buddypress.sql",
        "Enrich: Parts 12-13B (HCP specialty + BuddyPress xprofile + zero-party polls)",
    ),
    BuildStep(
        "enrich_mailchimp_ga4",
        "sql/enrich_mailchimp_ga4.sql",
        "Enrich: Parts 14-18 (Mailchimp engagement + GA4 content affinity + condition decay)",
    ),
    BuildStep(
        "enrich_condition_interest",
        "sql/enrich_condition_interest.sql",
        "Enrich: Parts 19-22 (condition interest signals + community engagement)",
    ),
    BuildStep(
        "enrich_surveyengine",
        "sql/enrich_surveyengine.sql",
        "Enrich: SurveyEngine registration answers (persona, preferences, names, bands)",
    ),
    BuildStep(
        "enrich_condition_normalization",
        "sql/enrich_condition_normalization.sql",
        "Enrich: v6.6 normalize preferred_condition.label against conditions_dict",
    ),
    BuildStep(
        "enrich_newsletter_clinical",
        "sql/enrich_newsletter_clinical.sql",
        "Enrich: Parts 23-24 (newsletter topics + clinical trial interest)",
    ),
    BuildStep(
        "personas_classify",
        "sql/personas_classify.sql",
        "Personas: 5 role flags (is_X) + preferred_condition (E1, E1B, E2, E3)",
    ),
    BuildStep(
        "personas_patient",
        "sql/personas_patient.sql",
        "Personas: patient sub-profiles (E4 - symptoms, treatments, diagnosis, community)",
    ),
    BuildStep(
        "personas_caregiver",
        "sql/personas_caregiver.sql",
        "Personas: caregiver sub-profiles (E5 - relationship, community, focus)",
    ),
    BuildStep(
        "personas_stage",
        "sql/personas_stage.sql",
        "Personas: profile stage + summary report (E6, E7)",
    ),
    BuildStep(
        "restore",
        "sql/profile_database_restore_app_fields.sql",
        "Restore app-written fields captured by snapshot (v6.4 persistence-aware)",
    ),
    BuildStep(
        "views",
        "sql/profile_database_views.sql",
        "Canonical views (profile_current, profile_current_safe, audiences, coverage)",
    ),
    BuildStep(
        "snapshot_core",
        "sql/profile_database_snapshot_core.sql",
        "v6.4 point-in-time snapshot of profile_core (rebuild only)",
    ),
    BuildStep(
        "refresh_scope",
        "sql/profile_database_refresh_scope.sql",
        "Populate refresh scope: bn_ids touched in lookback window",
    ),
    BuildStep(
        "refresh",
        "sql/profile_database_refresh.sql",
        "Daily lookback MERGE into existing tables",
    ),
    BuildStep(
        "reconcile",
        "sql/profile_database_reconcile.sql",
        "Scope-wide reconciliation: evict bn_ids no longer eligible, backfill missing eligible bn_ids, clean satellite orphans",
    ),
    BuildStep(
        "enrich_source_created_at",
        "sql/enrich_source_created_at.sql",
        "Write-once acquisition date from source systems, hub edges and cookie decode",
    ),
)

BUILD_STEP_MAP: Final[dict[str, BuildStep]] = {step.name: step for step in BUILD_STEPS}

BUILD_MODES: Final[dict[str, tuple[str, ...]]] = {
    "rebuild": (
        "snapshot",
        "rebuild_scope",
        "ddl",
        "maintenance",
        "populate_identity_core",
        "populate_engagement",
        "populate_survey",
        "populate_newsletter_forum",
        "populate_finalize",
        # rebuild CTASes profile_core, which drops source_created_at along with
        # its values. This refills it from the satellites, hub edges and cookie
        # decode -- without it a rebuild silently empties the column.
        "enrich_source_created_at",
        "fill_gaps_condition_site",
        "fill_gaps_ga4_engagement",
        "fill_gaps_aim_attribution",
        "fill_gaps_forum_infer",
        "backfill_site_events",
        # Must FOLLOW backfill_site_events: one of its six tiers reads
        # profile_data.site_events, which that step populates.
        "fill_gaps_site_domain",
        # Must FOLLOW fill_gaps_site_domain (2026-09-01). Part 15C of this step
        # infers a condition from profile_core.site_domain, and
        # fill_gaps_site_domain is what puts site_domain there for the great
        # majority of profiles -- 6.4M of them via the acceptor tier alone.
        # Running inference FIRST meant Part 15C read site_domain before it was
        # populated, so every profile whose site arrived from that step waited a
        # full day for the next refresh to get its condition. Measured on
        # 2026-09-01: 63,171 profiles sat with a mapped site and no condition,
        # and all 63,171 had site_domain_source set by fill_gaps_site_domain.
        # The step reported "completed" throughout -- it had simply run too
        # early to see the data it depends on.
        "enrich_condition_inference",
        "enrich_subtypes_diagnosis",
        "enrich_dictionaries_zeroparty",
        "enrich_segments_signup",
        "enrich_completeness_limesurvey",
        "enrich_hcp_buddypress",
        "enrich_mailchimp_ga4",
        "enrich_condition_interest",
        "enrich_surveyengine",
        "enrich_condition_normalization",
        "enrich_newsletter_clinical",
        "personas_classify",
        "personas_patient",
        "personas_caregiver",
        "personas_stage",
        "restore",
        "views",
        "snapshot_core",
    ),
    "resume_rebuild": (
        "restore",
        "views",
        "snapshot_core",
    ),
    # Operational recovery: resume a failed physical-table promotion from candidate
    # to production without re-running SQL build steps. Implemented in
    # plugins/profile_database_extractor.py (not a SQL step list).
    "resume_publish": (),
    "refresh": (
        "refresh_scope",
        "refresh",
        "reconcile",
        # Must follow reconcile: STEP 3C there moves created_at BACKWARD, and this
        # step rejects any source date later than created_at. Running it earlier
        # would write dates that reconcile then invalidates.
        "enrich_source_created_at",
        # Reference data only (profile_lookup + the four *_dict tables), plus two
        # forward-only MERGEs on engagement_tier/last_active_at. No profile field
        # is overwritten. Must run nightly: enrich_condition_normalization matches
        # preferred_condition against conditions_dict, so a dictionary edit that
        # only lands on full rebuild leaves newly-filled conditions unnormalized.
        "maintenance",
        # 2026-08-28: this was rebuild-only, and its Part 2 is one of only two
        # writers of site_domain. 9,711 profiles satisfy every one of its
        # conditions -- subscribed to a mapped Mailchimp list -- and sit NULL,
        # accumulating ~1,400/month since the last rebuild. Same rebuild-only
        # trap that stranded condition inference in August. Both parts are
        # forward-only (guarded on IS NULL), so running it nightly is a cheap
        # no-op for anyone already filled.
        "fill_gaps_condition_site",
        # Must precede enrich_condition_normalization and the personas_* steps:
        # they read preferred_condition, so it has to be filled first.
        "backfill_site_events",
        # Must FOLLOW backfill_site_events: one of its six tiers reads
        # profile_data.site_events, which that step populates.
        "fill_gaps_site_domain",
        # Must FOLLOW fill_gaps_site_domain (2026-09-01). Part 15C of this step
        # infers a condition from profile_core.site_domain, and
        # fill_gaps_site_domain is what puts site_domain there for the great
        # majority of profiles -- 6.4M of them via the acceptor tier alone.
        # Running inference FIRST meant Part 15C read site_domain before it was
        # populated, so every profile whose site arrived from that step waited a
        # full day for the next refresh to get its condition. Measured on
        # 2026-09-01: 63,171 profiles sat with a mapped site and no condition,
        # and all 63,171 had site_domain_source set by fill_gaps_site_domain.
        # The step reported "completed" throughout -- it had simply run too
        # early to see the data it depends on.
        "enrich_condition_inference",
        "enrich_subtypes_diagnosis",
        "enrich_dictionaries_zeroparty",
        "enrich_segments_signup",
        "enrich_completeness_limesurvey",
        "enrich_hcp_buddypress",
        "enrich_mailchimp_ga4",
        "enrich_condition_interest",
        "enrich_surveyengine",
        "enrich_condition_normalization",
        "enrich_newsletter_clinical",
        "personas_classify",
        "personas_patient",
        "personas_caregiver",
        "personas_stage",
        "views",
    ),
    "reenrich": (
        "rebuild_scope",
        "enrich_source_created_at",
        "enrich_condition_inference",
        "enrich_subtypes_diagnosis",
        "enrich_dictionaries_zeroparty",
        "enrich_segments_signup",
        "enrich_completeness_limesurvey",
        "enrich_hcp_buddypress",
        "enrich_mailchimp_ga4",
        "enrich_condition_interest",
        "enrich_surveyengine",
        "enrich_condition_normalization",
        "enrich_newsletter_clinical",
        "personas_classify",
        "personas_patient",
        "personas_caregiver",
        "personas_stage",
        "views",
    ),
    # One-shot: unfreeze / widen site_events from GA4 (full 365d reload).
    "backfill_site_events": ("backfill_site_events",),
    "views": ("views",),
}

# GA4 → site_events retention + reload policy (see sql/backfill_site_events_ga4.sql).
SITE_EVENTS_LOOKBACK_DAYS: Final[int] = 365
SITE_EVENTS_RELOAD_DAYS_FULL: Final[int] = 365  # rebuild + one-shot recovery
SITE_EVENTS_RELOAD_DAYS_REFRESH: Final[int] = 14  # nightly rolling overlap

LEGACY_MODE_ALIASES: Final[dict[str, str]] = {
    "incremental": "reenrich",
    "enrich": "reenrich",
}

ACTIVE_SQL_FILES: Final[tuple[str, ...]] = tuple(
    dict.fromkeys(step.sql_file for step in BUILD_STEPS)
)

SCHEMA_VALIDATION_SQL_FILES: Final[tuple[str, ...]] = ACTIVE_SQL_FILES

DRY_RUN_EXCLUDED_STEPS: Final[frozenset[str]] = frozenset(
    {"rebuild_scope", "refresh_scope"}
)
DRY_RUN_SQL_FILES: Final[tuple[str, ...]] = tuple(
    step.sql_file for step in BUILD_STEPS if step.name not in DRY_RUN_EXCLUDED_STEPS
)

EXTERNAL_DATASETS: Final[set[str]] = {
    "mailchimp_data",
    "limesurvey_data",
    "wordpress_data",
    "identity_hub_data",
    "npi_data",
    "df_warehouse_output",
    "BN_Warehouse",
    # Read by fill_gaps_site_domain: the acceptor carries the site on every
    # pageview, AIM carries it in the referrer URL. Both are upstream sources
    # we only read, so they must NOT be rewritten to dry-run stub datasets.
    "BN_Acceptor",
    "AIM_Clickstream",
}

LEGACY_MONOLITH_FILES: Final[tuple[str, ...]] = (
    "sql/legacy/profile_database/profile_database_populate.sql",
    "sql/legacy/profile_database/profile_database_fill_gaps.sql",
    "sql/legacy/profile_database/profile_database_enrich_v2.sql",
    "sql/legacy/profile_database/profile_enrich_personas.sql",
)

PHYSICAL_TABLE_GROUPS: Final[tuple[tuple[str, tuple[str, ...]], ...]] = (
    (
        "Consumer Core",
        (
            "profile_core",
            "profile_identifiers",
            "profile_engagement",
            "profile_preferences",
            "profile_survey_data",
            "site_events",
            "profile_zero_party",
        ),
    ),
    (
        "Derived Signals",
        (
            "profile_content_affinity",
            "profile_ad_attribution",
            "profile_segment_tags",
        ),
    ),
    (
        "Reference",
        (
            "conditions_dict",
            "symptoms_dict",
            "treatments_dict",
            "subtypes_dict",
            "dictionary_meta",
            "profile_lookup",
        ),
    ),
)

OPS_TABLE_GROUPS: Final[tuple[tuple[str, tuple[str, ...]], ...]] = (
    (
        "Profile Ops",
        (
            "profile_build_runs",
            "profile_build_steps",
            "profile_core_snapshot",
            "profile_field_changes",
            "profile_restore_unmapped",
            "profile_publish_manifest",
            "profile_dataset_leases",
            "profile_evictions",
        ),
    ),
)

STAGING_TABLE_GROUPS: Final[tuple[tuple[str, tuple[str, ...]], ...]] = (
    (
        "Profile Staging",
        (
            "profile_core_app_snapshot",
            "refresh_scope_bn_ids",
            "zero_party_staging",
        ),
    ),
    (
        "Identity Consistency Snapshots",
        (
            "identity_xref_snapshot",
            "identity_hub_snapshot",
            "identity_persistence_snapshot",
        ),
    ),
)

VIEW_GROUPS: Final[tuple[tuple[str, tuple[str, ...]], ...]] = (
    (
        "Supported Consumer Views",
        (
            "profile_current_safe",
            "profile_current",
            "profile_signals",
            "profile_explain",
            "profile_events",
            "profile_contactability",
            "profile_build_performance",
            "profile_release_status",
            "profile_marketing_audience",
            "profile_analytics_audience",
            "profile_ops_audience",
            "profile_exceptions",
            "profile_coverage",
            "profile_audience_hcp",
            "profile_audience_patients_confirmed",
            "profile_audience_caregivers",
            "profile_audience_high_engagement",
            "profile_metrics",
        ),
    ),
    (
        "Compatibility Views",
        (
            "profile_engagement_monthly",
            "profile_newsletter_preferences",
            "profile_forum_settings",
            "profile_consent",
        ),
    ),
)

SUPPORTED_VIEWS: Final[tuple[str, ...]] = VIEW_GROUPS[0][1]
COMPATIBILITY_VIEWS: Final[tuple[str, ...]] = VIEW_GROUPS[1][1]

PHYSICAL_TABLES: Final[tuple[str, ...]] = tuple(
    table for _, tables in PHYSICAL_TABLE_GROUPS for table in tables
)

TABLE_DESCRIPTIONS: Final[dict[str, str]] = {
    "profile_core": "Single row per bn_id; persona, condition, demographic, HCP, and actionability fields.",
    "profile_identifiers": "All known identifiers per profile; the main cross-system join surface.",
    "profile_engagement": "Rolled-up behavioral and lifecycle summary per profile.",
    "profile_preferences": "Launch-era consolidated preferences surface: newsletter subscriptions plus forum settings.",
    "profile_survey_data": "Normalized LimeSurvey answers linked to bn_id.",
    "site_events": "GA4-backed site interaction fact table feeding the unified event surface.",
    "profile_zero_party": "Identity-linked poll, quiz, and other zero-party responses feeding the unified event surface.",
    "profile_content_affinity": "Per-condition browsing affinity derived from content consumption.",
    "profile_ad_attribution": "Per-click attribution facts and ad-touch history.",
    "profile_segment_tags": "Governed tags used for segmentation and promoted downstream signals.",
    "conditions_dict": "Canonical condition vocabulary.",
    "symptoms_dict": "Canonical symptom vocabulary.",
    "treatments_dict": "Canonical treatment vocabulary.",
    "subtypes_dict": "Condition subtype vocabulary.",
    "dictionary_meta": "Dictionary provenance and curation metadata.",
    "profile_lookup": "General lookup/config table used by profile SQL modules.",
    "profile_build_runs": "Run-level build observability.",
    "profile_build_steps": "Step-level build observability.",
    "profile_core_snapshot": "Point-in-time rebuild snapshot of profile_core.",
    "profile_field_changes": "Narrow audit log for the 8 persona-classification fields tracked in v6.5: condition_subtype, diagnosis_stage, preferred_condition, and the 5 role flags (is_patient, is_hcp, is_caregiver, is_family_or_friend, is_other). Not a generic field-level lineage surface - other field changes are not tracked here.",
    "profile_restore_unmapped": "Restore diagnostics for snapshot rows that could not be remapped.",
    "profile_publish_manifest": "Per-table publish telemetry for candidate-to-production promotion during rebuilds (copy status, verification counts, and errors).",
    "profile_dataset_leases": "Mutual-exclusion lease rows for rebuild publish (candidate dataset); managed by shared/profile_rebuild_hardening.py.",
    "profile_evictions": "Audit of bn_ids removed from profile_core by reconcile: merge-forward survivors, ineligible profiles, and rows whose xref disappeared. One row per eviction with the reason and the surviving bn_id where there is one.",
    "profile_core_app_snapshot": "Pre-rebuild snapshot of app-written fields used for restore.",
    "refresh_scope_bn_ids": "Changed-profile subset table used to scope refresh-safe writes.",
    "zero_party_staging": "Anonymous zero-party response staging before identity promotion.",
    "identity_xref_snapshot": "Build-local snapshot copy of identity_hub_data.bn_id_xref used to pin one consistent identity source image per run.",
    "identity_hub_snapshot": "Build-local snapshot copy of identity_hub_data.bn_id_hub used to pin acceptor/web activity reads per run.",
    "identity_persistence_snapshot": "Build-local snapshot copy of identity_hub_data.bn_id_persistence used to pin merge/split history reads per run.",
}

VIEW_DESCRIPTIONS: Final[dict[str, str]] = {
    "profile_current_safe": "Default query surface. One row per profile with sensitive fields redacted.",
    "profile_current": "Full sensitive surface for approved use only.",
    "profile_signals": "Unified one-row-per-profile derived signal summary over affinity, tags, and attribution.",
    "profile_explain": "One-row explainability surface for source, confidence, and supporting signals.",
    "profile_events": "Unified event timeline view over site_events and profile_zero_party.",
    "profile_contactability": "One-row-per-profile consent and reachability summary.",
    "profile_build_performance": "Step-level build and refresh performance telemetry over profile_ops logs.",
    "profile_release_status": "One-row current release / publish health summary over build runs and publish steps.",
    "profile_marketing_audience": "Marketing-safe audience view derived from profile_current_safe.",
    "profile_analytics_audience": "Aggregate-safe analytics surface with identifiers removed.",
    "profile_metrics": "Single definition of every headline audience metric, one row per bn_id. Booleans for known person, verified HCP, mailable, active email (bot-filtered), known/active member (participation) and logged-in (presence), plus dimensions to slice by. Consumers group this view rather than redefining metrics; the traps (tier, raw opens, cookie dates, NULL role flags) are solved inside it.",
    "profile_ops_audience": "Support and operations lookup surface.",
    "profile_exceptions": "Data quality conflict surface.",
    "profile_coverage": "Field fill-rate monitoring surface.",
    "profile_audience_hcp": "HCP targeting audience.",
    "profile_audience_patients_confirmed": "Confirmed patient audience.",
    "profile_audience_caregivers": "Caregiver audience.",
    "profile_audience_high_engagement": "High-engagement audience.",
    "profile_engagement_monthly": "Compatibility view replacing the retired monthly engagement table.",
    "profile_newsletter_preferences": "Compatibility view over profile_preferences.newsletter_preferences.",
    "profile_forum_settings": "Compatibility view over profile_preferences.forum_settings.",
    "profile_consent": "COMPATIBILITY ONLY -- not the canonical consent fact. Re-shapes existing communication_opt_in / tracking_consent / GA4 OneTrust flags into a row-per-purpose surface for legacy consumers. Does not carry per-grant records or DSR state. For activation eligibility use profile_contactability; do not build marketing enforcement, compliance reporting, or legal decisions on this view.",
}


def ordered_subset(items: list[str], preferred: tuple[str, ...]) -> list[str]:
    present = set(items)
    return [item for item in preferred if item in present]


def build_step_map() -> dict[str, tuple[str, str]]:
    return {step.name: (step.sql_file, step.description) for step in BUILD_STEPS}
