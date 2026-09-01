"""Apply table and column descriptions to profile_data tables for Gemini Agent.

Reads the metadata defined below (sourced from
docs/GEMINI_AGENT_PROFILE_DATABASE_COMPLETE.md) and applies it to BigQuery via
google.cloud.bigquery. Idempotent: safe to re-run; only updates descriptions that
differ from the desired value.

AUTO-INVOCATION: This script's apply_metadata() function is also imported and
called by plugins.profile_database_extractor._restore_profile_metadata() after
every successful publish step. CREATE OR REPLACE VIEW (and CREATE TABLE in
rebuild mode) wipes descriptions, so the pipeline re-applies them automatically
so Data Canvas / Gemini Agent always have the metadata they need.

Usage (manual / ad-hoc):
    # Dry run (default) - shows what would change, no writes
    python scripts/apply_profile_metadata.py

    # Apply changes
    python scripts/apply_profile_metadata.py --apply

    # Limit to specific tables
    python scripts/apply_profile_metadata.py --apply --tables profile_core,conditions_dict

Auth: uses application-default credentials. If you see auth errors, run:
    gcloud auth application-default login

To extend with new tables/columns: edit TABLE_DESCRIPTIONS and COLUMN_DESCRIPTIONS
dicts below, then re-run with --apply. The pipeline will pick up changes
automatically on the next publish.
"""

from __future__ import annotations

import argparse
import sys
from typing import Optional

from google.cloud import bigquery


PROJECT = "bi-data-391216"
DATASET = "profile_data"


# ---------------------------------------------------------------------------
# TABLE-LEVEL DESCRIPTIONS
# ---------------------------------------------------------------------------

TABLE_DESCRIPTIONS: dict[str, str] = {
    "profile_core": (
        "Master customer profile record. One row per unique person (bn_id). "
        "Consolidates patient, HCP, caregiver, family/friend, and other-persona "
        "attributes into a single row per person. "
        "GRAIN: one row per bn_id. "
        "PRIMARY KEY: bn_id. "
        "SOURCE: Identity Hub (bn_id, identifiers), Mailchimp (email, consent), "
        "WordPress (registration), NPI Registry (HCP fields), LimeSurvey (demographics). "
        "UPDATE FREQUENCY: daily refresh at 02:00 UTC; full rebuild quarterly. "
        "PII LEVEL: HIGH (contains email, phone, names, health condition). "
        "TYPICAL QUESTIONS: 'How many patients with [condition]?', "
        "'What is the breakdown by persona?', 'Which HCPs are NPI-verified?', "
        "'How many users opted into marketing?', 'Audience by acquisition source'. "
        "JOINS: bn_id links to profile_engagement, profile_identifiers, "
        "profile_segment_tags, profile_preferences, profile_content_affinity, "
        "profile_survey_data, profile_zero_party, site_events, profile_ad_attribution. "
        "Always use COUNT(DISTINCT bn_id) for user counts. "
        "v6.5: 5 independent role flags (is_patient/is_hcp/is_caregiver/"
        "is_family_or_friend/is_other) replaced the single account_type column. "
        "Each can be TRUE independently -- one user can be is_hcp=TRUE AND "
        "is_patient=TRUE. Filter on the relevant flag BEFORE querying persona-"
        "specific fields (npi_number, specialty are NULL for non-HCPs; "
        "diagnosis_stage is NULL for non-patients). For multi-role queries see "
        "the profile_data.profile_roles view."
    ),
    "profile_engagement": (
        "Aggregated behavioral metrics per customer across email, web, community, "
        "and ads. 1:1 with profile_core (one row per bn_id). "
        "GRAIN: one row per bn_id. "
        "PRIMARY KEY: bn_id (FK to profile_core.bn_id). "
        "SOURCE: Mailchimp (email opens/clicks), GA4 (web sessions), site_events "
        "(form submits, video views, ad events), WordPress + BuddyPress (forum activity). "
        "UPDATE FREQUENCY: daily refresh at 02:00 UTC. "
        "PII LEVEL: MEDIUM (aggregate behavioral data; no direct identifiers). "
        "TYPICAL QUESTIONS: 'Which users are active/highly engaged?', "
        "'Show email open rate by condition', 'Who is at risk of churn?', "
        "'What is the best send time?', 'Average sessions by acquisition source'. "
        "For 'active users' or 'DAU' questions use engagement_tier IN ('high','medium') "
        "or last_seen_web >= CURRENT_TIMESTAMP() - INTERVAL 30 DAY."
    ),
    "profile_identifiers": (
        "All identifier types (email, phone, NPI, cookies, etc.) linked to each "
        "person. Used to map incoming data to a bn_id, debug identity graph, and "
        "build audience exports. "
        "GRAIN: one row per (bn_id, identifier_type, identifier_value) - many rows per person. "
        "PRIMARY KEY: (bn_id, identifier_type, identifier_value). "
        "FOREIGN KEY: bn_id -> profile_core.bn_id. "
        "SOURCE: Identity Hub. "
        "PII LEVEL: HIGH (contains raw emails, phones, NPIs). "
        "TYPICAL QUESTIONS: 'Which emails are linked to this bn_id?', "
        "'How many unique phone numbers do we have?', "
        "'Which users have an NPI in profile_identifiers?'. "
        "NEVER use this table without DISTINCT bn_id when counting users (1:N to profile_core)."
    ),
    "profile_segment_tags": (
        "Tag-based segmentation from Mailchimp tags and manual curation. Used for "
        "audience targeting and segment sizing. "
        "GRAIN: one row per (bn_id, tag_category, tag_value). "
        "FOREIGN KEY: bn_id -> profile_core.bn_id. "
        "SOURCE: Mailchimp tags (mailchimp_tag) or manual entry (manual). "
        "UPDATE FREQUENCY: daily. "
        "TYPICAL QUESTIONS: 'How many users tagged VIP?', "
        "'Audience for the ALS list', 'Geographic distribution of patients'. "
        "TAG CATEGORIES: geography, hcp_verification, condition, engagement_segment."
    ),
    "profile_content_affinity": (
        "Per-user content consumption by condition site. Indicates which conditions "
        "a user browses (may differ from their stated preferred_condition). "
        "GRAIN: one row per (bn_id, content_condition). "
        "FOREIGN KEY: bn_id -> profile_core.bn_id; content_condition -> conditions_dict.condition_key. "
        "SOURCE: BN_Acceptor page_load events aggregated by condition site. "
        "UPDATE FREQUENCY: daily. "
        "TYPICAL QUESTIONS: 'Which conditions does this patient browse beyond their primary?', "
        "'What is the audience for ALS content?', 'Top conditions by pageviews'."
    ),
    "profile_preferences": (
        "Consolidated newsletter subscriptions and BuddyBoss/forum settings per user. "
        "GRAIN: one row per bn_id (1:1). "
        "FOREIGN KEY: bn_id -> profile_core.bn_id. "
        "SOURCE: Mailchimp (newsletter), WordPress+BuddyBoss (forum settings). "
        "TYPICAL QUESTIONS: 'Which newsletters does this user subscribe to?', "
        "'How many users opted into forum notifications?'. "
        "newsletter_preferences is ARRAY<STRUCT> - UNNEST it to query. "
        "forum_settings is STRUCT - access via dot notation."
    ),
    "profile_survey_data": (
        "LimeSurvey responses mapped to Common Data Elements (CDE). "
        "GRAIN: one row per (bn_id, survey_id, response_id, field_name). "
        "FOREIGN KEY: bn_id -> profile_core.bn_id. "
        "SOURCE: LimeSurvey via columnar_completed table. "
        "PARTITIONED BY: DATE(response_submitted_at). "
        "TYPICAL QUESTIONS: 'How many users answered the symptom severity question?', "
        "'Average treatment satisfaction by condition', 'Survey participation by persona'. "
        "Survey-respondent linkage: (survey_id, response_id) -> participant_id -> email -> bn_id."
    ),
    "profile_zero_party": (
        "User-provided answers from embedded quizzes, polls, and inline prompts. "
        "GRAIN: one row per response (interaction). "
        "FOREIGN KEY: bn_id -> profile_core.bn_id. "
        "SOURCE: Gravity Forms (polls/quizzes), OptinMonster (inline prompts). "
        "PARTITIONED BY: DATE(responded_at). "
        "TYPICAL QUESTIONS: 'Most popular poll answers', 'Quiz completion by condition', "
        "'Inline prompt engagement'. "
        "interaction_type: quiz | poll | inline_prompt."
    ),
    "site_events": (
        "Event-level activity log for funnel analysis, attribution, and conversion "
        "tracking. Every meaningful interaction is recorded here. "
        "GRAIN: one row per event (event_id is UUID). "
        "FOREIGN KEY: bn_id -> profile_core.bn_id (NULL if anonymous). "
        "SOURCE: BioNews Acceptor -> GA4 -> site_events. "
        "PARTITIONED BY: DATE(event_timestamp). "
        "CLUSTERED BY: event_name, bn_id. "
        "UPDATE FREQUENCY: near real-time (~15 minute lag). "
        "PII LEVEL: MEDIUM (bn_id present; page_path may contain query params). "
        "TYPICAL QUESTIONS: 'How many newsletter signups last month?', "
        "'Funnel from page_view to form_submission', 'Most popular videos by condition'. "
        "ALWAYS add a date filter on event_timestamp - this table has 15M+ rows. "
        "Common event_name values: page_view, scroll, newsletter_signup, form_submission, "
        "user_registration, user_login, ad_impression, ad_click, video_view, "
        "podcast_play, article_comment, forum_interaction, file_download, "
        "external_link_click, cookie_notice_accept, aim_signal."
    ),
    "profile_ad_attribution": (
        "Per-user ad click history. One row per click ID per user. Captures click "
        "IDs from the identity graph (Facebook fbp/fbc, Google gcl_aw/gcl_au). "
        "GRAIN: one row per (bn_id, platform, click_id_type, click_id). "
        "FOREIGN KEY: bn_id -> profile_core.bn_id. "
        "SOURCE: Identity Hub ad click connectors. "
        "TYPICAL QUESTIONS: 'How many users are retargetable on Facebook?', "
        "'Recent Google ad clickers by condition'. "
        "PLATFORM values: facebook | google_ads. "
        "click_id_type values: fbp | fbc | gcl_aw | gcl_au."
    ),
    "conditions_dict": (
        "Master reference table for medical conditions. JOIN to profile_core via "
        "preferred_condition.mesh_id = conditions_dict.mesh_id. "
        "GRAIN: one row per unique condition. "
        "PRIMARY KEY: condition_key. "
        "JOIN KEY: mesh_id (to profile_core.preferred_condition.mesh_id and similar STRUCT fields). "
        "SOURCE: curated by Medical Content Team using MeSH (National Library of Medicine). "
        "UPDATE FREQUENCY: quarterly. "
        "TYPICAL QUESTIONS: 'How many conditions do we track?', "
        "'What is the MeSH ID for ALS?', 'List active conditions'."
    ),
    "treatments_dict": (
        "Master reference table for treatments/medications. JOIN to profile_core's "
        "treatments_current, treatments_of_interest, treatments_discussed via rxnorm_id. "
        "GRAIN: one row per treatment. "
        "PRIMARY KEY: treatment_key. "
        "JOIN KEY: rxnorm_id. "
        "SOURCE: curated by Medical Content Team using RxNorm (NIH/NLM)."
    ),
    "symptoms_dict": (
        "Master reference table for symptoms. JOIN to profile_core.symptom_tags via "
        "mesh_id or hpo_id. "
        "GRAIN: one row per symptom. "
        "PRIMARY KEY: symptom_key. "
        "JOIN KEYS: mesh_id (MeSH), hpo_id (Human Phenotype Ontology). "
        "SOURCE: curated by Medical Content Team."
    ),
    "subtypes_dict": (
        "Master reference table for condition subtypes. JOIN to "
        "profile_core.condition_subtype via business_code or mesh_id. "
        "GRAIN: one row per subtype. "
        "PRIMARY KEY: subtype_key. "
        "FOREIGN KEY: condition_key -> conditions_dict.condition_key."
    ),
    "dictionary_meta": (
        "Bookkeeping for the curated dictionary tables (conditions_dict, symptoms_dict, "
        "treatments_dict, subtypes_dict). Records expected row counts, the external "
        "source used to populate the dictionary, and who last curated it. The Q10 "
        "diagnostic in profile_database_maintenance.sql joins this against the live "
        "dictionary tables to flag drift. GRAIN: one row per dictionary."
    ),
    "profile_lookup": (
        "Consolidated enum/picklist reference for profile_core values "
        "(diagnosis_stage values, gender values, engagement_tier values, etc.). "
        "Single lookup replaces 16 separate ref_ tables. "
        "GRAIN: one row per (lookup_type, lookup_key). "
        "USE CASE: render UI dropdowns; the agent uses this to know valid enum values. "
        "TYPICAL QUERY: SELECT * FROM profile_lookup WHERE lookup_type = 'diagnosis_stage'."
    ),
    # ===== VIEWS =====
    "profile_current": (
        "VIEW. Comprehensive denormalized customer profile (150 columns) joining "
        "profile_core + profile_engagement + key derived metrics into ONE row per "
        "bn_id. Use this when an analyst wants a wide single-table view of 'everything "
        "we know about a person' without writing joins. "
        "GRAIN: one row per bn_id. "
        "PII LEVEL: HIGH (includes email, phone, names). "
        "TYPICAL QUESTIONS: 'Show me the full profile for [user]', 'Export all attributes "
        "of high-engagement patients'. "
        "PREFER OVER: writing manual JOINs of profile_core + profile_engagement."
    ),
    "profile_current_safe": (
        "VIEW. PII-MASKED version of profile_current (60 columns) with email, phone, "
        "and names redacted/hashed. Same one-row-per-bn_id grain, but safe to use in "
        "broader analytics contexts. "
        "PII LEVEL: LOW (PII masked). "
        "TYPICAL QUESTIONS: any aggregate analytics, audience sizing, segment analysis "
        "where raw identifiers are not needed. "
        "PREFER OVER profile_current FOR: all aggregate queries; only use profile_current "
        "when raw PII is explicitly required and authorized."
    ),
    "profile_roles": (
        "VIEW (v6.5). Multi-role persona surface that exposes the 5 boolean role "
        "flags (is_patient, is_hcp, is_caregiver, is_family_or_friend, is_other) "
        "as both an ARRAY<STRING> 'roles' column and a derived 'primary_role' "
        "label using priority hcp > patient > caregiver > family_or_friend > other. "
        "GRAIN: one row per bn_id (returns every profile, including role_count=0). "
        "Includes role_count (0-5) and a role_lineage STRUCT with per-role "
        "(source, confidence, updated_at) for each of the 5 flags. "
        "TYPICAL QUESTIONS: 'Which users are both HCPs and patients?' (WHERE "
        "'hcp' IN UNNEST(roles) AND 'patient' IN UNNEST(roles)); "
        "'Patients with high confidence?' (WHERE role_lineage.patient.confidence "
        ">= 0.9); 'How many multi-role users?' (WHERE role_count > 1). "
        "REPLACES queries against the dropped account_type column."
    ),
    "profile_audience_patients_confirmed": (
        "VIEW. Pre-filtered audience of confirmed patients (v6.5: is_patient = TRUE "
        "AND is_patient_source IS NOT NULL). 60 columns. "
        "GRAIN: one row per confirmed patient bn_id. "
        "TYPICAL QUESTIONS: 'How many confirmed patients with [condition]?', "
        "'Patient campaign audience by region'. "
        "PREFER OVER: filtering profile_core manually when you only want HIGH-CONFIDENCE "
        "patients (excludes profiles where is_patient is NULL)."
    ),
    "profile_audience_caregivers": (
        "VIEW. Pre-filtered audience of caregivers (v6.5: is_caregiver = TRUE). 60 columns "
        "including caregiver_condition, caregiver_relationship, caregiver_focus_areas. "
        "GRAIN: one row per caregiver bn_id. "
        "TYPICAL QUESTIONS: 'Audience size of caregivers focused on financial support', "
        "'Caregivers by relationship type'. "
        "PREFER OVER: filtering profile_core manually for caregiver targeting."
    ),
    "profile_audience_hcp": (
        "VIEW. Pre-filtered audience of healthcare professionals (v6.5: is_hcp = TRUE, "
        "via is_hcp_targetable flag in profile_current_safe). "
        "60 columns including npi_number, specialty, practice location, condition_focus, "
        "credentials. "
        "GRAIN: one row per HCP bn_id. "
        "TYPICAL QUESTIONS: 'HCPs by specialty in California', 'HCPs treating ALS', "
        "'Top HCPs by engagement', 'Verified-NPI HCP audience size'. "
        "PREFER OVER: filtering profile_core manually for HCP targeting."
    ),
    "profile_audience_high_engagement": (
        "VIEW. Pre-filtered audience of highly engaged users (engagement_tier='high' "
        "OR top-percentile activity). 60 columns spanning persona + engagement metrics. "
        "GRAIN: one row per high-engagement bn_id. "
        "TYPICAL QUESTIONS: 'Top users by activity', 'VIP audience by condition', "
        "'High-engagement opt-in rate'. "
        "PREFER OVER: manually filtering profile_engagement for engagement_tier='high'."
    ),
    "profile_marketing_audience": (
        "VIEW. The CAMPAIGN-READY marketing audience: 31 columns optimized for "
        "Mailchimp/marketing exports. Includes only users who are: (1) communication_opt_in=TRUE, "
        "(2) have an email, (3) mailchimp_status='subscribed', (4) not is_suspicious. "
        "GRAIN: one row per emailable bn_id. "
        "PII LEVEL: HIGH (email included - this is the export target). "
        "TYPICAL QUESTIONS: 'How many users can we email about X?', 'Build a Mailchimp "
        "segment of patients with [condition]'. "
        "PREFER OVER: filtering profile_core manually for email campaigns."
    ),
    "profile_analytics_audience": (
        "VIEW. PII-FREE analytics audience for aggregate analysis (33 columns, no email/phone/name). "
        "Includes persona, demographics, engagement summary, and segment tags. "
        "GRAIN: one row per bn_id. "
        "PII LEVEL: NONE (safe for broad sharing, dashboards, BI). "
        "TYPICAL QUESTIONS: 'Audience demographics by condition', 'Engagement by persona "
        "and country', 'Acquisition source breakdown'. "
        "PREFER OVER profile_current FOR: any dashboard or shared report."
    ),
    "profile_ops_audience": (
        "VIEW. Operations-team audience (24 columns) for support, troubleshooting, and "
        "manual outreach. Includes contact info + key persona/engagement fields. "
        "GRAIN: one row per bn_id. "
        "PII LEVEL: HIGH. "
        "TYPICAL QUESTIONS: 'Find user by name/email', 'User support context'. "
        "ACCESS: restricted to ops team."
    ),
    "profile_consent": (
        "VIEW. Per-user consent state across all purposes/categories with DSR "
        "(data subject request) status. 9 columns. "
        "GRAIN: one row per (bn_id, consent_purpose). One user has multiple rows "
        "(one per consent purpose: marketing, analytics, advertising, functional, etc.). "
        "TYPICAL QUESTIONS: 'How many users granted analytics consent?', "
        "'Consent breakdown by purpose', 'Users with pending DSR requests', "
        "'GDPR/CCPA compliance report'. "
        "FIELDS: consent_purpose (marketing|analytics|advertising|functional), "
        "consent_category, consent_granted BOOL, dsr_request_type, dsr_request_status."
    ),
    "profile_contactability": (
        "VIEW. Per-user contactability state: who can we email, retarget, or track? "
        "31 columns combining email + cookie + tracking consent flags. "
        "GRAIN: one row per bn_id (1:1). "
        "TYPICAL QUESTIONS: 'How many users can we email?', 'Retargetable audience "
        "with advertising consent', 'Trackable users by acquisition source'. "
        "USE THIS instead of joining profile_core + profile_engagement for consent "
        "questions."
    ),
    "profile_metrics": (
        "VIEW. One row per person with every headline metric precomputed as a "
        "flag, one definition per metric so every dashboard quotes the same "
        "number. START HERE for BI questions. "
        "GRAIN: one row per bn_id (1:1). "
        "TYPICAL QUESTIONS: 'How many known people?', 'Audience by condition', "
        "'Reachable clinicians', 'Growth on real signup dates'."
    ),
    "profile_coverage": (
        "VIEW. Data quality / field-fill report. Shows what percentage of profiles "
        "have each field populated, broken down by scope -- the population the "
        "check runs over (all | engagement | hcp | patient). 5 columns: scope, "
        "field, filled, total, fill_pct. "
        "GRAIN: one row per (scope, field). "
        "TYPICAL QUESTIONS: 'Which fields are sparsest for patients?', 'How complete "
        "are HCP profiles?', 'Where should we focus enrichment?'. "
        "USE CASE: data quality audits, enrichment prioritization."
    ),
    "profile_engagement_monthly": (
        "VIEW. Monthly time-series of email engagement per user. 5 columns: bn_id, "
        "year_month (YYYY-MM string), email_opens, email_clicks, loaded_at. "
        "GRAIN: one row per (bn_id, year_month). Many rows per user. "
        "TYPICAL QUESTIONS: 'Email engagement trend by month', 'Monthly active emailers', "
        "'Year-over-year engagement comparison'. "
        "USE THIS instead of profile_engagement (which is lifetime aggregates) when you "
        "need time-series."
    ),
    "profile_exceptions": (
        "VIEW. Data-quality exception log. Profiles flagged with anomalies during the "
        "rebuild (suspicious clusters, malformed identifiers, persona conflicts). "
        "3 columns: bn_id, exception_type, detail. "
        "GRAIN: one row per (bn_id, exception_type). "
        "TYPICAL QUESTIONS: 'How many profiles have exceptions?', 'List exception "
        "types', 'Quality issues by source'."
    ),
    "profile_explain": (
        "VIEW. Explainability surface for profile_core fields with source lineage. "
        "Every key field plus its _source, _confidence, and _updated_at companions. "
        "v6.5: includes the 5 role flag lineage triples (is_patient_source/_confidence/"
        "_updated_at and same for is_hcp, is_caregiver, is_family_or_friend, is_other). "
        "GRAIN: one row per bn_id. "
        "TYPICAL QUESTIONS: 'Why is this user classified as a patient?', 'Which "
        "profiles have low-confidence persona assignments?', 'Show me the data lineage "
        "for [bn_id]'. "
        "USE CASE: data governance, audit trail, debugging persona assignments."
    ),
    "profile_events": (
        "VIEW. Filtered/joined view of site_events with profile context. 10 columns: "
        "event_id, bn_id, event_name, event_timestamp, session_id, primary_role "
        "(v6.5 derived from role-flag priority), consent_status, ab_variant + 2 more. "
        "GRAIN: one row per event. "
        "TYPICAL QUESTIONS: 'Event funnel by persona', 'Conversions by A/B variant', "
        "'Recent events for [user]'. "
        "PREFER OVER: site_events when you want events pre-joined to persona/consent."
    ),
    "profile_forum_settings": (
        "VIEW. Flattened forum/BuddyBoss settings per user (12 columns) from the "
        "profile_preferences.forum_settings STRUCT. "
        "GRAIN: one row per bn_id (1:1). "
        "TYPICAL QUESTIONS: 'How many users opted into forum reply notifications?', "
        "'Forum subscribers by visibility setting'. "
        "PREFER OVER: querying profile_preferences.forum_settings STRUCT directly."
    ),
    "profile_newsletter_preferences": (
        "VIEW. Flattened newsletter subscriptions per user (9 columns) from the "
        "profile_preferences.newsletter_preferences ARRAY. "
        "GRAIN: one row per (bn_id, newsletter_key). Many rows per user. "
        "TYPICAL QUESTIONS: 'Subscribers per newsletter', 'How many newsletters does "
        "the average user subscribe to?', 'Newsletter unsubscribe rate by site'. "
        "PREFER OVER: UNNEST-ing profile_preferences.newsletter_preferences manually."
    ),
    "profile_signals": (
        "VIEW. Pre-computed top signals per user: top content condition, segment "
        "categories, geography tag. 17 columns. "
        "GRAIN: one row per bn_id. "
        "TYPICAL QUESTIONS: 'Top content interest by user', 'Geographic distribution', "
        "'Segment-tag rollup'. "
        "USE CASE: quick segment / interest summarization without UNNESTing arrays."
    ),
    "profile_release_status": (
        "VIEW. OPERATIONAL view of the most recent profile_database build. 33 columns "
        "describing the latest rebuild/refresh run: status, duration, step counts, "
        "data freshness. "
        "GRAIN: one row (singleton). "
        "TYPICAL QUESTIONS: 'When was the profile database last refreshed?', "
        "'Did the last build succeed?', 'How long did the rebuild take?'. "
        "USE CASE: ops dashboard / freshness check before running analyses."
    ),
    "profile_build_performance": (
        "VIEW. Historical performance metrics for profile_database builds (37 columns). "
        "Per-build runtime, bytes processed, slot usage, failed steps. "
        "GRAIN: one row per build_id. "
        "TYPICAL QUESTIONS: 'Build performance trend over the last 30 days', "
        "'Which builds failed?', 'Average build duration by mode (rebuild/refresh)'. "
        "USE CASE: ops/data engineering monitoring."
    ),
}


# ---------------------------------------------------------------------------
# COLUMN-LEVEL DESCRIPTIONS
# Keyed by (table, column). Only the columns the agent most needs are described
# below; uncovered columns are left untouched (idempotent).
# ---------------------------------------------------------------------------

COLUMN_DESCRIPTIONS: dict[tuple[str, str], str] = {
    # ---------------- profile_core ----------------
    ("profile_core", "bn_id"): (
        "BioNews Identity ID. Stable, unique, immutable person identifier from the "
        "identity hub. PRIMARY KEY for all profiles. Never NULL on this table. "
        "Use COUNT(DISTINCT bn_id) for user counts. Example: 'bnid_a1b2c3d4'."
    ),
    ("profile_core", "bionews_uk"): (
        "SSO User Key. Cross-site persistent login identifier (BioNews unified key). "
        "NULL for users who never created an SSO account. Used to map app login to bn_id."
    ),
    ("profile_core", "email"): (
        "Primary email address for login and communication. PII LEVEL: HIGH. "
        "Masked for tier-1 users (j***@example.com). NULL for anonymous tier-2 users "
        "(~90% of rows). Resolved priority: Mailchimp > WordPress > LimeSurvey > self-report."
    ),
    ("profile_core", "email_hash"): (
        "SHA-256 hash of email for privacy-safe matching and deduplication. "
        "Use this instead of raw email for joins and audience matching."
    ),
    # v6.5 multi-role flags (replace account_type). Each user can have multiple
    # flags TRUE -- a doctor who is also a patient gets is_hcp=TRUE AND
    # is_patient=TRUE simultaneously. Use profile_data.profile_roles view for
    # multi-role queries and a derived primary_role label.
    ("profile_core", "is_patient"): (
        "v6.5 role flag (BOOL). TRUE if the user is classified as a patient. "
        "Independent of other role flags -- a user can be is_patient=TRUE AND "
        "is_hcp=TRUE simultaneously. NULL until a positive signal sets it. "
        "Filter on this BEFORE querying patient-specific fields (symptom_tags, "
        "treatments_current, diagnosis_stage, condition_subtype). See "
        "profile_roles view for multi-role queries."
    ),
    ("profile_core", "is_patient_source"): (
        "Source string for is_patient. Values: 'npi' (deterministic), "
        "'mailchimp_merge_field' (probabilistic 0.7), 'buddypress_xprofile' "
        "(probabilistic 0.8), 'gravity_forms_poll' (probabilistic 0.7), "
        "'survey' (deterministic 1.0), 'populate_default'."
    ),
    ("profile_core", "is_patient_confidence"): (
        "Confidence 0.0-1.0 that is_patient = TRUE is correct. Deterministic "
        "sources (NPI, survey, app-confirmed) = 1.0; probabilistic sources "
        "(Mailchimp, BuddyPress, polls) range 0.5-0.9. Flag is set TRUE only "
        "when confidence >= PROBABILISTIC_FLAG_THRESHOLD (default 0.5)."
    ),
    ("profile_core", "is_patient_updated_at"): (
        "Timestamp when is_patient was last set/updated by a classifier."
    ),
    ("profile_core", "is_hcp"): (
        "v6.5 role flag (BOOL). TRUE if the user is classified as a healthcare "
        "professional. Independent of other role flags. NULL until a positive "
        "signal sets it. Filter on this BEFORE querying HCP-specific fields "
        "(npi_number, specialty, practice_city, credentials, condition_focus)."
    ),
    ("profile_core", "is_hcp_source"): (
        "Source string for is_hcp. 'npi' (deterministic 1.0 -- federal NPI "
        "registry match), 'mailchimp_merge_field' (0.7), 'buddypress_xprofile' "
        "(0.8), 'populate_default'."
    ),
    ("profile_core", "is_hcp_confidence"): (
        "Confidence 0.0-1.0 that is_hcp = TRUE is correct. NPI matches = 1.0 "
        "(authoritative); self-reported via Mailchimp/BuddyPress = 0.7-0.8."
    ),
    ("profile_core", "is_hcp_updated_at"): (
        "Timestamp when is_hcp was last set/updated by a classifier."
    ),
    ("profile_core", "is_caregiver"): (
        "v6.5 role flag (BOOL). TRUE if the user is classified as a caregiver "
        "(caring for someone with a condition). Independent of other role "
        "flags. NULL until a positive signal sets it. Filter on this BEFORE "
        "querying caregiver-specific fields (caregiver_condition, "
        "caregiver_relationship, caregiver_focus_areas)."
    ),
    ("profile_core", "is_caregiver_source"): (
        "Source string for is_caregiver. Values: 'mailchimp_merge_field' "
        "(0.7), 'buddypress_xprofile' (0.8), 'gravity_forms_poll' (0.7), "
        "'survey' (deterministic 1.0), 'populate_default'."
    ),
    ("profile_core", "is_caregiver_confidence"): (
        "Confidence 0.0-1.0 that is_caregiver = TRUE is correct. Surveys = 1.0; "
        "self-reported via Mailchimp/poll = 0.7; BuddyPress = 0.8."
    ),
    ("profile_core", "is_caregiver_updated_at"): (
        "Timestamp when is_caregiver was last set/updated by a classifier."
    ),
    ("profile_core", "is_family_or_friend"): (
        "v6.5 role flag (BOOL). TRUE if the user is classified as a family "
        "member or friend of someone with a condition (and is not themselves "
        "primarily classified as the patient or caregiver). Independent of "
        "other role flags."
    ),
    ("profile_core", "is_family_or_friend_source"): (
        "Source string for is_family_or_friend. Values: 'mailchimp_merge_field' "
        "(0.7), 'buddypress_xprofile' (0.8), 'gravity_forms_poll' (0.7), "
        "'survey' (deterministic 1.0), 'populate_default'."
    ),
    ("profile_core", "is_family_or_friend_confidence"): (
        "Confidence 0.0-1.0 that is_family_or_friend = TRUE is correct."
    ),
    ("profile_core", "is_family_or_friend_updated_at"): (
        "Timestamp when is_family_or_friend was last set/updated by a classifier."
    ),
    ("profile_core", "is_other"): (
        "v6.5 role flag (BOOL). TRUE ONLY when an explicit positive signal "
        "classifies the user as 'other' (e.g., Mailchimp merge field = 'other'). "
        "NOT a catch-all default -- ~3.25M profiles with account_type='other' in "
        "the v6.4 schema correctly carry is_other = NULL because they had no "
        "positive signal. Team decision: is_other stays NULL until a real "
        "signal arrives."
    ),
    ("profile_core", "is_other_source"): (
        "Source string for is_other. Values: 'mailchimp_merge_field' (0.7), "
        "'buddypress_xprofile' (0.8), 'gravity_forms_poll' (0.7), 'survey' "
        "(deterministic 1.0). 'populate_default' should never appear here -- "
        "is_other is NEVER set by the default-catch-all rule."
    ),
    ("profile_core", "is_other_confidence"): (
        "Confidence 0.0-1.0 that is_other = TRUE is correct. Per team rule, "
        "this is only ever set when an explicit positive signal exists."
    ),
    ("profile_core", "is_other_updated_at"): (
        "Timestamp when is_other was last set/updated by a classifier."
    ),
    ("profile_core", "first_name"): (
        "Given name. PII LEVEL: MEDIUM. Resolution priority: NPI > WordPress > "
        "Mailchimp > LimeSurvey. NULL ~90% (anonymous users)."
    ),
    ("profile_core", "last_name"): (
        "Family name. PII LEVEL: MEDIUM. Same resolution priority as first_name."
    ),
    ("profile_core", "display_name"): (
        "Computed display name (typically 'first_name last_name' or nickname)."
    ),
    ("profile_core", "nickname"): (
        "BuddyBoss/forum display name. May differ from first_name."
    ),
    ("profile_core", "gender"): (
        "Self-reported gender. ENUM: male | female | other | prefer_not_to_say. "
        "NULL means not reported. Sensitive demographic - prefer aggregate analysis."
    ),
    ("profile_core", "age_exact"): (
        "Exact age in years from LimeSurvey self-report. Sensitive PII. "
        "Sparse (~5% fill). Prefer age_band for analytics."
    ),
    ("profile_core", "age_band"): (
        "Bucketed age. ENUM: 18-24 | 25-34 | 35-44 | 45-54 | 55-64 | 65-74 | 75+ | "
        "prefer_not_to_say. Use this for demographic analysis."
    ),
    (
        "profile_core",
        "ethnicity",
    ): "Self-reported ethnicity (LimeSurvey). Sensitive PII.",
    (
        "profile_core",
        "veteran",
    ): "Self-reported US military veteran status (LimeSurvey).",
    (
        "profile_core",
        "primary_language",
    ): "ISO-639 language preference. Platform is English-only; informational.",
    ("profile_core", "short_bio"): "User-authored bio from BuddyPress xprofile.",
    (
        "profile_core",
        "country",
    ): "ISO 3166-1 alpha-2 country code (US, UK, CA, etc.). NULL = unknown location.",
    ("profile_core", "phone"): (
        "10-digit US phone (e.g., '2125551234'). PII LEVEL: HIGH. "
        "From Mailchimp merge fields. Sparse (~15% fill)."
    ),
    ("profile_core", "address_city"): "Home/mailing city. NULL = unknown.",
    (
        "profile_core",
        "address_state",
    ): "US state code (2-letter) or full name. NULL = unknown.",
    (
        "profile_core",
        "address_postal_code",
    ): "Home/mailing ZIP/postal code. PII LEVEL: MEDIUM (geo-identifying).",
    (
        "profile_core",
        "address_country",
    ): "Home/mailing country ISO code (symmetric with country).",
    (
        "profile_core",
        "consent_status",
    ): "Overall consent state. ENUM: granted | denied | pending.",
    (
        "profile_core",
        "terms_accepted_at",
    ): "Timestamp when user accepted Terms of Service.",
    (
        "profile_core",
        "privacy_acknowledged_at",
    ): "Timestamp when user acknowledged privacy policy.",
    ("profile_core", "cookie_consent_date"): "OneTrust cookie consent timestamp.",
    ("profile_core", "tracking_consent"): (
        "Analytics/GA4 tracking consent. TRUE = user allowed tracking. "
        "Required for GA4-derived metrics in queries (filter WHERE tracking_consent = TRUE)."
    ),
    ("profile_core", "communication_opt_in"): (
        "Marketing/newsletter opt-in. TRUE = can email; FALSE = unsubscribed; "
        "NULL = never asked. COMPLIANCE-CRITICAL: always filter "
        "WHERE communication_opt_in = TRUE before targeting campaigns."
    ),
    ("profile_core", "preferred_condition"): (
        "Primary condition of interest. STRUCT<label STRING, mesh_id STRING>. "
        "Access via dot notation: preferred_condition.label, preferred_condition.mesh_id. "
        "JOIN conditions_dict ON preferred_condition.mesh_id = conditions_dict.mesh_id "
        "for full condition details. "
        "Inferred priority: Mailchimp list > content_affinity > app_confirmed > survey."
    ),
    ("profile_core", "preferred_condition_source"): (
        "How preferred_condition was inferred. Values: mailchimp_list, "
        "content_affinity, app_confirmed, mailchimp_tag, survey."
    ),
    ("profile_core", "preferred_condition_confidence"): (
        "Confidence score 0.0-1.0 that preferred_condition is correct. "
        "Decays ~2% per 90 days without re-confirmation. <0.5 = low confidence."
    ),
    (
        "profile_core",
        "preferred_condition_updated_at",
    ): "When preferred_condition was last set/re-inferred.",
    ("profile_core", "preferred_condition_normalized"): (
        "v6.6 normalized condition derived from preferred_condition + conditions_dict. "
        "STRUCT<condition_key STRING, label STRING, mesh_id STRING>. "
        "preferred_condition.label can carry both short codes ('ms') and full names "
        "('Multiple Sclerosis') for the same disease, splitting audiences. This column "
        "collapses every recognized variant to one canonical condition_key. Filter on "
        "preferred_condition_normalized.condition_key (not preferred_condition.label) "
        "for accurate audience queries. NULL when preferred_condition.label is not in "
        "conditions_dict (gap surfaced as 'unnormalized_condition' in profile_exceptions)."
    ),
    ("profile_core", "preferred_condition_normalized_source"): (
        "How the dictionary match was made: dict_match_key (exact condition_key, conf 1.0), "
        "dict_match_alias (alias match, conf 1.0), dict_match_mesh (mesh_id match, conf 1.0), "
        "dict_match_label (case-insensitive label match, conf 0.9)."
    ),
    ("profile_core", "preferred_condition_normalized_confidence"): (
        "Confidence 0.0-1.0 of the dictionary match. 1.0 for key/alias/mesh matches, "
        "0.9 for label-only matches. Lower than preferred_condition_confidence because "
        "this is a deterministic dictionary join, not a probabilistic identity signal."
    ),
    (
        "profile_core",
        "preferred_condition_normalized_updated_at",
    ): "When the normalization was last computed.",
    ("profile_core", "condition_subtype"): (
        "Specific subtype of preferred_condition (e.g., 'Generalized Myasthenia Gravis'). "
        "STRUCT<label STRING, business_code STRING, mesh_id STRING>. "
        "JOIN subtypes_dict ON condition_subtype.business_code = subtypes_dict.business_code."
    ),
    ("profile_core", "condition_subtype_source"): (
        "Where condition_subtype was inferred from. Examples: app_confirmed | "
        "mailchimp_mmerge4 | survey | buddypress_xprofile."
    ),
    ("profile_core", "condition_subtype_confidence"): (
        "Confidence 0.0-1.0 in condition_subtype value. app_confirmed=1.0, "
        "mailchimp_mmerge4=0.8, survey=0.75, buddypress_xprofile=0.7."
    ),
    (
        "profile_core",
        "condition_subtype_updated_at",
    ): "When condition_subtype was last set/re-inferred.",
    ("profile_core", "diagnosis_stage"): (
        "Patient diagnosis stage (v6.5: is_patient = TRUE only). "
        "ENUM: undiagnosed | newly_diagnosed | progressing | stable | unknown. "
        "NULL for non-patients."
    ),
    ("profile_core", "diagnosis_stage_source"): (
        "Where diagnosis_stage was inferred from. Examples: app_confirmed | "
        "gravity_forms_poll | mailchimp_mmerge4 | survey."
    ),
    ("profile_core", "diagnosis_stage_confidence"): (
        "Confidence 0.0-1.0 in diagnosis_stage value. app_confirmed=1.0, "
        "gravity_forms_poll=0.85, mailchimp_mmerge4=0.8, survey=0.75."
    ),
    (
        "profile_core",
        "diagnosis_stage_updated_at",
    ): "When diagnosis_stage was last set/re-inferred.",
    ("profile_core", "diagnosis_timing_band"): (
        "Time since diagnosis (patient only). ENUM: <6mo | 6-24mo | >24mo | prefer_not_to_say. "
        "NULL for non-patients."
    ),
    (
        "profile_core",
        "joined_patient_community",
    ): "TRUE if patient enrolled in community/forums.",
    ("profile_core", "symptom_tags"): (
        "Reported symptoms (patient only). ARRAY<STRUCT<label STRING, mesh_id STRING, hpo_id STRING>>. "
        "UNNEST to query individual symptoms. JOIN symptoms_dict for clinical details."
    ),
    ("profile_core", "treatments_current"): (
        "Medications/treatments currently taken (patient only). "
        "ARRAY<STRUCT<label STRING, rxnorm_id STRING>>. UNNEST to query. "
        "JOIN treatments_dict ON rxnorm_id for clinical details."
    ),
    ("profile_core", "treatments_of_interest"): (
        "Medications the user is researching but may not be on yet. Strong intent signal. "
        "ARRAY<STRUCT<label STRING, rxnorm_id STRING>>."
    ),
    ("profile_core", "treatments_discussed"): (
        "Treatments an HCP has engaged with editorially (not clinical). "
        "ARRAY<STRUCT<label STRING, rxnorm_id STRING>>. HCP only."
    ),
    ("profile_core", "npi_number"): (
        "10-digit National Provider Identifier from NPI Registry. HCP-verified marker. "
        "NULL for non-HCPs. Public information (NOT PII). "
        "Use 'WHERE is_hcp = TRUE AND npi_number IS NOT NULL' for verified HCPs."
    ),
    (
        "profile_core",
        "aim_dgid",
    ): "AIM healthcare professional device group ID. Alternate HCP anchor.",
    ("profile_core", "specialty"): (
        "Medical specialty (HCP only). STRUCT<label STRING, snomed_id STRING>. "
        "NULL for non-HCPs. From NPI registry."
    ),
    (
        "profile_core",
        "specialty_source",
    ): "Source of specialty: npi | buddypress | app_confirmed.",
    (
        "profile_core",
        "specialty_confidence",
    ): "Confidence 0.0-1.0 in specialty value. NPI=1.0, app_confirmed=0.8, self-report=0.5.",
    (
        "profile_core",
        "specialty_updated_at",
    ): "When specialty was last set/re-inferred.",
    ("profile_core", "condition_focus"): (
        "Conditions this HCP focuses on (from AIM data). "
        "ARRAY<STRUCT<label STRING, mesh_id STRING>>. HCP only. UNNEST to query."
    ),
    ("profile_core", "condition_focus_source"): (
        "Where condition_focus was inferred from. Examples: aim_dgid | npi | app_confirmed | inferred."
    ),
    ("profile_core", "condition_focus_confidence"): (
        "Confidence 0.0-1.0 in condition_focus value. aim_dgid=0.9, app_confirmed=1.0, inferred=0.5."
    ),
    (
        "profile_core",
        "condition_focus_updated_at",
    ): "When condition_focus was last set/re-inferred.",
    ("profile_core", "credentials"): "HCP credentials: MD | DO | NP | PA | RN, etc.",
    (
        "profile_core",
        "primary_specialty_code",
    ): "NPI taxonomy code for primary specialty.",
    (
        "profile_core",
        "all_specialties",
    ): "All NPI taxonomy codes (comma-separated or array).",
    (
        "profile_core",
        "provider_organization_name",
    ): "Organization name for organizational NPIs.",
    (
        "profile_core",
        "practice_city",
    ): "HCP practice/office city (may differ from address_city home).",
    (
        "profile_core",
        "practice_state",
    ): "HCP practice/office state (where they practice, not where they live).",
    ("profile_core", "practice_postal_code"): "HCP practice ZIP.",
    ("profile_core", "practice_country"): "HCP practice country ISO code.",
    (
        "profile_core",
        "practice_phone",
    ): "HCP practice telephone (from NPI registry). Public info.",
    (
        "profile_core",
        "practice_setting",
    ): "HCP practice setting: Academic | Hospital | Private Practice | Community Clinic | Telemedicine | Other.",
    ("profile_core", "years_in_practice_band"): "ENUM: 0-2 | 3-5 | 6-10 | 11-20 | 21+.",
    (
        "profile_core",
        "patient_volume_band",
    ): "Patients seen per month (HCP self-report). ENUM: 0-10 | 11-50 | 51-100 | 101+.",
    ("profile_core", "npi_enumeration_date"): "Date NPI was issued (from registry).",
    ("profile_core", "npi_deactivation_date"): "Non-NULL if NPI has been deactivated.",
    ("profile_core", "caregiver_condition"): (
        "Condition of the care recipient (caregiver only). STRUCT<label, mesh_id>. "
        "NULL for non-caregivers."
    ),
    ("profile_core", "caregiver_relationship"): (
        "Relationship to care recipient. ENUM: parent | spouse | sibling | child | "
        "friend | other. NULL for non-caregivers."
    ),
    (
        "profile_core",
        "joined_caregiver_community",
    ): "TRUE if caregiver enrolled in community.",
    ("profile_core", "caregiver_focus_areas"): (
        "Caregiver concerns. ARRAY<STRING> with values: daily_care | logistics | "
        "insurance_financials | advocacy | emotional_support | care_coordination. Max 3."
    ),
    (
        "profile_core",
        "family_condition",
    ): "Condition of family member (family_or_friend persona). STRUCT<label, mesh_id>.",
    (
        "profile_core",
        "family_relationship",
    ): "Relationship to family member: parent | spouse | sibling | child | friend | relative | other.",
    (
        "profile_core",
        "interest_tags",
    ): "Curated topic interests (general/other persona). ARRAY<STRING>.",
    ("profile_core", "follow_conditions"): (
        "Conditions the user follows but is not patient/caregiver for. "
        "ARRAY<STRUCT<label, mesh_id>>. Max 3."
    ),
    (
        "profile_core",
        "content_preferences",
    ): "App-written content format prefs: video | news | podcast | long_form, etc. ARRAY<STRING>.",
    (
        "profile_core",
        "joined_newsletter_topics",
    ): "Convenience rollup of profile_preferences.newsletter_preferences. ARRAY<STRING>.",
    ("profile_core", "clinical_trials_interest"): (
        "TRUE if user expressed interest in clinical trial opportunities (Mailchimp MMERGE4). "
        "Use for trial recruitment audience."
    ),
    (
        "profile_core",
        "patient_community_interest",
    ): "TRUE if user expressed interest in patient community (Mailchimp merge field).",
    (
        "profile_core",
        "hcp_status",
    ): "Computed flag: TRUE if is_hcp = TRUE OR NPI present. Legacy column maintained for back-compat alongside the v6.5 is_hcp flag.",
    (
        "profile_core",
        "identity_confidence_score",
    ): "0-100 composite (DEPRECATED in v6.4; use field-level confidence columns instead).",
    ("profile_core", "ga_user_id"): "GA4 user_id (set on logged-in sessions).",
    ("profile_core", "profile_stage"): (
        "Lifecycle stage. ENUM: S1 (minimal) | S2 (email known) | S3 (demographics) | "
        "S4 (condition known) | S5 (deep journey/survey)."
    ),
    ("profile_core", "profile_completeness"): (
        "Computed 0-100 score: how rich this profile is. Weighted: essential (20%), "
        "demographics (30%), health/specialty (30%), engagement (20%). "
        "Higher = better data. Use for enrichment targeting."
    ),
    ("profile_core", "created_at"): "When this bn_id was first issued (UTC).",
    ("profile_core", "last_active_at"): (
        "MAX activity timestamp across all channels (email, web, community). "
        "NULL = never active. Use for churn: WHERE last_active_at < CURRENT_DATE() - 180."
    ),
    ("profile_core", "profile_updated_at"): "When this profile row was last refreshed.",
    ("profile_core", "acquisition_source"): (
        "First-touch acquisition source. Examples: organic | paid_search | paid_social | "
        "email | direct | referral."
    ),
    (
        "profile_core",
        "acquisition_medium",
    ): "First-touch medium: cpc | organic_search | newsletter | referral | social_paid.",
    ("profile_core", "acquisition_campaign"): "Acquisition campaign name from UTM.",
    ("profile_core", "signup_source"): "Signup source UI: OptinMonster | direct | etc.",
    (
        "profile_core",
        "site_domain",
    ): "Registration site domain (e.g., myasthenia-gravis.com).",
    ("profile_core", "user_photo_url"): "Avatar URL.",
    ("profile_core", "name_prefix"): "Honorific from NPI: Dr., Prof., etc.",
    ("profile_core", "middle_name"): "Middle name from NPI.",
    ("profile_core", "name_suffix"): "Suffix from NPI: Jr, Sr, III, etc.",
    ("profile_core", "cluster_tier"): (
        "Identity confidence tier. ENUM: tier1 (known: has email/SSO) | tier2 "
        "(anonymous trackable: only cookies). Filter cluster_tier='tier1' for CRM/marketing."
    ),
    ("profile_core", "cluster_size"): "Number of identifiers in this bn_id cluster.",
    (
        "profile_core",
        "is_shared_workstation",
    ): "TRUE if 2+ different email subscribers seen on same device. Exclude in per-person analyses.",
    (
        "profile_core",
        "is_suspicious",
    ): "TRUE if cluster_health_score < 60. Exclude in production analyses.",
    (
        "profile_core",
        "cluster_health_score",
    ): "0-100 composite quality score for identity cluster.",
    (
        "profile_core",
        "source_systems",
    ): "Which sources contributed (mailchimp, ga4, npi_registry, etc.). Stored as JSON/string array.",
    # ---------------- profile_engagement ----------------
    ("profile_engagement", "bn_id"): "Foreign key to profile_core.bn_id. 1:1.",
    ("profile_engagement", "mailchimp_status"): (
        "Email subscription status. ENUM: subscribed | unsubscribed | cleaned | "
        "pending | transactional. Filter WHERE mailchimp_status='subscribed' for sendable list."
    ),
    (
        "profile_engagement",
        "mailchimp_member_rating",
    ): "Mailchimp star rating 1-5 (deliverability/engagement health).",
    ("profile_engagement", "mailchimp_vip"): "Mailchimp VIP flag (manually curated).",
    (
        "profile_engagement",
        "email_open_count",
    ): "Lifetime count of unique campaigns opened (distinct campaign_id, not events).",
    (
        "profile_engagement",
        "email_click_count",
    ): "Lifetime count of unique campaigns clicked.",
    (
        "profile_engagement",
        "last_email_open",
    ): "Most recent email open (UTC). NULL = never opened.",
    ("profile_engagement", "last_email_click"): "Most recent email click (UTC).",
    ("profile_engagement", "unique_email_opens"): "Distinct campaigns opened (dedup).",
    ("profile_engagement", "unique_email_clicks"): "Distinct campaigns clicked.",
    ("profile_engagement", "first_email_open"): "First-ever email open timestamp.",
    ("profile_engagement", "member_rating"): "Numeric subscriber rating (1-5).",
    ("profile_engagement", "preferred_email_hour"): (
        "Best send hour 0-23 (UTC). Only set when >=5 click events observed "
        "(thresholded for reliability). Use for send-time optimization."
    ),
    (
        "profile_engagement",
        "preferred_email_dow",
    ): "Best day of week 1=Sun..7=Sat. Same >=5-click threshold.",
    ("profile_engagement", "first_seen_web"): "First GA4/web session timestamp.",
    (
        "profile_engagement",
        "last_seen_web",
    ): "Most recent web session. Use for recency/churn analysis.",
    ("profile_engagement", "total_sessions"): "Lifetime GA4 session count.",
    ("profile_engagement", "total_pageviews"): "Lifetime GA4 pageview count.",
    (
        "profile_engagement",
        "days_retained",
    ): "Days between ga4_first_visit_date and ga4_last_visit_date. High = loyal.",
    ("profile_engagement", "ga4_first_visit_date"): "First visit date from GA4.",
    ("profile_engagement", "ga4_last_visit_date"): "Most recent visit from GA4.",
    ("profile_engagement", "engagement_tier"): (
        "COMPUTED activity tier. ENUM: high | medium | low | inactive. "
        "Use this for 'active users' or 'DAU' questions. Recomputed daily. "
        "Logic: high = top 25% activity; medium = 50-75th pct; low = 25-50th; "
        "inactive = no activity > 180 days OR last_active_at IS NULL."
    ),
    (
        "profile_engagement",
        "predicted_ltv",
    ): "Predicted lifetime value in USD (ad revenue model). Computed monthly.",
    (
        "profile_engagement",
        "total_form_submissions",
    ): "Form submits (newsletter signups, contact forms, polls).",
    ("profile_engagement", "total_ad_impressions"): "Lifetime GAM ad impressions seen.",
    ("profile_engagement", "total_ad_clicks"): "Lifetime ad clicks.",
    ("profile_engagement", "total_video_views"): "JW Player video plays.",
    ("profile_engagement", "total_file_downloads"): "PDF/resource downloads.",
    ("profile_engagement", "total_comments"): "Article comments posted.",
    (
        "profile_engagement",
        "total_forum_interactions",
    ): "Forum posts + replies + reactions.",
    (
        "profile_engagement",
        "total_engagement_time_sec",
    ): "Lifetime GA4 active engagement time in seconds.",
    ("profile_engagement", "total_scrolls"): "Lifetime scroll events.",
    ("profile_engagement", "total_searches"): "Lifetime on-site search events.",
    ("profile_engagement", "total_shares"): "Lifetime content share events.",
    ("profile_engagement", "total_logins"): "Lifetime login count.",
    ("profile_engagement", "total_signups"): "Lifetime signup events.",
    (
        "profile_engagement",
        "avg_scroll_depth",
    ): "Average max scroll % per session (0-100).",
    (
        "profile_engagement",
        "avg_session_duration_sec",
    ): "Average session length in seconds.",
    ("profile_engagement", "avg_session_depth"): "Average pages per session.",
    ("profile_engagement", "avg_ctr"): "Average CTR on recommended content (0.0-1.0).",
    ("profile_engagement", "viewability_score"): "Ad viewability score (0.0-1.0).",
    (
        "profile_engagement",
        "content_clusters",
    ): "Top content topic clusters as JSON/array: treatment | research | living_with | etc.",
    (
        "profile_engagement",
        "wordpress_registered_at",
    ): "WordPress registration timestamp.",
    (
        "profile_engagement",
        "wordpress_comment_count",
    ): "WordPress article comment count.",
    ("profile_engagement", "survey_response_count"): "LimeSurvey responses count.",
    (
        "profile_engagement",
        "first_facebook_click",
    ): "First Facebook/Meta ad click. Use for attribution.",
    ("profile_engagement", "last_facebook_click"): "Most recent Facebook ad click.",
    ("profile_engagement", "first_google_click"): "First Google ad click.",
    ("profile_engagement", "last_google_click"): "Most recent Google ad click.",
    (
        "profile_engagement",
        "primary_device",
    ): "Most recent device: desktop | mobile | tablet.",
    ("profile_engagement", "primary_browser"): "Most recent browser.",
    ("profile_engagement", "primary_os"): "Most recent OS.",
    (
        "profile_engagement",
        "ga4_country",
    ): "Most recent geo country from GA4. May override profile_core.country.",
    ("profile_engagement", "ga4_city"): "Most recent geo city from GA4.",
    ("profile_engagement", "ga4_channel_grouping"): (
        "GA4 acquisition channel: Organic Search | Paid Search | Social | Email | "
        "Direct | Referral."
    ),
    (
        "profile_engagement",
        "consent_analytics",
    ): "Analytics/measurement cookie consent (OneTrust).",
    (
        "profile_engagement",
        "consent_advertising",
    ): "Advertising/retargeting cookie consent.",
    (
        "profile_engagement",
        "consent_functional",
    ): "Functional/performance cookie consent.",
    ("profile_engagement", "consent_updated_at"): "When consent state last changed.",
    (
        "profile_engagement",
        "bp_activity_count",
    ): "Total BuddyPress activities (posts, updates, replies).",
    (
        "profile_engagement",
        "bp_last_activity",
    ): "Most recent BuddyPress activity timestamp.",
    ("profile_engagement", "bp_follower_count"): "BuddyPress followers count.",
    ("profile_engagement", "bp_following_count"): "BuddyPress following count.",
    (
        "profile_engagement",
        "bp_friend_count",
    ): "Confirmed friend connections in BuddyPress.",
    ("profile_engagement", "bp_group_count"): "Group memberships.",
    (
        "profile_engagement",
        "bp_is_group_admin",
    ): "TRUE if admin/mod of any BuddyPress group.",
    (
        "profile_engagement",
        "bp_reaction_count",
    ): "Reactions given on community content.",
    (
        "profile_engagement",
        "has_paid_subscription",
    ): "TRUE if active paid membership (Paid Memberships Subscriptions).",
    (
        "profile_engagement",
        "subscription_status",
    ): "PMS status: active | expired | pending | cancelled.",
    ("profile_engagement", "loaded_at"): "When this row was inserted by the pipeline.",
    ("profile_engagement", "updated_at"): "When this row was last updated.",
    # ---------------- profile_identifiers ----------------
    (
        "profile_identifiers",
        "bn_id",
    ): "Foreign key to profile_core.bn_id. Many rows per bn_id (1:N).",
    ("profile_identifiers", "identifier_type"): (
        "Identifier type. Common values: email | phone | npi_number | bionews_uk | "
        "wp_user_id | bnfpvid | mc_euid | client_id | fbp | gcl_au | aim_dgid | "
        "participant_id | subscriber_hash. Plus 25+ less-common types."
    ),
    (
        "profile_identifiers",
        "identifier_value",
    ): "Actual identifier value (e.g., 'john@example.com'). PII level depends on type.",
    (
        "profile_identifiers",
        "source_system",
    ): "Where this identifier was observed: mailchimp | wordpress | ga4 | acceptor | npi_registry | limesurvey.",
    (
        "profile_identifiers",
        "confidence",
    ): "Match confidence 0.0-1.0 that this identifier belongs to this bn_id.",
    ("profile_identifiers", "first_seen"): "First time this identifier was linked.",
    ("profile_identifiers", "last_seen"): "Most recent observation of this identifier.",
    (
        "profile_identifiers",
        "is_primary",
    ): "TRUE if this is the preferred form of this type (e.g., primary email vs aliases).",
    ("profile_identifiers", "loaded_at"): "Row insertion timestamp.",
    # ---------------- profile_segment_tags ----------------
    ("profile_segment_tags", "bn_id"): "Foreign key to profile_core.bn_id.",
    ("profile_segment_tags", "tag_category"): (
        "Tag category. ENUM: geography | hcp_verification | condition | engagement_segment."
    ),
    (
        "profile_segment_tags",
        "tag_value",
    ): "Tag value. Examples: state_california | verified_npi | condition_als | high_engagement.",
    ("profile_segment_tags", "source"): "Source: mailchimp_tag | manual.",
    ("profile_segment_tags", "first_seen"): "When this tag was first applied.",
    ("profile_segment_tags", "loaded_at"): "Row load timestamp.",
    # ---------------- profile_content_affinity ----------------
    ("profile_content_affinity", "bn_id"): "Foreign key to profile_core.bn_id.",
    ("profile_content_affinity", "content_condition"): (
        "Condition site key (als | mg | nmo | etc.). Matches conditions_dict.condition_key. "
        "JOIN conditions_dict ON content_condition = conditions_dict.condition_key."
    ),
    (
        "profile_content_affinity",
        "pageview_count",
    ): "Lifetime pageviews on this condition's content.",
    (
        "profile_content_affinity",
        "active_days",
    ): "Distinct days user viewed this condition.",
    ("profile_content_affinity", "first_viewed"): "First view timestamp.",
    ("profile_content_affinity", "last_viewed"): "Most recent view timestamp.",
    (
        "profile_content_affinity",
        "affinity_score",
    ): "Recency * Frequency decay score (0.0-1.0). Higher = stronger affinity.",
    ("profile_content_affinity", "loaded_at"): "Row load timestamp.",
    # ---------------- profile_preferences ----------------
    ("profile_preferences", "bn_id"): "Foreign key to profile_core.bn_id. 1:1.",
    ("profile_preferences", "newsletter_preferences"): (
        "ARRAY<STRUCT<site_domain, newsletter_key, newsletter_label, is_subscribed BOOL, "
        "subscribed_at, unsubscribed_at, source>>. UNNEST to query individual subscriptions."
    ),
    ("profile_preferences", "forum_settings"): (
        "STRUCT<notify_forum_replies, notify_group_invites, notify_direct_messages, "
        "notify_mentions, subscribed_forum_ids ARRAY<STRING>, subscribed_group_ids "
        "ARRAY<STRING>, profile_visibility, show_activity_feed, forum_registered_at, "
        "last_forum_activity>. Access via dot notation."
    ),
    ("profile_preferences", "loaded_at"): "Row load timestamp.",
    # ---------------- profile_survey_data ----------------
    ("profile_survey_data", "bn_id"): "Foreign key to profile_core.bn_id.",
    ("profile_survey_data", "survey_id"): "LimeSurvey survey ID.",
    (
        "profile_survey_data",
        "response_id",
    ): "LimeSurvey unique response ID. (survey_id, response_id) identifies a respondent.",
    (
        "profile_survey_data",
        "cde_id",
    ): "Common Data Element ID (standardized question identifier).",
    ("profile_survey_data", "field_name"): "Question key/identifier.",
    ("profile_survey_data", "value_text"): "Text answer.",
    ("profile_survey_data", "value_numeric"): "Numeric answer.",
    ("profile_survey_data", "value_code"): "Coded answer (for multi-choice).",
    ("profile_survey_data", "value_array"): "Multi-select answers as JSON/array.",
    (
        "profile_survey_data",
        "response_submitted_at",
    ): "When survey response was submitted. Table partitioned by DATE(this).",
    (
        "profile_survey_data",
        "mapping_confidence",
    ): "Confidence 0.0-1.0 that question was correctly mapped to CDE.",
    ("profile_survey_data", "loaded_at"): "Row load timestamp.",
    # ---------------- profile_zero_party ----------------
    ("profile_zero_party", "bn_id"): "Foreign key to profile_core.bn_id.",
    ("profile_zero_party", "interaction_type"): "Type: quiz | poll | inline_prompt.",
    (
        "profile_zero_party",
        "interaction_id",
    ): "Identifier of the specific quiz/poll/prompt.",
    ("profile_zero_party", "question_text"): "The question that was asked.",
    (
        "profile_zero_party",
        "answer_value",
    ): "The user's answer (text or selected option).",
    ("profile_zero_party", "answer_score"): "Numeric score (quiz scoring).",
    ("profile_zero_party", "site_domain"): "Where the interaction occurred.",
    ("profile_zero_party", "page_path"): "Page where the prompt appeared.",
    (
        "profile_zero_party",
        "responded_at",
    ): "When user responded. Table partitioned by DATE(this).",
    ("profile_zero_party", "loaded_at"): "Row load timestamp.",
    # ---------------- site_events ----------------
    ("site_events", "event_id"): "Unique event UUID.",
    (
        "site_events",
        "bn_id",
    ): "Foreign key to profile_core.bn_id. NULL for anonymous events.",
    (
        "site_events",
        "bnfpvid",
    ): "BioNews persistent visitor ID (browser-level). Always present.",
    ("site_events", "client_id"): "GA4 client ID.",
    ("site_events", "event_name"): (
        "Canonical event name. Common values: page_view | scroll | newsletter_signup | "
        "form_submission | user_registration | user_login | poll_submission | "
        "ad_impression | ad_click | sponsor_link_click | video_view | podcast_play | "
        "article_comment | forum_interaction | file_download | external_link_click | "
        "cookie_notice_accept | cookie_notice_reject | aim_signal | hcp_conversion."
    ),
    (
        "site_events",
        "event_category",
    ): "engagement | conversion | ad | media | community | compliance | hcp | paid.",
    (
        "site_events",
        "event_timestamp",
    ): "Event timestamp (UTC). Table partitioned by DATE(this). ALWAYS add a date filter on this column.",
    ("site_events", "session_id"): "GA4 session identifier.",
    ("site_events", "site_domain"): "Which property (e.g., myasthenia-gravis.com).",
    ("site_events", "page_path"): "URL path of the page where event occurred.",
    ("site_events", "page_title"): "Page title at event time.",
    (
        "site_events",
        "event_label",
    ): "Type-specific label (form name, ad unit, video title).",
    (
        "site_events",
        "event_value",
    ): "Monetary value (USD) for ad_impression/ad_click events.",
    ("site_events", "event_detail"): "JSON blob with event-type-specific fields.",
    (
        "site_events",
        "traffic_source",
    ): "UTM source: google | facebook | newsletter | direct | etc.",
    ("site_events", "traffic_medium"): "UTM medium: cpc | organic | email | social.",
    ("site_events", "traffic_campaign"): "UTM campaign name.",
    ("site_events", "device_category"): "desktop | mobile | tablet.",
    ("site_events", "user_agent"): "Browser user-agent string.",
    ("site_events", "consent_status"): "Consent state at the time of the event.",
    ("site_events", "loaded_at"): "Row load timestamp.",
    # ---------------- profile_ad_attribution ----------------
    ("profile_ad_attribution", "bn_id"): "Foreign key to profile_core.bn_id.",
    ("profile_ad_attribution", "platform"): "Ad platform: facebook | google_ads.",
    (
        "profile_ad_attribution",
        "click_id_type",
    ): "Type of click ID: fbp | fbc | gcl_aw | gcl_au.",
    ("profile_ad_attribution", "click_id"): "The actual click ID value.",
    (
        "profile_ad_attribution",
        "first_seen",
    ): "First time this click ID was observed for this user.",
    (
        "profile_ad_attribution",
        "last_seen",
    ): "Most recent observation of this click ID.",
    ("profile_ad_attribution", "loaded_at"): "Row load timestamp.",
    # ---------------- conditions_dict ----------------
    (
        "conditions_dict",
        "condition_key",
    ): "Machine key (snake_case). PRIMARY KEY. Examples: myasthenia_gravis | als | nmo.",
    ("conditions_dict", "label"): "Display name. Example: 'Myasthenia Gravis'.",
    ("conditions_dict", "mesh_id"): (
        "MeSH (Medical Subject Headings) descriptor ID. JOIN KEY to profile_core's "
        "STRUCT columns (preferred_condition.mesh_id, condition_subtype.mesh_id, etc.). "
        "Example: 'D009157' = Myasthenia Gravis."
    ),
    (
        "conditions_dict",
        "mesh_tree_number",
    ): "MeSH tree number for hierarchy (e.g., 'C10.668.758.550').",
    (
        "conditions_dict",
        "icd10_code",
    ): "ICD-10 diagnostic code if applicable (e.g., G70.0 for MG).",
    (
        "conditions_dict",
        "condition_category",
    ): "Grouping: neuromuscular | autoimmune | rare_disease | etc.",
    (
        "conditions_dict",
        "aliases",
    ): "Alternative names for search/matching (JSON array).",
    ("conditions_dict", "description"): "Brief clinical description.",
    ("conditions_dict", "sort_order"): "UI display order.",
    (
        "conditions_dict",
        "is_active",
    ): "TRUE if currently tracked. FALSE = historical/retired.",
    ("conditions_dict", "created_at"): "When this row was created.",
    ("conditions_dict", "updated_at"): "When this row was last updated.",
    # ---------------- treatments_dict ----------------
    ("treatments_dict", "treatment_key"): "Machine key. PRIMARY KEY. Example: vyvgart.",
    ("treatments_dict", "label"): "Brand name. Example: 'Vyvgart'.",
    (
        "treatments_dict",
        "generic_name",
    ): "Generic/INN name. Example: 'efgartigimod alfa'.",
    ("treatments_dict", "rxnorm_id"): (
        "RxNorm concept ID (NIH standardized drug vocabulary). JOIN KEY to "
        "profile_core's treatments_current.rxnorm_id, treatments_of_interest.rxnorm_id, etc."
    ),
    ("treatments_dict", "ndc_code"): "National Drug Code if applicable.",
    (
        "treatments_dict",
        "treatment_class",
    ): "Class: biologic | immunosuppressant | symptomatic | surgical | supportive.",
    (
        "treatments_dict",
        "applicable_conditions",
    ): "Conditions this treats (JSON array of condition_key values).",
    (
        "treatments_dict",
        "fda_approval_year",
    ): "Year of FDA approval (NULL if not FDA-approved).",
    ("treatments_dict", "description"): "Treatment description.",
    ("treatments_dict", "sort_order"): "UI display order.",
    ("treatments_dict", "is_active"): "TRUE if currently tracked.",
    ("treatments_dict", "created_at"): "When this row was created.",
    ("treatments_dict", "updated_at"): "When this row was last updated.",
    # ---------------- symptoms_dict ----------------
    ("symptoms_dict", "symptom_key"): "Machine key. PRIMARY KEY. Example: fatigue.",
    ("symptoms_dict", "label"): "Display label. Example: 'Fatigue'.",
    (
        "symptoms_dict",
        "mesh_id",
    ): "MeSH descriptor ID (e.g., D005221). JOIN KEY to profile_core.symptom_tags.mesh_id.",
    ("symptoms_dict", "hpo_id"): "Human Phenotype Ontology ID (e.g., HP:0012378).",
    ("symptoms_dict", "snomed_id"): "SNOMED CT concept ID if applicable.",
    (
        "symptoms_dict",
        "applicable_conditions",
    ): "Conditions this symptom applies to (JSON array of condition_key).",
    ("symptoms_dict", "aliases"): "Alternative names for search.",
    (
        "symptoms_dict",
        "severity_scale",
    ): "Scale type: binary | mild_moderate_severe | 1_10.",
    ("symptoms_dict", "description"): "Symptom description.",
    ("symptoms_dict", "sort_order"): "UI order.",
    ("symptoms_dict", "is_active"): "TRUE if active.",
    ("symptoms_dict", "created_at"): "Row created timestamp.",
    ("symptoms_dict", "updated_at"): "Row updated timestamp.",
    # ---------------- subtypes_dict ----------------
    (
        "subtypes_dict",
        "subtype_key",
    ): "Machine key. PRIMARY KEY. Example: mg_gen (Generalized Myasthenia Gravis).",
    ("subtypes_dict", "condition_key"): "Foreign key to conditions_dict.condition_key.",
    ("subtypes_dict", "label"): "Display label. Example: 'Generalized'.",
    ("subtypes_dict", "business_code"): (
        "Business targeting code (e.g., 'mg_gen'). JOIN KEY to "
        "profile_core.condition_subtype.business_code."
    ),
    ("subtypes_dict", "mesh_id"): "MeSH ID for this subtype.",
    ("subtypes_dict", "snomed_code"): "SNOMED CT code.",
    ("subtypes_dict", "clinical_description"): "Clinical description of subtype.",
    ("subtypes_dict", "sort_order"): "UI order.",
    ("subtypes_dict", "is_active"): "TRUE if active.",
    ("subtypes_dict", "created_at"): "Row created timestamp.",
    (
        "subtypes_dict",
        "updated_at",
    ): "Row last-updated timestamp (curator edits or dictionary refresh).",
    # ---------------- dictionary_meta ----------------
    (
        "dictionary_meta",
        "dictionary_name",
    ): "Name of the dictionary this row tracks. PRIMARY KEY. Examples: conditions_dict | symptoms_dict | treatments_dict | subtypes_dict.",
    (
        "dictionary_meta",
        "entry_count",
    ): "Expected number of active rows in the dictionary (set by curator). Compare against live SELECT COUNT(*) to detect drift.",
    (
        "dictionary_meta",
        "external_source",
    ): "External system the dictionary was sourced from (e.g., MeSH, RxNorm, SNOMED CT, internal-curation).",
    (
        "dictionary_meta",
        "external_source_version",
    ): "Version/release of the external source (e.g., MeSH 2026, RxNorm 2026-04-01). NULL if internally curated.",
    (
        "dictionary_meta",
        "last_curated_at",
    ): "When a human last reviewed or edited the dictionary contents.",
    (
        "dictionary_meta",
        "last_curated_by",
    ): "Email or handle of the person who last curated this dictionary.",
    (
        "dictionary_meta",
        "notes",
    ): "Free-text curator notes (gotchas, special cases, refresh cadence, owners).",
    (
        "dictionary_meta",
        "loaded_at",
    ): "When this dictionary_meta row was last refreshed.",
    # ---------------- profile_lookup ----------------
    (
        "profile_lookup",
        "lookup_type",
    ): "Category. Examples: diagnosis_stage | gender | engagement_tier | condition_subtype.",
    (
        "profile_lookup",
        "lookup_key",
    ): "Machine value within the category (e.g., 'newly_diagnosed' under lookup_type='diagnosis_stage').",
    ("profile_lookup", "label"): "Display label for UI.",
    ("profile_lookup", "description"): "Optional longer description.",
    (
        "profile_lookup",
        "metadata",
    ): "JSON string with type-specific fields (mesh_id, snomed_id, etc.).",
    ("profile_lookup", "sort_order"): "UI sort order within the lookup_type.",
    ("profile_lookup", "is_active"): "TRUE if active.",
    ("profile_lookup", "created_at"): "Row created timestamp.",
    # ---------------- profile_current (view-derived columns) ----------------
    (
        "profile_current",
        "identity_confidence_tier",
    ): "Derived tier from identity_confidence_score: tier1 (>=90) | tier2 (>=50) | tier3 (<50).",
    (
        "profile_current",
        "primary_condition_label",
    ): "Label of preferred_condition (denormalized from STRUCT) for SELECT * convenience.",
    (
        "profile_current",
        "primary_subtype_label",
    ): "Label of condition_subtype (denormalized from STRUCT) for SELECT * convenience.",
    (
        "profile_current",
        "primary_persona_label",
    ): "v6.5 derived label representing the highest-confidence active role: 'patient' | 'hcp' | 'caregiver' | 'family_or_friend' | 'other'. Replaces v6.4 account_type. Multi-role users — use profile_roles for the full ARRAY.",
    (
        "profile_current",
        "primary_interest_label",
    ): "First entry in interest_tags (general/other persona).",
    (
        "profile_current",
        "top_content_condition",
    ): "Highest-affinity condition from profile_content_affinity (denormalized for convenience).",
    (
        "profile_current",
        "top_affinity_score",
    ): "Affinity score (0-1) for top_content_condition.",
    (
        "profile_current",
        "top_condition_pageviews",
    ): "Pageviews behind top_content_condition.",
    (
        "profile_current",
        "geo_tag",
    ): "Coarse geography rollup (country or region) for segmentation.",
    (
        "profile_current",
        "hcp_verified_tag",
    ): "Marker label when is_hcp=TRUE AND npi_number IS NOT NULL; NULL otherwise.",
    (
        "profile_current",
        "segment_categories",
    ): "Distinct categories rolled up from profile_segment_tags. ARRAY<STRING>.",
    (
        "profile_current",
        "ad_platforms",
    ): "Distinct ad platforms touched (from profile_ad_attribution). ARRAY<STRING>.",
    ("profile_current", "first_ad_click_at"): "Earliest ad attribution timestamp.",
    ("profile_current", "last_ad_click_at"): "Latest ad attribution timestamp.",
    (
        "profile_current",
        "has_paid_media_signal",
    ): "TRUE if any ad attribution rows exist.",
    (
        "profile_current",
        "last_activity_any",
    ): "GREATEST of last_active_at, last_email_*, last_seen_web — single liveness anchor.",
    (
        "profile_current",
        "months_since_last_email",
    ): "Months since last_email_open or last_email_click, whichever is later. NULL if never.",
    (
        "profile_current",
        "months_since_last_web",
    ): "Months since last_seen_web. NULL if never.",
    ("profile_current", "active_last_30d"): "TRUE if last_activity_any within 30 days.",
    ("profile_current", "active_last_90d"): "TRUE if last_activity_any within 90 days.",
    (
        "profile_current",
        "repeat_engager",
    ): "TRUE if multiple engagement signals fired in the lookback window.",
    (
        "profile_current",
        "email_recently_engaged",
    ): "TRUE if last_email_open or last_email_click within 30 days.",
    (
        "profile_current",
        "has_any_interaction",
    ): "TRUE if any engagement metric (>0) or any last_* timestamp is non-NULL.",
    (
        "profile_current",
        "is_marketing_eligible",
    ): "TRUE if consent + contactability gates pass and no DSR suppression.",
    (
        "profile_current",
        "is_personalization_eligible",
    ): "TRUE if tracking_consent and analytics consent both granted.",
    (
        "profile_current",
        "is_hcp_targetable",
    ): "TRUE if is_hcp = TRUE AND can_email_market AND no DSR. Used by profile_audience_hcp.",
    (
        "profile_current",
        "is_research_candidate",
    ): "TRUE if clinical_trials_interest = TRUE AND patient role active AND contactable.",
    (
        "profile_current",
        "is_high_confidence_profile",
    ): "TRUE if identity_confidence_score >= 90.",
    # ---------------- profile_current_safe / profile_audience_* (shared derived) ----------------
    # Inherits all base columns from profile_core/profile_engagement via the inheritance map.
    (
        "profile_current_safe",
        "primary_persona_label",
    ): "v6.5 derived label of the highest-confidence active role. See profile_current.primary_persona_label.",
    (
        "profile_current_safe",
        "is_hcp_targetable",
    ): "TRUE if is_hcp = TRUE AND can_email_market AND no DSR.",
    (
        "profile_audience_caregivers",
        "primary_persona_label",
    ): "Derived primary role label (see profile_current). Filter is is_caregiver = TRUE.",
    (
        "profile_audience_caregivers",
        "is_hcp_targetable",
    ): "Inherited from profile_current_safe.",
    (
        "profile_audience_hcp",
        "primary_persona_label",
    ): "Derived primary role label. Filter is is_hcp_targetable = TRUE.",
    (
        "profile_audience_hcp",
        "is_hcp_targetable",
    ): "Filter column for this view: TRUE only.",
    (
        "profile_audience_high_engagement",
        "primary_persona_label",
    ): "Derived primary role label. Filter is engagement_tier = 'high'.",
    (
        "profile_audience_high_engagement",
        "is_hcp_targetable",
    ): "Inherited from profile_current_safe.",
    (
        "profile_audience_patients_confirmed",
        "primary_persona_label",
    ): "Derived primary role label. Filter is is_patient = TRUE AND is_patient_source IS NOT NULL.",
    (
        "profile_audience_patients_confirmed",
        "is_hcp_targetable",
    ): "Inherited from profile_current_safe.",
    (
        "profile_analytics_audience",
        "primary_persona_label",
    ): "Derived primary role label. Filter is can_analytics_track = TRUE.",
    (
        "profile_marketing_audience",
        "primary_persona_label",
    ): "Derived primary role label. Filter is can_email_market = TRUE.",
    (
        "profile_marketing_audience",
        "can_email_market",
    ): "Filter column: TRUE if consent + contactability allow marketing email.",
    (
        "profile_marketing_audience",
        "can_personalize",
    ): "Filter column: TRUE if a condition is on file AND advertising consent (GA4 OneTrust group C0004) was observed. NULL consent counts as FALSE.",
    (
        "profile_marketing_audience",
        "contactability_status",
    ): "Rolled-up label of effective marketing status. See profile_contactability.",
    (
        "profile_marketing_audience",
        "marketing_status_reason",
    ): "Why marketing is allowed/disallowed for this profile.",
    (
        "profile_marketing_audience",
        "consent_last_updated_at",
    ): "MAX of consent timestamps on the profile.",
    (
        "profile_ops_audience",
        "primary_persona_label",
    ): "Derived primary role label. Used by ops-side audience exports.",
    (
        "profile_ops_audience",
        "contactability_status",
    ): "Rolled-up label of effective marketing status.",
    (
        "profile_ops_audience",
        "marketing_status_reason",
    ): "Why marketing is allowed/disallowed.",
    (
        "profile_ops_audience",
        "can_email_market",
    ): "TRUE if consent + contactability allow marketing email.",
    (
        "profile_ops_audience",
        "can_personalize",
    ): "TRUE if a condition is on file AND advertising consent (GA4 OneTrust group C0004) was observed. NULL consent counts as FALSE.",
    (
        "profile_ops_audience",
        "can_analytics_track",
    ): "TRUE if analytics consent granted.",
    ("profile_ops_audience", "can_advertise"): "TRUE if advertising consent granted.",
    ("profile_ops_audience", "consent_last_updated_at"): "MAX of consent timestamps.",
    # ---------------- profile_consent (view-derived) ----------------
    (
        "profile_consent",
        "consent_purpose",
    ): "Specific consent purpose: marketing | analytics | personalization | advertising.",
    (
        "profile_consent",
        "consent_category",
    ): "Category grouping (OneTrust C0001/C0002/etc. mapping).",
    (
        "profile_consent",
        "consent_granted",
    ): "TRUE if this purpose is currently granted.",
    (
        "profile_consent",
        "dsr_request_type",
    ): "Data subject request type: access | deletion | suppression. NULL if no DSR.",
    (
        "profile_consent",
        "dsr_request_status",
    ): "DSR status: open | in_progress | completed | rejected.",
    ("profile_consent", "dsr_requested_at"): "When the DSR was filed.",
    # ---------------- profile_contactability (view-derived) ----------------
    (
        "profile_contactability",
        "has_email_channel",
    ): "TRUE if a deliverable email is on file.",
    (
        "profile_contactability",
        "can_email_market",
    ): "TRUE if consent + suppression checks pass for marketing email.",
    (
        "profile_contactability",
        "can_personalize",
    ): "TRUE if a condition is on file AND advertising consent (GA4 OneTrust group C0004) was observed. NULL consent counts as FALSE.",
    (
        "profile_contactability",
        "can_analytics_track",
    ): "TRUE if analytics consent granted.",
    ("profile_contactability", "can_advertise"): "TRUE if advertising consent granted.",
    (
        "profile_contactability",
        "marketing_status_reason",
    ): "Why marketing is allowed/disallowed (e.g., 'unsubscribed', 'no_consent', 'dsr_suppression', 'eligible').",
    (
        "profile_contactability",
        "contactability_status",
    ): "Single rolled-up label: contactable | suppressed | unknown.",
    ("profile_contactability", "consent_last_updated_at"): "MAX of consent timestamps.",
    # ---------------- profile_metrics (view) ----------------
    ("profile_metrics", "bn_id"): "Stable identity-graph person ID; the join key everywhere.",
    ("profile_metrics", "cluster_tier"): "tier1 (known person: durable anchor) | tier2 (anonymous cookie-only browser).",
    ("profile_metrics", "site_domain"): "Registration-origin site short code; ~7% filled. Not browsing.",
    ("profile_metrics", "country"): "ISO country code.",
    ("profile_metrics", "condition_key"): "Canonical condition key from conditions_dict. Filter on this, not raw labels.",
    ("profile_metrics", "condition_label"): "Canonical condition display label matching condition_key.",
    ("profile_metrics", "primary_role"): "Mutually exclusive persona (priority hcp > patient > caregiver > family > other); NULL = no role signal. Use for charts that must sum to 100%.",
    ("profile_metrics", "acquisition_month"): "Month of source_created_at. With has_registration_date, the growth axis.",
    ("profile_metrics", "source_created_basis"): "What kind of date source_created_at is: registration-grade (mailchimp_signup, wordpress_registered, survey_response, npi_enumeration, hub:*) vs cookie_creation vs email_activity_upper_bound.",
    ("profile_metrics", "has_registration_date"): "TRUE when source_created_at is registration-grade. Growth charts filter on this.",
    ("profile_metrics", "is_known_person"): "tier1 with a durable anchor -- a person you can name. The defensible audience denominator.",
    ("profile_metrics", "is_patient"): "Positive patient signal (tri-state upstream; here TRUE/FALSE with FALSE meaning no positive signal). Roles are independent and can overlap.",
    ("profile_metrics", "is_hcp"): "Positive healthcare-professional signal. Independent of other roles.",
    ("profile_metrics", "is_caregiver"): "Positive caregiver signal. Independent of other roles.",
    ("profile_metrics", "is_family_or_friend"): "Positive family/friend signal. Independent of other roles.",
    ("profile_metrics", "is_other"): "Positive 'other' role signal.",
    ("profile_metrics", "has_any_role"): "Any role flag positive. The honest denominator for persona percentages (~8% of profiles).",
    ("profile_metrics", "is_verified_hcp"): "Clinician with a real, current credential (NPI or explicit source). A credential count, NOT an audience -- see is_engaged_hcp and is_mailable.",
    ("profile_metrics", "has_deactivated_npi"): "The federal registry retired this person's NPI (retired/changed/died). Excluded from is_verified_hcp since 2026-08-19; keep them in audience work -- most still read.",
    ("profile_metrics", "is_engaged_hcp"): "Verified clinician who has EVER produced a pageview, human email open, or click. A floor, not a live audience; no time bound.",
    ("profile_metrics", "is_mailable"): "Allowed to email: subscribed, opted in, not suppressed. The commercial audience gate.",
    ("profile_metrics", "is_active_email_90d"): "Human (bot-filtered) email activity in the last 90 days.",
    ("profile_metrics", "is_active_email_30d"): "Human (bot-filtered) email activity in the last 30 days.",
    ("profile_metrics", "is_known_member"): "Has a forum/community registration.",
    ("profile_metrics", "is_active_member_90d"): "Deliberate forum activity (post/reply/comment) in the last 90 days.",
    ("profile_metrics", "is_active_member_30d"): "Deliberate forum activity in the last 30 days.",
    ("profile_metrics", "is_logged_in_90d"): "Any forum presence incl. login heartbeats in the last 90 days ('logged in recently' vs 'engaged').",
    ("profile_metrics", "has_sso_key"): "Has a bionews_uk SSO key -- cross-site login identity.",
    ("profile_metrics", "source_created_at"): "Best-known true acquisition date from the source system (write-once). Use with has_registration_date for growth.",
    ("profile_metrics", "last_active_at"): "Most recent human activity (bot-gated).",
    ("profile_metrics", "last_email_open"): "Most recent email open (includes bot/MPP opens; pair with the 90d human flags).",
    ("profile_metrics", "last_email_click"): "Most recent email click (clicks are not bot-inflated).",
    ("profile_metrics", "last_forum_activity"): "Most recent deliberate forum act -- the Active Member clock.",
    ("profile_metrics", "last_forum_presence"): "Most recent forum row of any kind incl. logins; always >= last_forum_activity.",
    # ---------------- profile_coverage (view) ----------------
    (
        "profile_coverage",
        "scope",
    ): "Population the fill rate is measured over: all (every profile) | "
    "engagement (profile_engagement rows) | hcp / patient (that persona's "
    "profiles, for persona-specific fields). Not a persona column.",
    ("profile_coverage", "field"): "Column being measured for fill rate.",
    (
        "profile_coverage",
        "filled",
    ): "Count of rows where the field is non-NULL/non-empty.",
    ("profile_coverage", "total"): "Total rows in the persona bucket.",
    (
        "profile_coverage",
        "fill_pct",
    ): "100 * filled / total, rounded to 1 decimal place.",
    # ---------------- profile_engagement_monthly (view) ----------------
    (
        "profile_engagement_monthly",
        "year_month",
    ): "First-of-month DATE for the bucket (e.g., 2026-05-01).",
    ("profile_engagement_monthly", "email_opens"): "Email opens in the month bucket.",
    ("profile_engagement_monthly", "email_clicks"): "Email clicks in the month bucket.",
    # ---------------- profile_events (view) ----------------
    (
        "profile_events",
        "primary_role",
    ): "v6.5 primary role of the bn_id at event time (denormalized from profile_roles).",
    ("profile_events", "ab_variant"): "A/B test variant label if applicable.",
    ("profile_events", "properties"): "Event-specific properties (JSON or STRUCT).",
    # ---------------- profile_exceptions (view) ----------------
    (
        "profile_exceptions",
        "exception_type",
    ): "Category of profile data issue: patient_without_condition | hcp_without_npi | etc.",
    ("profile_exceptions", "detail"): "Human-readable description of the issue.",
    # ---------------- profile_explain (view-derived) ----------------
    (
        "profile_explain",
        "identifier_count",
    ): "Total distinct identifiers attached to this bn_id (from profile_identifiers).",
    (
        "profile_explain",
        "identifier_type_count",
    ): "Distinct identifier_type values for this bn_id.",
    (
        "profile_explain",
        "identifier_types",
    ): "Distinct identifier_type values as ARRAY<STRING>.",
    (
        "profile_explain",
        "top_content_condition",
    ): "Highest-affinity condition from profile_content_affinity.",
    (
        "profile_explain",
        "top_affinity_score",
    ): "Affinity score for top_content_condition.",
    (
        "profile_explain",
        "segment_categories",
    ): "Distinct categories from profile_segment_tags.",
    ("profile_explain", "geography_tag"): "Coarse geography rollup.",
    (
        "profile_explain",
        "hcp_verified_tag",
    ): "Label when is_hcp=TRUE AND npi_number IS NOT NULL.",
    (
        "profile_explain",
        "ad_platforms",
    ): "Distinct ad platforms from profile_ad_attribution.",
    (
        "profile_explain",
        "click_id_types",
    ): "Distinct click ID type names (gclid, fbclid, etc.).",
    ("profile_explain", "first_ad_click_at"): "Earliest ad attribution timestamp.",
    ("profile_explain", "last_ad_click_at"): "Latest ad attribution timestamp.",
    (
        "profile_explain",
        "has_content_affinity_signal",
    ): "TRUE if profile_content_affinity has any rows.",
    (
        "profile_explain",
        "has_segment_signal",
    ): "TRUE if profile_segment_tags has any rows.",
    (
        "profile_explain",
        "has_paid_media_signal",
    ): "TRUE if profile_ad_attribution has any rows.",
    (
        "profile_explain",
        "has_condition_signal_conflict",
    ): "TRUE if preferred_condition disagrees with top_content_condition.",
    (
        "profile_explain",
        "has_hcp_signal_conflict",
    ): "TRUE if is_hcp=TRUE but no NPI OR is_hcp=FALSE with NPI present.",
    # ---------------- profile_forum_settings (view) ----------------
    (
        "profile_forum_settings",
        "notify_forum_replies",
    ): "TRUE if user wants email notifications on forum replies.",
    (
        "profile_forum_settings",
        "notify_group_invites",
    ): "TRUE if user wants notifications on group invites.",
    (
        "profile_forum_settings",
        "notify_direct_messages",
    ): "TRUE if user wants notifications on direct messages.",
    (
        "profile_forum_settings",
        "notify_mentions",
    ): "TRUE if user wants notifications on @mentions.",
    (
        "profile_forum_settings",
        "subscribed_forum_ids",
    ): "ARRAY<INT64> of subscribed BuddyPress forum IDs.",
    (
        "profile_forum_settings",
        "subscribed_group_ids",
    ): "ARRAY<INT64> of subscribed BuddyPress group IDs.",
    (
        "profile_forum_settings",
        "profile_visibility",
    ): "BuddyPress profile visibility setting: public | members | private.",
    (
        "profile_forum_settings",
        "show_activity_feed",
    ): "TRUE if activity feed is visible to others.",
    (
        "profile_forum_settings",
        "forum_registered_at",
    ): "When the user joined the BuddyPress forum (NULL if not joined).",
    (
        "profile_forum_settings",
        "last_forum_activity",
    ): "MAX activity timestamp in forums.",
    # ---------------- profile_newsletter_preferences (view) ----------------
    (
        "profile_newsletter_preferences",
        "newsletter_key",
    ): "Newsletter identifier (Mailchimp list_id or internal key).",
    (
        "profile_newsletter_preferences",
        "newsletter_label",
    ): "Display name for the newsletter.",
    (
        "profile_newsletter_preferences",
        "is_subscribed",
    ): "TRUE if currently subscribed (status='subscribed').",
    (
        "profile_newsletter_preferences",
        "subscribed_at",
    ): "When the user first subscribed.",
    (
        "profile_newsletter_preferences",
        "unsubscribed_at",
    ): "When the user unsubscribed (NULL if still subscribed).",
    # ---------------- profile_release_status (view) ----------------
    (
        "profile_release_status",
        "observed_at",
    ): "When the release snapshot was taken (CURRENT_TIMESTAMP at view query).",
    (
        "profile_release_status",
        "latest_run_id",
    ): "build_id of the most recent run regardless of mode/status.",
    (
        "profile_release_status",
        "latest_run_mode",
    ): "Mode of the most recent run: rebuild | refresh | reenrich | views | resume_publish.",
    (
        "profile_release_status",
        "latest_run_status",
    ): "Status: completed | completed_with_warnings | failed | aborted.",
    (
        "profile_release_status",
        "latest_run_started_at",
    ): "Start timestamp of latest run.",
    (
        "profile_release_status",
        "latest_run_completed_at",
    ): "Completion timestamp of latest run.",
    (
        "profile_release_status",
        "latest_run_runtime_fingerprint",
    ): "SHA fingerprint of code/config in the latest run.",
    (
        "profile_release_status",
        "latest_run_manifest_id",
    ): "identity_hub manifest the latest run consumed.",
    (
        "profile_release_status",
        "latest_rebuild_id",
    ): "build_id of the most recent rebuild.",
    (
        "profile_release_status",
        "latest_rebuild_status",
    ): "Status of the most recent rebuild.",
    (
        "profile_release_status",
        "latest_rebuild_completed_at",
    ): "Completion timestamp of the most recent rebuild.",
    (
        "profile_release_status",
        "latest_rebuild_runtime_fingerprint",
    ): "SHA fingerprint of code/config in the latest rebuild.",
    (
        "profile_release_status",
        "latest_refresh_id",
    ): "build_id of the most recent refresh.",
    (
        "profile_release_status",
        "latest_refresh_status",
    ): "Status of the most recent refresh.",
    (
        "profile_release_status",
        "latest_refresh_completed_at",
    ): "Completion timestamp of the most recent refresh.",
    (
        "profile_release_status",
        "latest_refresh_runtime_fingerprint",
    ): "SHA fingerprint of code/config in the latest refresh.",
    (
        "profile_release_status",
        "current_release_build_id",
    ): "build_id whose tables/views are currently published as profile_data.",
    (
        "profile_release_status",
        "current_release_mode",
    ): "Mode of the currently-published release.",
    (
        "profile_release_status",
        "current_release_build_status",
    ): "Build status of the currently-published release.",
    (
        "profile_release_status",
        "current_release_boundary_step",
    ): "Pipeline step name that delineated the current release boundary.",
    (
        "profile_release_status",
        "current_release_published_at",
    ): "When the current release was published to profile_data.",
    (
        "profile_release_status",
        "current_release_runtime_fingerprint",
    ): "Code/config SHA of the currently-published release.",
    (
        "profile_release_status",
        "current_release_schema_version",
    ): "Schema version string (e.g., '6.5') of the currently-published release.",
    (
        "profile_release_status",
        "current_release_manifest_id",
    ): "identity_hub manifest the current release consumed.",
    (
        "profile_release_status",
        "current_release_storage_dataset",
    ): "Underlying dataset name backing the current release (e.g., profile_data_candidate_xxx).",
    (
        "profile_release_status",
        "current_release_tables_published_at",
    ): "When base tables were copied/promoted in this release.",
    (
        "profile_release_status",
        "current_release_views_finalized_at",
    ): "When views were created/swapped in this release.",
    (
        "profile_release_status",
        "current_release_refresh_scope_n",
    ): "Number of bn_ids in the refresh scope (NULL for rebuilds).",
    (
        "profile_release_status",
        "current_release_refresh_scope_sources_json",
    ): 'JSON breakdown of refresh scope sources (e.g., {"wordpress": 50000, "mailchimp": 12000}).',
    (
        "profile_release_status",
        "current_release_assertion_summary",
    ): "JSON: counts of passed/total assertions, hard failures, soft warnings.",
    (
        "profile_release_status",
        "release_state",
    ): "Computed health label: healthy | degraded | failed | stale.",
    (
        "profile_release_status",
        "is_release_healthy",
    ): "TRUE if release_state = 'healthy'.",
    (
        "profile_release_status",
        "current_release_requires_followup",
    ): "TRUE if current_release has open assertions, warnings, or stale data.",
    # ---------------- profile_roles (view) ----------------
    (
        "profile_roles",
        "roles",
    ): "v6.5 multi-role surface: ARRAY<STRING> of every TRUE role flag for the bn_id. Possible values: patient | hcp | caregiver | family_or_friend | other. Empty array if no role is set.",
    (
        "profile_roles",
        "primary_role",
    ): "Single best-confidence role label (highest is_X_confidence; ties broken by source priority). Use for back-compat in single-role consumers.",
    (
        "profile_roles",
        "role_count",
    ): "INT64 number of TRUE role flags on this bn_id (0 to 5).",
    (
        "profile_roles",
        "role_lineage",
    ): "STRUCT nested by role with source/confidence/updated_at for each. Lets a single SELECT show the full provenance of every role.",
    # ---------------- profile_signals (view-derived) ----------------
    (
        "profile_signals",
        "top_content_condition",
    ): "Highest-affinity condition from profile_content_affinity.",
    (
        "profile_signals",
        "top_affinity_score",
    ): "Affinity score for top_content_condition.",
    (
        "profile_signals",
        "top_condition_pageviews",
    ): "Pageviews behind top_content_condition.",
    (
        "profile_signals",
        "content_affinity_signals",
    ): "Rolled-up ARRAY<STRUCT> of all condition affinities for the bn_id.",
    (
        "profile_signals",
        "segment_signals",
    ): "Rolled-up ARRAY<STRUCT> of segment tag categories.",
    (
        "profile_signals",
        "segment_categories",
    ): "Distinct segment categories. ARRAY<STRING>.",
    ("profile_signals", "geography_tag"): "Coarse geography rollup.",
    (
        "profile_signals",
        "hcp_verified_tag",
    ): "Label when is_hcp=TRUE AND npi_number IS NOT NULL.",
    (
        "profile_signals",
        "ad_platforms",
    ): "Distinct ad platforms from profile_ad_attribution.",
    (
        "profile_signals",
        "click_id_types",
    ): "Distinct click ID type names (gclid, fbclid, etc.).",
    ("profile_signals", "first_ad_click_at"): "Earliest ad attribution timestamp.",
    ("profile_signals", "last_ad_click_at"): "Latest ad attribution timestamp.",
    ("profile_signals", "ad_identifier_count"): "Distinct ad identifier count.",
    (
        "profile_signals",
        "has_content_affinity_signal",
    ): "TRUE if profile_content_affinity has any rows.",
    (
        "profile_signals",
        "has_segment_signal",
    ): "TRUE if profile_segment_tags has any rows.",
    (
        "profile_signals",
        "has_paid_media_signal",
    ): "TRUE if profile_ad_attribution has any rows.",
    # ---------------- profile_build_performance (view) ----------------
    ("profile_build_performance", "build_id"): "Build ID (UUID) of the run.",
    (
        "profile_build_performance",
        "mode",
    ): "Build mode: rebuild | refresh | reenrich | views | resume_publish.",
    ("profile_build_performance", "build_status"): "Status of the entire run.",
    ("profile_build_performance", "build_started_at"): "Run start timestamp.",
    ("profile_build_performance", "build_completed_at"): "Run completion timestamp.",
    (
        "profile_build_performance",
        "build_duration_seconds",
    ): "Total run duration in seconds.",
    (
        "profile_build_performance",
        "total_steps",
    ): "Number of pipeline steps in the run.",
    (
        "profile_build_performance",
        "failed_steps",
    ): "Number of pipeline steps that failed.",
    (
        "profile_build_performance",
        "build_total_bytes_processed",
    ): "Sum of bytes processed across all BigQuery jobs in the run.",
    (
        "profile_build_performance",
        "build_total_bytes_billed",
    ): "Sum of bytes billed across all BigQuery jobs in the run.",
    (
        "profile_build_performance",
        "build_total_slot_millis",
    ): "Sum of slot-milliseconds across all BigQuery jobs in the run.",
    (
        "profile_build_performance",
        "step_name",
    ): "Pipeline step name (one row per step per run).",
    ("profile_build_performance", "step_status"): "Status of this step.",
    ("profile_build_performance", "step_started_at"): "Step start timestamp.",
    ("profile_build_performance", "step_completed_at"): "Step completion timestamp.",
    ("profile_build_performance", "duration_seconds"): "Step duration in seconds.",
    (
        "profile_build_performance",
        "rows_affected",
    ): "Rows affected by this step (sum across statements).",
    (
        "profile_build_performance",
        "statements_executed",
    ): "Number of SQL statements executed in this step.",
    (
        "profile_build_performance",
        "step_total_bytes_processed",
    ): "Sum of bytes processed by BigQuery jobs in this step.",
    (
        "profile_build_performance",
        "step_total_bytes_billed",
    ): "Sum of bytes billed by BigQuery jobs in this step.",
    (
        "profile_build_performance",
        "step_total_slot_millis",
    ): "Sum of slot-milliseconds for BigQuery jobs in this step.",
    (
        "profile_build_performance",
        "step_bytes_share",
    ): "This step's share of build_total_bytes_billed (0-1).",
    (
        "profile_build_performance",
        "step_slot_share",
    ): "This step's share of build_total_slot_millis (0-1).",
    (
        "profile_build_performance",
        "rows_affected_per_statement",
    ): "rows_affected / statements_executed (mean).",
    (
        "profile_build_performance",
        "identity_hub_manifest_id",
    ): "identity_hub manifest consumed by this run.",
    (
        "profile_build_performance",
        "preflight_status",
    ): "Pre-run gate status (passed | failed | skipped).",
    (
        "profile_build_performance",
        "schema_version",
    ): "Schema version string (e.g., '6.5').",
    (
        "profile_build_performance",
        "runtime_fingerprint",
    ): "SHA fingerprint of code/config at run time.",
    (
        "profile_build_performance",
        "refresh_scope_n",
    ): "Number of bn_ids in the refresh scope (NULL for rebuilds).",
    (
        "profile_build_performance",
        "refresh_scope_sentinel_n",
    ): "Number of sentinel rows added to the refresh scope.",
    (
        "profile_build_performance",
        "refresh_scope_source_breakdown_json",
    ): "JSON breakdown of refresh scope sources.",
    (
        "profile_build_performance",
        "assertion_summary",
    ): "JSON: counts of passed/total assertions, hard failures, soft warnings.",
    (
        "profile_build_performance",
        "warnings",
    ): "ARRAY<STRING> of soft warnings raised during the run.",
    (
        "profile_build_performance",
        "error_message",
    ): "Truncated error message if the run failed.",
    (
        "profile_build_performance",
        "is_latest_run",
    ): "TRUE for the single most recent row across all runs.",
    (
        "profile_build_performance",
        "is_latest_for_mode",
    ): "TRUE for the most recent row of each mode.",
}


# ---------------------------------------------------------------------------
# Logic
# ---------------------------------------------------------------------------


class Stats:
    def __init__(self) -> None:
        self.tables_updated = 0
        self.tables_unchanged = 0
        self.tables_missing = 0
        self.columns_updated = 0
        self.columns_unchanged = 0
        self.columns_missing = 0


def update_schema_descriptions(
    client: bigquery.Client,
    table_id: str,
    column_descriptions: dict[str, str],
) -> tuple[int, int]:
    """Update column descriptions on a table. Returns (updated, unchanged)."""
    table = client.get_table(table_id)
    new_schema = []
    updated = 0
    unchanged = 0

    def patch_field(field: bigquery.SchemaField) -> bigquery.SchemaField:
        nonlocal updated, unchanged
        desired = column_descriptions.get(field.name)
        if desired is None:
            return field
        if (field.description or "").strip() == desired.strip():
            unchanged += 1
            return field
        updated += 1
        # SchemaField is immutable; rebuild
        return bigquery.SchemaField(
            name=field.name,
            field_type=field.field_type,
            mode=field.mode,
            description=desired,
            fields=field.fields,
            policy_tags=field.policy_tags,
        )

    new_schema = [patch_field(f) for f in table.schema]

    if updated > 0:
        table.schema = new_schema
        client.update_table(table, ["schema"])

    return updated, unchanged


def apply_metadata(
    client: bigquery.Client,
    only_tables: Optional[set[str]] = None,
    dry_run: bool = True,
) -> Stats:
    stats = Stats()

    target_tables = sorted(TABLE_DESCRIPTIONS.keys())
    if only_tables:
        target_tables = [t for t in target_tables if t in only_tables]

    # Build an inheritance map for views: any view column whose name matches a
    # documented base-table column reuses that description. Resolution order:
    # explicit (view-specific) > profile_core > profile_engagement > profile_identifiers >
    # profile_lookup > anything else. This keeps the script DRY (no duplicating
    # bn_id/email/is_patient descriptions across 22 views) while still letting us
    # override a view-specific column with an explicit entry.
    INHERITANCE_ORDER = (
        "profile_core",
        "profile_engagement",
        "profile_identifiers",
        "profile_lookup",
        "profile_preferences",
        "profile_content_affinity",
        "profile_ad_attribution",
        "profile_segment_tags",
        "profile_survey_data",
        "profile_zero_party",
        "site_events",
    )
    inherited_col_desc: dict[str, str] = {}
    for source_tbl in INHERITANCE_ORDER:
        for (t, col), desc in COLUMN_DESCRIPTIONS.items():
            if t == source_tbl and col not in inherited_col_desc:
                inherited_col_desc[col] = desc

    for tname in target_tables:
        table_id = f"{PROJECT}.{DATASET}.{tname}"
        try:
            table = client.get_table(table_id)
        except Exception as e:
            print(f"[MISSING] {tname}: {e}")
            stats.tables_missing += 1
            continue

        # Filter column descriptions to actual columns on this table.
        # For views, fall back to the inheritance map for any column without
        # an explicit entry. Explicit (table, col) entries always win.
        actual_cols = {f.name for f in table.schema}
        explicit_descs = {
            col: desc
            for (t, col), desc in COLUMN_DESCRIPTIONS.items()
            if t == tname and col in actual_cols
        }
        col_descs = dict(explicit_descs)
        if table.table_type == "VIEW":
            for col in actual_cols - col_descs.keys():
                if col in inherited_col_desc:
                    col_descs[col] = inherited_col_desc[col]
        documented_cols = {col for (t, col) in COLUMN_DESCRIPTIONS if t == tname}
        missing_in_table = documented_cols - actual_cols
        for col in sorted(missing_in_table):
            print(f"[SKIP] {tname}.{col}: documented but column not in table")
            stats.columns_missing += 1

        # Table description
        desired_table_desc = TABLE_DESCRIPTIONS[tname].strip()
        current_table_desc = (table.description or "").strip()
        table_desc_changed = current_table_desc != desired_table_desc

        if dry_run:
            if table_desc_changed:
                print(
                    f"[DRY] {tname}: would update TABLE description ({len(desired_table_desc)} chars)"
                )
                stats.tables_updated += 1
            else:
                stats.tables_unchanged += 1

            # Preview column changes (without writing)
            preview_updates = 0
            for f in table.schema:
                desired = col_descs.get(f.name)
                if desired is None:
                    continue
                if (f.description or "").strip() != desired.strip():
                    preview_updates += 1
            print(
                f"[DRY] {tname}: would update {preview_updates} column(s) of {len(col_descs)} documented"
            )
            stats.columns_updated += preview_updates
            stats.columns_unchanged += len(col_descs) - preview_updates
            continue

        # Apply table description
        if table_desc_changed:
            table.description = desired_table_desc
            client.update_table(table, ["description"])
            stats.tables_updated += 1
            print(
                f"[OK ] {tname}: table description updated ({len(desired_table_desc)} chars)"
            )
        else:
            stats.tables_unchanged += 1
            print(f"[OK ] {tname}: table description already current")

        # Apply column descriptions
        updated, unchanged = update_schema_descriptions(client, table_id, col_descs)
        stats.columns_updated += updated
        stats.columns_unchanged += unchanged
        print(f"[OK ] {tname}: {updated} column(s) updated, {unchanged} unchanged")

    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually write changes. Default is dry-run.",
    )
    parser.add_argument(
        "--tables",
        type=str,
        default=None,
        help="Comma-separated list of tables to update (default: all).",
    )
    args = parser.parse_args()

    client = bigquery.Client(project=PROJECT)
    only = set(args.tables.split(",")) if args.tables else None

    mode = "APPLY (writing)" if args.apply else "DRY RUN (no writes)"
    print(f"=== Profile metadata applicator [{mode}] ===")
    print(f"Project: {PROJECT}")
    print(f"Dataset: {DATASET}")
    if only:
        print(f"Tables filter: {', '.join(sorted(only))}")
    print()

    stats = apply_metadata(client, only_tables=only, dry_run=not args.apply)

    print()
    print("=== Summary ===")
    verb = "Would update" if not args.apply else "Updated"
    print(
        f"  Tables: {verb}={stats.tables_updated}  Unchanged={stats.tables_unchanged}  Missing={stats.tables_missing}"
    )
    print(
        f"  Columns: {verb}={stats.columns_updated}  Unchanged={stats.columns_unchanged}  Missing={stats.columns_missing}"
    )

    if not args.apply:
        print()
        print("This was a DRY RUN. Re-run with --apply to write changes.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
