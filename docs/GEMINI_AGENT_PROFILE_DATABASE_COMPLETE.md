> # ⛔ DO NOT USE FOR SCHEMA — SUPERSEDED (2026-08-18)
>
> This document was written against the **v6.0 schema** and every query in it
> that touches persona is now **wrong**, not merely dated. It contains 33
> references to `account_type`, a column **dropped in v6.5** (2026-05-19).
> Queries copied from here will fail outright or silently return nothing.
>
> It also predates `preferred_condition_normalized` (v6.6) and
> `source_created_at` (v6.7).
>
> **Use instead:**
> - [PROFILE_DATABASE_DATA_DICTIONARY.md](PROFILE_DATABASE_DATA_DICTIONARY.md) — every table and column, generated from the DDL
> - [PROFILE_DATABASE_COMPLETE_REFERENCE.md](PROFILE_DATABASE_COMPLETE_REFERENCE.md) — orientation and query guidance
>
> Translation: `WHERE account_type = 'hcp'` becomes `WHERE is_hcp = TRUE`.
> Roles are now five independent BOOL flags, so a person can hold several.
>
> Retained only for the agent-prompt patterns and business framing.

# Google Gemini Agent: Profile Database Complete Guide
## Single-Source Reference for Vertex AI Agent Builder Implementation

**Version**: 2.0 (Consolidated)
**Last Updated**: 2026-05-13
**Target**: Google Gemini Agent / Vertex AI Agent Builder
**Dataset**: `bi-data-391216.profile_data`
**Purpose**: Complete document that Gemini Agent reads to understand the Profile Database and answer customer intelligence questions accurately.

---

## TABLE OF CONTENTS

1. [Agent System Prompt (Copy-Paste Ready)](#agent-system-prompt)
2. [What This Database Is](#what-this-database-is)
3. [Core Concepts the Agent Must Understand](#core-concepts)
4. [Vertex AI Agent Builder Setup (3 Phases)](#vertex-ai-setup)
5. [Metadata Templates (Tables & Columns)](#metadata-templates)
6. [Complete Schema with Descriptions](#complete-schema)
7. [25 Sample Questions with SQL](#sample-questions)
8. [Few-Shot Examples for the Agent](#few-shot-examples)
9. [SQL Patterns & Anti-Patterns](#sql-patterns)
10. [Governance, PII & Security](#governance)

---

## 1. AGENT SYSTEM PROMPT {#agent-system-prompt}

**Paste this directly into Vertex AI Agent Builder → Instructions panel:**

```
You are an expert data analyst with deep knowledge of the BioNews Profile Database
(BigQuery dataset: bi-data-391216.profile_data).

## Your Role
You answer questions about 2.5M customer profiles by generating BigQuery SQL,
executing it, and presenting clear, governed answers with visualizations when
appropriate.

## What You Know
- The database stores ONE row per unique person (keyed by bn_id).
- Each person has a persona: patient | hcp | caregiver | family_or_friend | other.
- 16 tables connect via bn_id (master key from the identity hub).
- Engagement, conditions, treatments, identifiers, and events are all keyed by bn_id.

## Hard Rules
1. ALWAYS use COUNT(DISTINCT bn_id) for user counts (never COUNT(*) on 1:N joins).
2. ALWAYS filter account_type when querying persona-specific fields
   (npi_number is NULL for patients; diagnosis_stage is NULL for HCPs).
3. ALWAYS add a date filter on site_events (it has 156M+ rows).
4. NEVER return raw PII (email, phone, full name) without confirming user is tier2.
5. When asked about "active" or "engaged" users, JOIN profile_engagement and use engagement_tier.
6. When asked about a condition, JOIN conditions_dict on mesh_id.

## When You Don't Know
- If a metric name is ambiguous, ask: "Do you mean [X] or [Y]?"
- If a table isn't in your allowed list, say so — don't invent table names.
- If the answer requires PII you can't show, return aggregates and explain the mask.

## How to Present Results
- Lead with the answer (number, top item, trend).
- Show the SQL you ran.
- Offer 2-3 follow-up questions ("Want to break this down by condition? By month?").
```

---

## 2. WHAT THIS DATABASE IS {#what-this-database-is}

The **Profile Database** is BioNews's unified customer intelligence platform. It is
the company's **#1 strategic data asset** — a single, governed source of truth for
**2.5 million people** across all properties (websites, email lists, communities,
healthcare partners).

### What It Contains
- **Identity**: Each person has a stable `bn_id` that survives across cookies, emails,
  devices, and sites.
- **Persona**: Each person is classified — patient, HCP (healthcare professional),
  caregiver, family/friend, or other.
- **Demographics**: Name, age, gender, location, language.
- **Health context**: Conditions, treatments, symptoms, diagnosis stage (for patients);
  specialty, NPI, practice info (for HCPs).
- **Behavior**: Email opens, web sessions, video views, form submits, forum posts.
- **Preferences**: Newsletter subscriptions, communication consent, content interests.
- **Attribution**: How they found us (paid, organic, social, referral).

### Where the Data Comes From (11 Source Systems)
| Source | Data | Update Frequency |
|--------|------|------------------|
| Mailchimp | Email subscribers, opens, clicks, segments, tags | Daily |
| WordPress | User accounts, forum posts, comments | Daily |
| GA4 (Google Analytics) | Web sessions, pageviews, events | Daily |
| BioNews Acceptor | Cookies, localStorage, device signals | Real-time |
| NPI Registry | Healthcare provider profiles | Monthly |
| Facebook/Instagram | Ad audience matching, pixel events | Daily |
| LimeSurvey | Survey responses with health details | Daily |
| Stripe/PayPal | Paid subscription status | Daily |
| AIM Clickstream | HCP identification signals | Weekly |
| DMD | Verified HCP audience matches | Quarterly |
| OneTrust | Cookie consent state | Real-time |

### Why It Matters
- **Audience targeting**: "Show me all patients with Myasthenia Gravis who clicked
  treatment content in the last 90 days."
- **Persona detection**: "Which caregivers are most engaged with community forums?"
- **Attribution**: "What's the LTV by acquisition source?"
- **Compliance**: "Which users have NOT consented to marketing?"
- **Content strategy**: "Which treatments are patients researching but not yet on?"

---

## 3. CORE CONCEPTS THE AGENT MUST UNDERSTAND {#core-concepts}

### Concept 1: `bn_id` — The Master Key
- **Definition**: Stable, unique person identifier (UUID-like string, e.g.,
  `bnid_a1b2c3d4e5f6`).
- **Created by**: Identity hub when 2+ identifiers link (email + phone = same person).
- **Lifecycle**: Immutable. Survives merges, splits, and identity corrections.
- **Used as**: Primary key in `profile_core`; foreign key in every other table.
- **Rule**: For user counts, ALWAYS use `COUNT(DISTINCT bn_id)`.

### Concept 2: `account_type` — The Persona
| Value | Description | Persona-Specific Fields Populated |
|-------|-------------|------------------------------------|
| `patient` | Consumer with a condition (or interest in one) | `diagnosis_stage`, `symptom_tags`, `treatments_current`, `condition_subtype` |
| `hcp` | Licensed healthcare professional | `npi_number`, `specialty`, `practice_city`, `credentials`, `condition_focus` |
| `caregiver` | Non-patient caring for someone with a condition | `caregiver_condition`, `caregiver_relationship`, `caregiver_focus_areas` |
| `family_or_friend` | Related to patient, not primary caregiver | `family_condition`, `family_relationship` |
| `other` | Researchers, journalists, observers | `interest_tags`, `follow_conditions` |

**CRITICAL**: Always filter `account_type` before querying persona-specific columns,
or you'll get all-NULL results.

### Concept 3: `engagement_tier` — Computed Activity Level
- Lives on `profile_engagement`, NOT `profile_core`. Must JOIN.
- Values: `high`, `medium`, `low`, `inactive`.
- Computed daily from: `last_active_at`, `email_open_count`, `total_sessions`,
  `bp_activity_count`, `days_retained`.
- "Active" or "DAU" questions → use `engagement_tier IN ('high','medium')` OR
  `last_active_at >= CURRENT_DATE() - 30`.

### Concept 4: `mesh_id` — Standardized Medical Vocabulary
- MeSH (Medical Subject Headings) ID from US National Library of Medicine.
- Example: `D009157` = Myasthenia Gravis.
- Stored in `preferred_condition.mesh_id`, `condition_subtype.mesh_id`, etc.
- JOIN to `conditions_dict.mesh_id` to get the full label and category.

### Concept 5: Tier-1 vs Tier-2 Identity
- **Tier-1** (known person): Has an email, mc_euid, OR bionews_uk. Real, named person.
- **Tier-2** (anonymous trackable): Only has cookies (bnfpvid). Browser, not name.
- 75% of profiles are tier-1; 25% are tier-2 (mostly anonymous web visitors).
- For most marketing/CRM questions, filter `WHERE cluster_tier = 'tier1'`.

### Concept 6: STRUCT and ARRAY Columns
Several columns are nested types — query them with dot notation or UNNEST:

```sql
-- STRUCT access (dot notation):
SELECT preferred_condition.label, preferred_condition.mesh_id FROM profile_core;

-- ARRAY access (UNNEST):
SELECT bn_id, treatment.label, treatment.rxnorm_id
FROM profile_core, UNNEST(treatments_current) AS treatment
WHERE account_type = 'patient';
```

---

## 4. VERTEX AI AGENT BUILDER SETUP {#vertex-ai-setup}

### Phase 1: Enrich Metadata (The "Understanding" Layer)

Before the agent works well, every table and column needs a description.
The agent's accuracy is **directly proportional** to metadata quality.

**Three ways to add descriptions:**

1. **Manual** (BigQuery Console → Schema tab → Edit description).
2. **Gemini Auto-Generate**: In BigQuery console, click the **Gemini** button next to
   the schema — it suggests descriptions based on column values. Review before saving.
3. **Programmatic** (Python SDK — see Section 6).

### Phase 2: Create a Data Agent in Vertex AI

**Step-by-step in Google Cloud Console:**

1. **Go to Vertex AI Agent Builder**
   - Console → Search "Agent Builder" → Open.

2. **Create a New App**
   - Click **CREATE APP**.
   - Select app type: **Agent**.
   - Name: `profile-db-agent`.
   - Display name: `BioNews Profile Database Agent`.

3. **Define the Data Store**
   - Source: **BigQuery**.
   - Project: `bi-data-391216`.
   - Dataset: `profile_data`.
   - Tables to include:
     - `profile_core`
     - `profile_engagement`
     - `profile_identifiers`
     - `profile_segment_tags`
     - `profile_content_affinity`
     - `profile_preferences`
     - `profile_survey_data`
     - `profile_zero_party`
     - `site_events`
     - `conditions_dict`
     - `treatments_dict`
     - `symptoms_dict`
     - `subtypes_dict`
     - `profile_lookup`

4. **Configure Instructions**
   - Paste the **Agent System Prompt** from Section 1.

5. **Add Few-Shot Examples**
   - See Section 8 for 8 question→SQL example pairs.

6. **Configure Security**
   - Enable PII masking ✓
   - Enable audit logging ✓
   - Max query bytes: 10 GB
   - Max execution: 600 seconds
   - Max rows returned: 50,000

7. **Deploy**
   - Click **PUBLISH**.
   - Test in the agent playground before sharing.

### Phase 3: How the Agent Answers (NL2SQL Flow)

```
User question:    "How many patients in New York are highly engaged?"
       ↓
Agent parses:     entity="patients", location="New York",
                  metric="engagement", level="high"
       ↓
Schema mapping:   "patients"   → profile_core WHERE account_type='patient'
                  "New York"   → address_state IN ('NY','New York')
                  "engaged"    → JOIN profile_engagement
                  "highly"     → engagement_tier='high'
       ↓
Generated SQL:    SELECT COUNT(DISTINCT pc.bn_id)
                  FROM profile_core pc
                  JOIN profile_engagement pe USING(bn_id)
                  WHERE pc.account_type='patient'
                    AND pc.address_state IN ('NY','New York')
                    AND pe.engagement_tier='high';
       ↓
Result:           1,247 patients
       ↓
Agent offers:     "Break this down by condition?"
                  "Trend over the last 6 months?"
                  "Compare to other states?"
```

### Phase 4: Alternative — BigQuery Data Canvas (Low-Code)

If you want **interactive exploration** without a full agent:
1. BigQuery Studio → Right-click `profile_data` dataset → **Open in Data Canvas**.
2. Use natural language at the top: "Show me top conditions by patient count."
3. Data Canvas generates SQL + visualization automatically.
4. Great for ad-hoc questions; the full Vertex AI agent is better for repeated, governed use.

---

## 5. METADATA TEMPLATES {#metadata-templates}

### 5.1 Table/View Metadata Template

Apply this to the **Description** field of each table in BigQuery.

| Field | What to Write | Example for `profile_core` |
|-------|---------------|----------------------------|
| **Business Purpose** | What business process does this table support? | Stores the primary demographic, persona, and status information for all customer profiles. Single source of truth for who each person is. |
| **Grain** | What does one row represent? | One unique row per `bn_id` (one row per person, regardless of how many emails/cookies/devices they have). |
| **Source System** | Where does this data originate? | Identity Hub (bn_id, identifiers); Mailchimp (email, opt-in); WordPress (name, registration); NPI Registry (HCP fields); LimeSurvey (demographics). |
| **Update Frequency** | How often is this data refreshed? | Daily refresh at 02:00 UTC (3-day lookback merge). Full rebuild quarterly. |
| **Primary Key** | Which columns uniquely identify a row? | `bn_id` (STRING, NOT NULL). |
| **Foreign Keys** | What does this link to? | `bn_id` → joins to profile_engagement, profile_identifiers, profile_segment_tags, and 8 other satellite tables. |
| **Typical Questions** | What should the agent answer using this? | "How many patients with [condition]?" / "What's the persona breakdown?" / "Which HCPs are NPI-verified?" / "How many users opted out of marketing?" |
| **PII Level** | What is the highest sensitivity in this table? | HIGH — contains email, phone, names, health condition. |
| **Owner** | Team responsible | Data Intelligence Team (data-team@bionews.com). |

### 5.2 Column Metadata Template

Apply this to the **Description** field of each column in BigQuery.

| Property | What to Write | Example for `engagement_tier` |
|----------|---------------|-------------------------------|
| **Clear Name** | Natural-language name if ID is cryptic | Engagement Tier (Computed Activity Level) |
| **Definition** | Plain-English explanation | Categorical rank of how active this person is across all channels (email, web, community). Computed daily. |
| **Value Logic** | How is it calculated? What do codes mean? | Enum: `high` = top 25% by activity OR last_active_at <= 14 days AND (email_open_count > 10 OR total_sessions > 20). `medium` = last_active_at <= 90 days. `low` = last_active_at <= 180 days. `inactive` = last_active_at > 180 days OR NULL. |
| **Unit of Measure** | Dollars / days / percent / count / score? | Categorical (ordinal: high > medium > low > inactive). |
| **Data Type** | BigQuery type | STRING |
| **Null Handling** | What does NULL mean? | Never NULL (always assigned to one of 4 tiers). If you see NULL, the bn_id has no row in profile_engagement yet. |
| **Example Values** | 2-3 real values | `high`, `medium`, `low`, `inactive` |
| **Typical Aggregation** | COUNT, SUM, AVG, GROUP BY? | GROUP BY; COUNTIF(engagement_tier='high'); never SUM (it's categorical). |
| **Related Columns** | Used with what other columns? | `last_active_at`, `email_open_count`, `total_sessions`, `bp_activity_count`. |
| **PII Level** | none / low / medium / high | None (derived, not identifying). |

### 5.3 Why This Matters for the Agent

| Without Metadata | With Metadata |
|------------------|---------------|
| Agent guesses what columns mean | Agent reads description, uses correctly |
| "Active users" → unclear which field | "Active" → maps to `engagement_tier` or `last_active_at` |
| Sums a score column (wrong) | Agent reads "Unit of Measure: Score" → uses AVG |
| Joins tables incorrectly | Agent reads "Primary Key: bn_id" → joins correctly |
| Returns raw PII | Agent reads "PII Level: HIGH" → masks |

**Pro-tip**: Use the **Gemini** button in the BigQuery schema view to auto-generate
draft descriptions based on column values — then review and refine.

---

## 6. COMPLETE SCHEMA WITH DESCRIPTIONS {#complete-schema}

### 6.1 Table: `profile_core` (Master Profile)

```
TABLE DESCRIPTION:
Business Purpose: Master customer profile record. Single source of truth for who
  each person is across all properties. Consolidates patient, HCP, caregiver, and
  general-audience attributes in one row per bn_id.
Grain: One row per unique person (bn_id). Never duplicate rows per person.
Source System: Identity Hub (bn_id, identifiers); Mailchimp (email, consent);
  WordPress (registration); NPI Registry (HCP fields); LimeSurvey (demographics).
Update Frequency: Daily refresh at 02:00 UTC. Full rebuild quarterly.
Primary Key: bn_id (STRING).
Foreign Keys: bn_id links to profile_engagement, profile_identifiers,
  profile_segment_tags, profile_preferences, profile_content_affinity,
  profile_survey_data, profile_zero_party, site_events, profile_ad_attribution.
Typical Questions:
  - "How many patients with Myasthenia Gravis?"
  - "What's the breakdown of HCPs by specialty?"
  - "Which users have NOT consented to marketing?"
  - "Show me users acquired from Facebook in Q1 2026."
PII Level: HIGH (contains email, phone, names, health condition).
Owner: Data Intelligence Team.
Row Count: ~2.5M.
```

**Key Columns** (with full descriptions to paste into BigQuery):

| Column | Type | Description |
|--------|------|-------------|
| `bn_id` | STRING (PK) | **BioNews Identity ID** — stable, unique, immutable person ID from identity hub. Primary key. Never NULL. Use COUNT(DISTINCT bn_id) for user counts. Example: `bnid_a1b2c3d4`. |
| `bionews_uk` | STRING | **SSO User Key** — login identifier for cross-site authentication. Persists across domains. NULL for users who never created an SSO account. |
| `email` | STRING | **Primary Email Address** — used for login and communication. PII level: HIGH. Masked for tier-1 users as `j***@example.com`. NULL for anonymous tier-2 users (~25% of rows). |
| `email_hash` | STRING | **SHA-256 Email Hash** — for privacy-safe matching and deduplication. Never raw email. |
| `account_type` | STRING (ENUM) | **Persona Type** — categorical: `patient` \| `hcp` \| `caregiver` \| `family_or_friend` \| `other`. **CRITICAL**: filter on this before querying persona-specific fields. NULL means persona couldn't be inferred (~2%). |
| `persona_source` | STRING | **How Persona Was Determined** — `confirmed` (user self-reported), `npi` (NPI registry match), `inferred` (from Mailchimp lists), `survey` (LimeSurvey answer), `buddypress_xprofile`, `mailchimp_merge_field`. |
| `first_name` | STRING | **First Name** — PII: MEDIUM. Resolved priority: NPI > WordPress > Mailchimp > LimeSurvey. NULL ~30% (anonymous users). |
| `last_name` | STRING | **Last Name** — PII: MEDIUM. Same resolution as first_name. |
| `gender` | STRING (ENUM) | **Gender** — `male` \| `female` \| `other` \| `prefer_not_to_say`. NULL means not self-reported. Sensitive (governance: only show in aggregate). |
| `age_exact` | INT64 | **Exact Age (Years)** — from LimeSurvey self-report. Sensitive PII. Sparse (~5% fill). Prefer `age_band` for analytics. |
| `age_band` | STRING (ENUM) | **Age Band** — `18-24` \| `25-34` \| `35-44` \| `45-54` \| `55-64` \| `65-74` \| `75+` \| `prefer_not_to_say`. Use for demographic analysis. |
| `country` | STRING | **Country Code** — ISO 3166-1 alpha-2 (e.g., `US`, `UK`, `CA`). NULL means location unknown. |
| `phone` | STRING | **Phone Number** — 10-digit US format (e.g., `2125551234`). PII: HIGH. From Mailchimp merge fields. Sparse (~15% fill). |
| `address_state` | STRING | **State (US Mailing Address)** — 2-letter code (e.g., `NY`, `CA`) or full name. NULL = unknown. |
| `address_city` | STRING | **City (Mailing Address)** — free text. NULL = unknown. |
| `consent_status` | STRING (ENUM) | **Overall Consent** — `granted` \| `denied` \| `pending`. |
| `communication_opt_in` | BOOL | **Marketing Email Opt-In** — TRUE means user agreed to receive marketing emails (CAN send). FALSE means they unsubscribed (DO NOT send). NULL means never asked. **Compliance critical**: always filter `WHERE communication_opt_in = TRUE` before targeting campaigns. |
| `tracking_consent` | BOOL | **Analytics Tracking Consent** — from OneTrust. TRUE = can track. Required for GA4-derived metrics. |
| `preferred_condition` | STRUCT<label STRING, mesh_id STRING> | **Primary Condition of Interest** — STRUCT with `label` (e.g., "Myasthenia Gravis") and `mesh_id` (e.g., "D009157"). Inferred from Mailchimp list > content browsing > self-report. Access via dot notation: `preferred_condition.label`. JOIN `conditions_dict` ON `mesh_id` for full details. |
| `preferred_condition_confidence` | FLOAT64 | **Confidence Score (0.0-1.0)** — how sure we are this is their primary condition. Decays 2% per 90 days without re-confirmation. < 0.5 = low confidence. |
| `npi_number` | STRING | **National Provider Identifier** — 10-digit NPI for healthcare professionals. NULL for non-HCPs. Use `WHERE account_type='hcp' AND npi_number IS NOT NULL` for verified HCPs. Public information (not PII). |
| `specialty` | STRUCT<label STRING, snomed_id STRING> | **Medical Specialty (HCP only)** — STRUCT with `label` (e.g., "Neurology") and `snomed_id`. NULL for non-HCPs. From NPI registry. |
| `credentials` | STRING | **HCP Credentials** — `MD` \| `DO` \| `NP` \| `PA` \| `RN` etc. NULL for non-HCPs. |
| `practice_state` | STRING | **HCP Practice State** — where the HCP practices. May differ from `address_state` (home). |
| `practice_city` | STRING | **HCP Practice City**. |
| `years_in_practice_band` | STRING | **Years in Practice** — `0-2` \| `3-5` \| `6-10` \| `11-20` \| `21+`. |
| `patient_volume_band` | STRING | **Patients Seen per Month (HCP self-report)** — `0-10` \| `11-50` \| `51-100` \| `101+`. |
| `diagnosis_stage` | STRING (ENUM) | **Diagnosis Stage (Patient only)** — `undiagnosed` \| `newly_diagnosed` \| `progressing` \| `stable` \| `unknown`. NULL for non-patients. |
| `diagnosis_timing_band` | STRING (ENUM) | **Time Since Diagnosis** — `<6mo` \| `6-24mo` \| `>24mo` \| `prefer_not_to_say`. |
| `symptom_tags` | ARRAY<STRUCT> | **Reported Symptoms** — array of STRUCT<label, mesh_id, hpo_id>. UNNEST to query. JOIN to `symptoms_dict` for details. |
| `treatments_current` | ARRAY<STRUCT> | **Treatments Currently Taken** — array of STRUCT<label, rxnorm_id>. UNNEST to query. |
| `treatments_of_interest` | ARRAY<STRUCT> | **Treatments Researching** — what the user is reading about but may not be on yet. Strong intent signal. |
| `caregiver_relationship` | STRING (ENUM) | **Relationship to Care Recipient** — `parent` \| `spouse` \| `sibling` \| `child` \| `friend` \| `other`. NULL for non-caregivers. |
| `caregiver_focus_areas` | ARRAY<STRING> | **Caregiver Concerns** — array of: `daily_care` \| `logistics` \| `insurance_financials` \| `advocacy` \| `emotional_support` \| `care_coordination`. Max 3. |
| `acquisition_source` | STRING | **First-Touch Acquisition Source** — `organic` \| `paid_search` \| `paid_social` \| `email` \| `direct` \| `referral`. |
| `acquisition_medium` | STRING | **Acquisition Medium** — `cpc` \| `organic_search` \| `newsletter` \| `referral` \| `social_paid`. |
| `acquisition_campaign` | STRING | **Acquisition Campaign Name** — from UTM. |
| `profile_completeness` | INT64 | **Profile Completeness Score** — 0-100. Weighted by essential fields (20%), demographics (30%), health (30%), engagement (20%). Higher = richer profile. Use for enrichment targeting. |
| `profile_stage` | STRING | **Profile Lifecycle Stage** — S1 (minimal) \| S2 (email known) \| S3 (demographics) \| S4 (condition) \| S5 (deep journey/survey). |
| `created_at` | TIMESTAMP | **Profile Creation Timestamp** — when this bn_id was first issued. UTC. |
| `last_active_at` | TIMESTAMP | **Last Known Activity** — MAX across email, web, community. NULL = never active. Use for churn: `WHERE last_active_at < CURRENT_DATE() - 180`. |
| `cluster_tier` | STRING (ENUM) | **Identity Confidence Tier** — `tier1` (known person: has email/SSO) \| `tier2` (anonymous trackable: only cookies). Filter `cluster_tier='tier1'` for CRM/marketing questions. |
| `is_shared_workstation` | BOOL | **Shared Computer Flag** — TRUE if 2+ different email subscribers seen on same device. Exclude when analyzing per-person behavior. |
| `is_suspicious` | BOOL | **Quality Flag** — TRUE if cluster_health_score < 60. Exclude in production analyses. |
| `source_systems` | ARRAY<STRING> | **Which Data Sources Contributed** — e.g., `["mailchimp", "ga4", "npi_registry"]`. Used to understand provenance. |

### 6.2 Table: `profile_engagement` (Behavioral Metrics)

```
TABLE DESCRIPTION:
Business Purpose: Tracks customer interaction metrics across email, web, community,
  and ads. Use this table to answer questions about user activity levels, churn risk,
  engagement segments, and top-performing users.
Grain: One row per bn_id (1:1 with profile_core).
Source System: Mailchimp (email opens/clicks), GA4 (web sessions), site_events
  (form submits, video views), WordPress + BuddyPress (forum activity).
Update Frequency: Daily refresh at 02:00 UTC.
Primary Key: bn_id (also FK to profile_core).
Typical Questions:
  - "Which users are highly engaged?"
  - "Show me email open rate by condition."
  - "Who is at risk of churn (inactive > 180 days)?"
  - "What's the best send time for HCPs?"
PII Level: MEDIUM (aggregate behavioral data; no direct identifiers).
Row Count: ~2.5M (1:1 with profile_core).
```

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `bn_id` | STRING (PK/FK) | Foreign key to profile_core.bn_id. |
| `mailchimp_status` | STRING | **Email Subscription Status** — `subscribed` \| `unsubscribed` \| `cleaned` \| `pending` \| `transactional`. Filter `WHERE mailchimp_status='subscribed'` for sendable list. |
| `email_open_count` | INT64 | **Lifetime Unique Campaign Opens** — count of distinct campaigns this user opened. NOT total open events. |
| `email_click_count` | INT64 | **Lifetime Unique Campaign Clicks**. |
| `last_email_open` | TIMESTAMP | **Most Recent Email Open** — UTC. NULL = never opened any campaign. |
| `unique_email_opens` | INT64 | **Distinct Campaigns Opened (deduped)**. |
| `preferred_email_hour` | INT64 | **Best Send Hour (0-23 UTC)** — only set when >=5 click events observed. Use for send-time optimization. |
| `preferred_email_dow` | INT64 | **Best Day of Week (1=Sun, 7=Sat)** — only set when >=5 click events. |
| `total_sessions` | INT64 | **Lifetime Web Sessions** — from GA4. |
| `total_pageviews` | INT64 | **Lifetime Pageviews**. |
| `last_seen_web` | TIMESTAMP | **Most Recent Web Visit**. |
| `days_retained` | INT64 | **Days Between First & Last Web Visit** — high value = loyal user. |
| `engagement_tier` | STRING (ENUM) | **Computed Activity Level** — `high` \| `medium` \| `low` \| `inactive`. **Use this for "active users" questions.** Recomputed daily. |
| `predicted_ltv` | FLOAT64 | **Predicted Lifetime Value (Ad Revenue Model)** — USD. Computed monthly. |
| `total_form_submissions` | INT64 | **Form Submits** (newsletter signups, contact forms, polls). |
| `total_video_views` | INT64 | **Videos Played** (JW Player). |
| `total_forum_interactions` | INT64 | **Forum Posts + Replies + Reactions**. |
| `bp_activity_count` | INT64 | **BuddyPress Activity Count** — total community posts/updates. |
| `bp_group_count` | INT64 | **Group Memberships**. |
| `has_paid_subscription` | BOOL | **Paid Member** — TRUE = active paid subscription. |
| `subscription_status` | STRING | `active` \| `expired` \| `pending` \| NULL. |
| `consent_analytics` | BOOL | **Analytics Cookie Consent** — from OneTrust. |
| `consent_advertising` | BOOL | **Ad Cookie Consent**. |
| `first_facebook_click` | TIMESTAMP | **First Meta Ad Click** — for attribution. |
| `last_google_click` | TIMESTAMP | **Most Recent Google Ad Click**. |
| `primary_device` | STRING | **Most Recent Device** — `desktop` \| `mobile` \| `tablet`. |
| `ga4_country` | STRING | **Most Recent Geo (GA4)** — overrides profile_core.country when present. |
| `ga4_channel_grouping` | STRING | **Acquisition Channel (GA4)** — `Organic Search` \| `Paid Search` \| `Social` \| `Email` \| `Direct` \| `Referral`. |

### 6.3 Table: `profile_identifiers` (1:N Identity Keys)

```
TABLE DESCRIPTION:
Business Purpose: All identifier types (email, phone, NPI, cookies, etc.) linked
  to each person. Used for matching incoming data to bn_id, debugging identity
  graph issues, and audience export.
Grain: One row per (bn_id, identifier_type, identifier_value) — many rows per person.
Source System: Identity Hub.
Update Frequency: Per refresh.
Primary Key: (bn_id, identifier_type, identifier_value).
Foreign Key: bn_id → profile_core.bn_id.
Typical Questions:
  - "Which emails are linked to this person?"
  - "How many unique phone numbers do we have?"
  - "Which users have an NPI?"
PII Level: HIGH (contains raw emails, phones, NPIs).
Row Count: ~18.6M.
```

**Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `bn_id` | STRING (FK) | Foreign key to profile_core. |
| `identifier_type` | STRING (ENUM) | Type of ID: `email` \| `phone` \| `npi_number` \| `bionews_uk` \| `wp_user_id` \| `bnfpvid` \| `mc_euid` \| `client_id` \| `fbp` \| `gcl_au` \| `aim_dgid` \| `participant_id` \| `subscriber_hash` (and 25+ more). |
| `identifier_value` | STRING | Actual value (e.g., `john@example.com`). PII level depends on type. |
| `source_system` | STRING | Where this ID was observed — `mailchimp` \| `wordpress` \| `ga4` \| `npi_registry` \| `acceptor` \| `limesurvey` etc. |
| `confidence` | FLOAT64 | **Match Confidence** (0.0-1.0) — how sure we are this ID belongs to this person. |
| `first_seen` | TIMESTAMP | When this ID was first linked. |
| `last_seen` | TIMESTAMP | Most recent observation. |
| `is_primary` | BOOL | TRUE if this is the preferred form of this type (e.g., primary email vs. alias). |

### 6.4 Table: `profile_segment_tags` (Governed Tags)

```
TABLE DESCRIPTION:
Business Purpose: Tag-based segmentation from Mailchimp and manual curation.
  Use for audience targeting and segment sizing.
Grain: One row per (bn_id, tag_category, tag_value).
Update Frequency: Daily.
Typical Questions:
  - "How many users tagged as VIP?"
  - "What's the audience for the ALS list?"
  - "Geographic distribution of patients?"
Row Count: ~9M.
```

| Column | Type | Description |
|--------|------|-------------|
| `bn_id` | STRING (FK) | Foreign key to profile_core. |
| `tag_category` | STRING (ENUM) | `geography` \| `hcp_verification` \| `condition` \| `engagement_segment`. |
| `tag_value` | STRING | Specific tag (e.g., `state_california`, `verified_npi`, `condition_als`, `high_engagement`). |
| `source` | STRING | `mailchimp_tag` or `manual`. |

### 6.5 Table: `profile_content_affinity` (Browsing Patterns)

```
TABLE DESCRIPTION:
Business Purpose: Per-user content consumption by condition site. Indicates which
  conditions a user browses (may differ from their stated preferred_condition).
Grain: One row per (bn_id, content_condition).
Update Frequency: Daily.
Typical Questions:
  - "Which conditions does this patient browse beyond their primary?"
  - "What's the audience for ALS content?"
Row Count: ~3.2M.
```

| Column | Type | Description |
|--------|------|-------------|
| `bn_id` | STRING (FK) | Foreign key. |
| `content_condition` | STRING | Condition site key (`als`, `mg`, `nmo`, etc.). Matches `conditions_dict.condition_key`. |
| `pageview_count` | INT64 | Lifetime pageviews on this condition's content. |
| `active_days` | INT64 | Distinct days user viewed this condition. |
| `last_viewed` | TIMESTAMP | Most recent view. |
| `affinity_score` | FLOAT64 | **Recency × Frequency Decay Score** (0.0-1.0). Higher = stronger affinity. |

### 6.6 Table: `site_events` (Granular Event Log)

```
TABLE DESCRIPTION:
Business Purpose: Event-level activity log for funnel analysis, attribution, and
  conversion tracking. Every meaningful interaction is recorded here.
Grain: One row per event (event_id is unique UUID).
Source System: BioNews Acceptor → GA4 → site_events table.
Update Frequency: Near real-time (15-min lag).
Partitioned By: DATE(event_timestamp). **ALWAYS add a date filter**.
Clustered By: event_name, bn_id.
Typical Questions:
  - "How many newsletter signups last month?"
  - "Funnel from page_view to form_submission?"
  - "Most popular videos by condition?"
PII Level: MEDIUM (bn_id present; page_path may contain query params).
Row Count: ~156M (and growing).
```

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | STRING (PK) | UUID. |
| `bn_id` | STRING (FK) | Foreign key to profile_core. NULL if anonymous. |
| `bnfpvid` | STRING | Browser visitor ID. Always present. |
| `event_name` | STRING (ENUM) | Canonical event — `page_view` \| `scroll` \| `newsletter_signup` \| `form_submission` \| `user_registration` \| `user_login` \| `ad_impression` \| `ad_click` \| `video_view` \| `podcast_play` \| `article_comment` \| `forum_interaction` \| `file_download` \| `external_link_click` \| `cookie_notice_accept` \| `aim_signal` (and 10+ more). |
| `event_category` | STRING | `engagement` \| `conversion` \| `ad` \| `media` \| `community` \| `compliance`. |
| `event_timestamp` | TIMESTAMP | When the event occurred (UTC). **Always filter on this**. |
| `site_domain` | STRING | Which property (e.g., `myasthenia-gravis.com`). |
| `page_path` | STRING | URL path. |
| `page_title` | STRING | Page title at event time. |
| `event_label` | STRING | Type-specific (form name, ad unit, video title). |
| `event_value` | FLOAT64 | Monetary value (ad revenue) for ad_impression events. |
| `traffic_source` | STRING | UTM source — `google` \| `facebook` \| `newsletter` \| etc. |
| `traffic_medium` | STRING | UTM medium — `cpc` \| `organic` \| `email`. |
| `traffic_campaign` | STRING | Campaign name. |
| `device_category` | STRING | `desktop` \| `mobile` \| `tablet`. |

### 6.7 Table: `profile_preferences` (Subscriptions & Settings)

```
TABLE DESCRIPTION:
Business Purpose: Newsletter subscriptions and BuddyBoss/forum settings per user.
Grain: One row per bn_id (1:1).
Typical Questions:
  - "Which newsletters does this user subscribe to?"
  - "How many users opted into forum notifications?"
Row Count: ~2.5M.
```

| Column | Type | Description |
|--------|------|-------------|
| `bn_id` | STRING (FK) | Foreign key. |
| `newsletter_preferences` | ARRAY<STRUCT> | Array of newsletter subscriptions: STRUCT<site_domain, newsletter_key, newsletter_label, is_subscribed BOOL, subscribed_at, unsubscribed_at, source>. UNNEST to query. |
| `forum_settings` | STRUCT | Forum/BuddyBoss config: STRUCT<notify_forum_replies, notify_group_invites, notify_direct_messages, notify_mentions, subscribed_forum_ids, subscribed_group_ids, profile_visibility, show_activity_feed, forum_registered_at, last_forum_activity>. |

### 6.8 Table: `profile_survey_data` (LimeSurvey Responses)

```
TABLE DESCRIPTION:
Business Purpose: Survey responses mapped to Common Data Elements (CDE).
Grain: One row per (bn_id, survey_id, response_id, field_name).
Source System: LimeSurvey.
Typical Questions:
  - "How many users answered the symptom severity question?"
  - "Average treatment satisfaction by condition?"
Row Count: ~445K.
```

| Column | Type | Description |
|--------|------|-------------|
| `bn_id` | STRING (FK) | Foreign key. |
| `survey_id` | INT64 | LimeSurvey survey ID. |
| `response_id` | STRING | Unique response ID. |
| `cde_id` | STRING | Common Data Element ID (standardized question). |
| `field_name` | STRING | Question key. |
| `value_text` | STRING | Text answer. |
| `value_numeric` | FLOAT64 | Numeric answer. |
| `value_code` | STRING | Coded answer (multi-choice). |
| `value_array` | ARRAY<STRING> | Multi-select answers. |
| `response_submitted_at` | TIMESTAMP | When survey was completed. |

### 6.9 Tables: Dictionaries (Master Data)

#### `conditions_dict` — Conditions Vocabulary

```
TABLE DESCRIPTION:
Business Purpose: Master list of all health conditions tracked by the platform.
  JOIN to profile_core via preferred_condition.mesh_id = conditions_dict.mesh_id.
Grain: One row per unique condition.
Source System: Curated by Medical Content Team. External source: MeSH.
Update Frequency: Quarterly.
Primary Key: condition_key.
Row Count: ~63.
```

| Column | Type | Description |
|--------|------|-------------|
| `condition_key` | STRING (PK) | Machine key — `myasthenia_gravis`, `als`, `nmo`. |
| `label` | STRING | Display name — "Myasthenia Gravis". |
| `mesh_id` | STRING | **JOIN KEY** — MeSH descriptor ID — `D009157`. JOIN: `ON profile_core.preferred_condition.mesh_id = conditions_dict.mesh_id`. |
| `mesh_tree_number` | STRING | MeSH hierarchy code. |
| `icd10_code` | STRING | ICD-10 code if applicable. |
| `condition_category` | STRING | `neuromuscular` \| `autoimmune` \| `rare_disease` etc. |
| `aliases` | ARRAY<STRING> | Search aliases. |
| `description` | STRING | Clinical description. |
| `is_active` | BOOL | TRUE = currently tracked. |

#### `treatments_dict` — Treatments Vocabulary

| Column | Type | Description |
|--------|------|-------------|
| `treatment_key` | STRING (PK) | Machine key — `vyvgart`, `mestinon`. |
| `label` | STRING | Brand name — "Vyvgart". |
| `generic_name` | STRING | INN name — "efgartigimod alfa". |
| `rxnorm_id` | STRING | **JOIN KEY** — RxNorm concept ID. |
| `treatment_class` | STRING | `biologic` \| `immunosuppressant` \| `symptomatic` etc. |
| `applicable_conditions` | ARRAY<STRING> | Condition keys this treats. |
| `fda_approval_year` | INT64 | Year of FDA approval. |

#### `symptoms_dict` — Symptoms Vocabulary

| Column | Type | Description |
|--------|------|-------------|
| `symptom_key` | STRING (PK) | Machine key — `fatigue`, `muscle_weakness`. |
| `label` | STRING | Display label. |
| `mesh_id` | STRING | MeSH ID. |
| `hpo_id` | STRING | Human Phenotype Ontology ID. |
| `applicable_conditions` | ARRAY<STRING> | Conditions this symptom applies to. |

#### `profile_lookup` — Consolidated Enums

```
TABLE DESCRIPTION:
Business Purpose: Single lookup table for all enum/picklist values used across
  profile_core (diagnosis_stage values, gender values, engagement_tier values, etc.).
Grain: One row per (lookup_type, lookup_key).
Use Case: Render dropdowns in UI; agent uses this to know valid enum values.
Row Count: ~300.
```

| Column | Type | Description |
|--------|------|-------------|
| `lookup_type` | STRING | Category — `diagnosis_stage`, `gender`, `engagement_tier`, `condition_subtype`. |
| `lookup_key` | STRING | Machine value — `newly_diagnosed`, `male`, `high`. |
| `label` | STRING | Display value — "Newly Diagnosed". |
| `description` | STRING | Optional description. |
| `sort_order` | INT64 | UI ordering. |

---

## 7. 25 SAMPLE QUESTIONS WITH SQL {#sample-questions}

### PERSONA & SEGMENTATION

**Q1. How many HCPs do we have, and what are the top specialties?**
```sql
SELECT
  specialty.label AS specialty,
  COUNT(DISTINCT bn_id) AS hcp_count,
  COUNTIF(npi_number IS NOT NULL) AS verified_npi_count
FROM `bi-data-391216.profile_data.profile_core`
WHERE account_type = 'hcp'
GROUP BY specialty
ORDER BY hcp_count DESC
LIMIT 20;
```

**Q2. Which patients are in their first 6 months post-diagnosis?**
```sql
SELECT COUNT(DISTINCT bn_id) AS newly_diagnosed_count
FROM `bi-data-391216.profile_data.profile_core`
WHERE account_type = 'patient'
  AND diagnosis_timing_band = '<6mo';
```

**Q3. Audience breakdown by condition?**
```sql
SELECT
  preferred_condition.label AS condition,
  COUNT(DISTINCT bn_id) AS user_count,
  COUNTIF(account_type='patient') AS patients,
  COUNTIF(account_type='caregiver') AS caregivers,
  COUNTIF(account_type='hcp') AS hcps
FROM `bi-data-391216.profile_data.profile_core`
WHERE preferred_condition IS NOT NULL
GROUP BY condition
ORDER BY user_count DESC
LIMIT 25;
```

**Q4. High-engagement users by persona?**
```sql
SELECT
  pc.account_type,
  COUNT(DISTINCT pc.bn_id) AS high_engagement_users,
  ROUND(AVG(pe.email_open_count), 1) AS avg_opens,
  ROUND(AVG(pe.total_sessions), 1) AS avg_sessions
FROM `bi-data-391216.profile_data.profile_core` pc
JOIN `bi-data-391216.profile_data.profile_engagement` pe USING(bn_id)
WHERE pe.engagement_tier = 'high'
GROUP BY pc.account_type
ORDER BY high_engagement_users DESC;
```

**Q5. Caregivers focused on financial vs. emotional support?**
```sql
SELECT
  focus_area,
  COUNT(DISTINCT bn_id) AS caregiver_count
FROM `bi-data-391216.profile_data.profile_core`,
  UNNEST(caregiver_focus_areas) AS focus_area
WHERE account_type = 'caregiver'
GROUP BY focus_area
ORDER BY caregiver_count DESC;
```

### ENGAGEMENT & RETENTION

**Q6. Churn rate by acquisition cohort?**
```sql
WITH cohorts AS (
  SELECT
    DATE_TRUNC(DATE(created_at), MONTH) AS signup_month,
    bn_id,
    last_active_at
  FROM `bi-data-391216.profile_data.profile_core`
  WHERE created_at >= '2025-01-01'
)
SELECT
  signup_month,
  COUNT(DISTINCT bn_id) AS cohort_size,
  COUNTIF(last_active_at >= CURRENT_TIMESTAMP() - INTERVAL 180 DAY) AS still_active,
  ROUND(100.0 * COUNTIF(last_active_at >= CURRENT_TIMESTAMP() - INTERVAL 180 DAY)
        / COUNT(DISTINCT bn_id), 1) AS retention_pct
FROM cohorts
GROUP BY signup_month
ORDER BY signup_month DESC;
```

**Q7. Users who opened at least one email in the last 90 days?**
```sql
SELECT COUNT(DISTINCT bn_id) AS active_email_users
FROM `bi-data-391216.profile_data.profile_engagement`
WHERE last_email_open >= CURRENT_TIMESTAMP() - INTERVAL 90 DAY;
```

**Q8. Optimal send time by engagement tier?**
```sql
SELECT
  engagement_tier,
  preferred_email_hour,
  preferred_email_dow,
  COUNT(DISTINCT bn_id) AS user_count
FROM `bi-data-391216.profile_data.profile_engagement`
WHERE preferred_email_hour IS NOT NULL
GROUP BY engagement_tier, preferred_email_hour, preferred_email_dow
ORDER BY engagement_tier, user_count DESC;
```

**Q9. Highest-engagement content topics (last 90 days)?**
```sql
SELECT
  page_title,
  COUNT(*) AS event_count,
  COUNT(DISTINCT bn_id) AS unique_users,
  COUNTIF(event_name = 'scroll') AS scroll_events,
  COUNTIF(event_name = 'video_view') AS video_plays
FROM `bi-data-391216.profile_data.site_events`
WHERE event_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 90 DAY
  AND event_category = 'engagement'
GROUP BY page_title
ORDER BY unique_users DESC
LIMIT 25;
```

**Q10. Email engagement vs. community engagement correlation?**
```sql
SELECT
  CASE
    WHEN email_open_count >= 10 AND bp_activity_count >= 5 THEN 'both_active'
    WHEN email_open_count >= 10 THEN 'email_only'
    WHEN bp_activity_count >= 5 THEN 'community_only'
    ELSE 'neither'
  END AS engagement_segment,
  COUNT(DISTINCT bn_id) AS user_count
FROM `bi-data-391216.profile_data.profile_engagement`
GROUP BY engagement_segment
ORDER BY user_count DESC;
```

### CONDITION & CONTENT

**Q11. Most engaged patients vs. HCPs per condition?**
```sql
SELECT
  preferred_condition.label AS condition,
  COUNTIF(account_type='patient' AND engagement_tier='high') AS engaged_patients,
  COUNTIF(account_type='hcp' AND engagement_tier='high') AS engaged_hcps
FROM `bi-data-391216.profile_data.profile_core` pc
JOIN `bi-data-391216.profile_data.profile_engagement` pe USING(bn_id)
WHERE preferred_condition IS NOT NULL
GROUP BY condition
ORDER BY engaged_patients DESC;
```

**Q12. Top treatments discussed by patients per condition?**
```sql
SELECT
  pc.preferred_condition.label AS condition,
  treatment.label AS treatment,
  COUNT(DISTINCT pc.bn_id) AS patient_count
FROM `bi-data-391216.profile_data.profile_core` pc,
  UNNEST(treatments_current) AS treatment
WHERE pc.account_type = 'patient'
GROUP BY condition, treatment
ORDER BY condition, patient_count DESC
LIMIT 100;
```

**Q13. Do users browsing treatment content engage more?**
```sql
WITH treatment_browsers AS (
  SELECT DISTINCT bn_id
  FROM `bi-data-391216.profile_data.site_events`
  WHERE event_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 180 DAY
    AND page_path LIKE '%/treatment%'
)
SELECT
  CASE WHEN tb.bn_id IS NOT NULL THEN 'treatment_browser' ELSE 'other' END AS segment,
  COUNT(DISTINCT pc.bn_id) AS user_count,
  AVG(pe.email_open_count) AS avg_email_opens,
  COUNTIF(pe.engagement_tier='high') AS high_engagement_count
FROM `bi-data-391216.profile_data.profile_core` pc
JOIN `bi-data-391216.profile_data.profile_engagement` pe USING(bn_id)
LEFT JOIN treatment_browsers tb USING(bn_id)
WHERE pc.account_type = 'patient'
GROUP BY segment;
```

**Q14. Which conditions do caregivers support?**
```sql
SELECT
  caregiver_condition.label AS condition,
  COUNT(DISTINCT bn_id) AS caregiver_count
FROM `bi-data-391216.profile_data.profile_core`
WHERE account_type = 'caregiver'
  AND caregiver_condition IS NOT NULL
GROUP BY condition
ORDER BY caregiver_count DESC;
```

**Q15. Profile completeness by condition?**
```sql
SELECT
  preferred_condition.label AS condition,
  COUNT(DISTINCT bn_id) AS user_count,
  ROUND(AVG(profile_completeness), 1) AS avg_completeness,
  COUNTIF(profile_completeness < 50) AS incomplete_profiles
FROM `bi-data-391216.profile_data.profile_core`
WHERE preferred_condition IS NOT NULL
GROUP BY condition
ORDER BY user_count DESC;
```

### ACQUISITION & ATTRIBUTION

**Q16. LTV by acquisition source?**
```sql
SELECT
  pc.acquisition_source,
  COUNT(DISTINCT pc.bn_id) AS user_count,
  ROUND(AVG(pe.predicted_ltv), 2) AS avg_ltv,
  ROUND(SUM(pe.predicted_ltv), 2) AS total_ltv
FROM `bi-data-391216.profile_data.profile_core` pc
JOIN `bi-data-391216.profile_data.profile_engagement` pe USING(bn_id)
WHERE pc.acquisition_source IS NOT NULL
GROUP BY pc.acquisition_source
ORDER BY total_ltv DESC;
```

**Q17. Engagement tier distribution by acquisition source?**
```sql
SELECT
  pc.acquisition_source,
  pe.engagement_tier,
  COUNT(DISTINCT pc.bn_id) AS user_count
FROM `bi-data-391216.profile_data.profile_core` pc
JOIN `bi-data-391216.profile_data.profile_engagement` pe USING(bn_id)
WHERE pc.acquisition_source IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;
```

**Q18. Users retargetable on Facebook & Google?**
```sql
SELECT
  COUNT(DISTINCT bn_id) AS retargetable_users,
  COUNTIF(platform = 'facebook') AS facebook_retargetable,
  COUNTIF(platform = 'google_ads') AS google_retargetable
FROM `bi-data-391216.profile_data.profile_ad_attribution`
WHERE last_seen >= CURRENT_TIMESTAMP() - INTERVAL 90 DAY;
```

**Q19. Conversion funnel from visit to newsletter signup (last 90 days)?**
```sql
WITH visitors AS (
  SELECT DISTINCT bn_id, MIN(event_timestamp) AS first_visit
  FROM `bi-data-391216.profile_data.site_events`
  WHERE event_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 90 DAY
    AND event_name = 'page_view'
    AND bn_id IS NOT NULL
  GROUP BY bn_id
),
signups AS (
  SELECT DISTINCT bn_id
  FROM `bi-data-391216.profile_data.site_events`
  WHERE event_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 90 DAY
    AND event_name = 'newsletter_signup'
)
SELECT
  COUNT(DISTINCT v.bn_id) AS visitors,
  COUNT(DISTINCT s.bn_id) AS signups,
  ROUND(100.0 * COUNT(DISTINCT s.bn_id) / NULLIF(COUNT(DISTINCT v.bn_id), 0), 2) AS conversion_pct
FROM visitors v
LEFT JOIN signups s USING(bn_id);
```

**Q20. Paid vs. organic engagement difference?**
```sql
SELECT
  pc.acquisition_medium,
  COUNT(DISTINCT pc.bn_id) AS user_count,
  ROUND(AVG(pe.total_sessions), 1) AS avg_sessions,
  ROUND(AVG(pe.email_open_count), 1) AS avg_email_opens,
  COUNTIF(pe.engagement_tier='high') AS high_engagement_users
FROM `bi-data-391216.profile_data.profile_core` pc
JOIN `bi-data-391216.profile_data.profile_engagement` pe USING(bn_id)
WHERE pc.acquisition_medium IN ('cpc', 'organic_search')
GROUP BY pc.acquisition_medium;
```

### COMPLIANCE & CONSENT

**Q21. Marketing opt-in rate by persona?**
```sql
SELECT
  account_type,
  COUNT(DISTINCT bn_id) AS total_users,
  COUNTIF(communication_opt_in = TRUE) AS opted_in,
  ROUND(100.0 * COUNTIF(communication_opt_in = TRUE) / COUNT(DISTINCT bn_id), 1) AS opt_in_pct
FROM `bi-data-391216.profile_data.profile_core`
WHERE account_type IS NOT NULL
GROUP BY account_type
ORDER BY total_users DESC;
```

**Q22. Users without tracking consent?**
```sql
SELECT COUNT(DISTINCT bn_id) AS no_tracking_users
FROM `bi-data-391216.profile_data.profile_core`
WHERE tracking_consent = FALSE
  OR tracking_consent IS NULL;
```

**Q23. High-risk identity clusters (shared workstation, suspicious)?**
```sql
SELECT
  COUNT(DISTINCT bn_id) AS flagged_users,
  COUNTIF(is_shared_workstation = TRUE) AS shared_workstation,
  COUNTIF(is_suspicious = TRUE) AS suspicious,
  ROUND(AVG(cluster_health_score), 1) AS avg_health_score
FROM `bi-data-391216.profile_data.profile_core`
WHERE is_shared_workstation = TRUE OR is_suspicious = TRUE;
```

### ADVANCED

**Q24. High-intent clinical trial candidates?**
```sql
SELECT
  pc.preferred_condition.label AS condition,
  pc.diagnosis_stage,
  COUNT(DISTINCT pc.bn_id) AS candidate_count
FROM `bi-data-391216.profile_data.profile_core` pc
JOIN `bi-data-391216.profile_data.profile_engagement` pe USING(bn_id)
WHERE pc.clinical_trials_interest = TRUE
  AND pc.account_type IN ('patient', 'caregiver')
  AND pe.engagement_tier IN ('high', 'medium')
  AND pc.communication_opt_in = TRUE
GROUP BY condition, diagnosis_stage
ORDER BY candidate_count DESC;
```

**Q25. Profile completeness trajectory: engaged vs. churned?**
```sql
SELECT
  pe.engagement_tier,
  COUNT(DISTINCT pc.bn_id) AS user_count,
  ROUND(AVG(pc.profile_completeness), 1) AS avg_completeness,
  ROUND(AVG(DATE_DIFF(CURRENT_DATE(), DATE(pc.created_at), DAY)), 0) AS avg_age_days
FROM `bi-data-391216.profile_data.profile_core` pc
JOIN `bi-data-391216.profile_data.profile_engagement` pe USING(bn_id)
GROUP BY pe.engagement_tier
ORDER BY avg_completeness DESC;
```

---

## 8. FEW-SHOT EXAMPLES FOR THE AGENT {#few-shot-examples}

**Paste these into Vertex AI Agent Builder → Examples panel.**

These teach the agent your business logic. Add 5-10 examples covering common patterns.

### Example 1
**User**: How many patients do we have?
**SQL**:
```sql
SELECT COUNT(DISTINCT bn_id) AS patient_count
FROM `bi-data-391216.profile_data.profile_core`
WHERE account_type = 'patient';
```
**Why this SQL**: Filters account_type='patient'; uses COUNT(DISTINCT bn_id) for unique people.

### Example 2
**User**: Show me active users by condition.
**Interpretation**: "Active" = engagement_tier IN ('high', 'medium'). Always JOIN profile_engagement.
**SQL**:
```sql
SELECT
  pc.preferred_condition.label AS condition,
  COUNT(DISTINCT pc.bn_id) AS active_users
FROM `bi-data-391216.profile_data.profile_core` pc
JOIN `bi-data-391216.profile_data.profile_engagement` pe USING(bn_id)
WHERE pe.engagement_tier IN ('high', 'medium')
  AND pc.preferred_condition IS NOT NULL
GROUP BY condition
ORDER BY active_users DESC;
```

### Example 3
**User**: VIP users with Myasthenia Gravis.
**Interpretation**: "VIP" = engagement_tier='high' AND has_paid_subscription=TRUE.
**SQL**:
```sql
SELECT COUNT(DISTINCT pc.bn_id) AS vip_count
FROM `bi-data-391216.profile_data.profile_core` pc
JOIN `bi-data-391216.profile_data.profile_engagement` pe USING(bn_id)
WHERE pc.preferred_condition.label = 'Myasthenia Gravis'
  AND pe.engagement_tier = 'high'
  AND pe.has_paid_subscription = TRUE;
```

### Example 4
**User**: Newsletter signups last month.
**Interpretation**: Use site_events with event_name='newsletter_signup'. Filter by event_timestamp.
**SQL**:
```sql
SELECT COUNT(*) AS signups
FROM `bi-data-391216.profile_data.site_events`
WHERE event_name = 'newsletter_signup'
  AND DATE(event_timestamp) BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
                                 AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY);
```

### Example 5
**User**: HCPs in California treating ALS.
**Interpretation**: account_type='hcp'; practice_state filter; condition_focus is ARRAY (UNNEST).
**SQL**:
```sql
SELECT COUNT(DISTINCT pc.bn_id) AS hcp_count
FROM `bi-data-391216.profile_data.profile_core` pc,
  UNNEST(pc.condition_focus) AS cf
WHERE pc.account_type = 'hcp'
  AND pc.practice_state IN ('CA', 'California')
  AND cf.label = 'ALS';
```

### Example 6
**User**: Email opt-in rate.
**Interpretation**: communication_opt_in is BOOL; calculate percentage; treat NULL as not opted in.
**SQL**:
```sql
SELECT
  COUNT(DISTINCT bn_id) AS total_users,
  COUNTIF(communication_opt_in = TRUE) AS opted_in,
  ROUND(100.0 * COUNTIF(communication_opt_in = TRUE) / COUNT(DISTINCT bn_id), 2) AS opt_in_pct
FROM `bi-data-391216.profile_data.profile_core`;
```

### Example 7
**User**: Show me users at risk of churning.
**Interpretation**: "At risk" = engagement_tier IN ('low','inactive') AND last_active_at between 30-90 days ago (recent activity declined but not fully churned yet).
**SQL**:
```sql
SELECT COUNT(DISTINCT pc.bn_id) AS churn_risk_users
FROM `bi-data-391216.profile_data.profile_core` pc
JOIN `bi-data-391216.profile_data.profile_engagement` pe USING(bn_id)
WHERE pe.engagement_tier IN ('low', 'inactive')
  AND pe.last_seen_web BETWEEN CURRENT_TIMESTAMP() - INTERVAL 90 DAY
                            AND CURRENT_TIMESTAMP() - INTERVAL 30 DAY
  AND pc.communication_opt_in = TRUE;
```

### Example 8
**User**: Send me the email addresses of high-engagement patients.
**Response (NO SQL)**: I can show aggregated counts and segments, but I can't export raw email addresses without an explicit data export approval. For aggregate stats: "There are 14,328 high-engagement patients across 12 conditions. Want me to break this down by condition or acquisition source?" If you need the actual email list for a campaign, please request a tier-2 audience export via the data ops team.

---

## 9. SQL PATTERNS & ANTI-PATTERNS {#sql-patterns}

### Always Use These Patterns

| Pattern | Example | Why |
|---------|---------|-----|
| `COUNT(DISTINCT bn_id)` | `COUNT(DISTINCT bn_id) FROM profile_core` | Avoids double-counting from 1:N joins |
| `JOIN ... USING(bn_id)` | `JOIN profile_engagement USING(bn_id)` | Cleaner; bn_id is the universal key |
| Date filters on `site_events` | `WHERE event_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 90 DAY` | 156M rows; date filter is mandatory |
| Filter `account_type` first | `WHERE account_type='patient' AND treatments_current IS NOT NULL` | Persona-specific fields are NULL for other types |
| `UNNEST` for ARRAY columns | `FROM profile_core, UNNEST(treatments_current) AS t` | ARRAY can't be queried with dot notation |
| Dot notation for STRUCT | `preferred_condition.label`, `specialty.snomed_id` | STRUCT access syntax |
| Backtick fully-qualified names | `` `bi-data-391216.profile_data.profile_core` `` | Required for cross-project queries |

### Never Do These Things

| Anti-pattern | Why It's Wrong | Correct Version |
|--------------|----------------|------------------|
| `COUNT(*) FROM profile_core JOIN profile_identifiers` | Multiplies rows (1:N join) | `COUNT(DISTINCT bn_id)` |
| `SELECT * FROM site_events` | Scans 156M rows; will timeout | Add `WHERE event_timestamp >= ...` |
| `WHERE specialty.label = 'Neurology'` (no account_type filter) | Other personas have NULL specialty | Add `AND account_type='hcp'` |
| `WHERE email = 'user@example.com'` | PII leak; bypasses governance | Use `email_hash` or filter via segment tags |
| `GROUP BY bn_id, account_type, gender` (no aggregate) | Returns row-level PII | Add `COUNT(DISTINCT bn_id)` |
| `WHERE created_at >= '2020-01-01'` | 6+ years of data; expensive | Use rolling window: `>= CURRENT_DATE() - 365` |
| Treating `treatments_current` as STRING | It's an ARRAY<STRUCT> | UNNEST it first |
| `WHERE communication_opt_in = TRUE OR NULL` | NULL is not TRUE; don't conflate | Just `WHERE communication_opt_in = TRUE` |

---

## 10. GOVERNANCE, PII & SECURITY {#governance}

### PII Classification

| Level | Examples | Tier-1 Users See | Tier-2 Users See |
|-------|----------|-------------------|-------------------|
| **HIGH** | email, phone, first_name, last_name, address_postal_code | Masked (`j***@example.com`) | Full value (with audit log) |
| **MEDIUM** | age_exact, gender, ethnicity, condition, diagnosis_stage | Aggregated only | Full value |
| **LOW** | npi_number, country, state, account_type | Full value | Full value |
| **NONE** | bn_id, engagement_tier, profile_completeness, counts | Full value | Full value |

### Required Filters (Compliance)

```sql
-- For email campaigns:
WHERE communication_opt_in = TRUE

-- For ad retargeting:
WHERE consent_advertising = TRUE

-- For analytics dashboards:
WHERE tracking_consent = TRUE

-- For "active user" segments:
WHERE cluster_tier = 'tier1'  -- Exclude anonymous tier-2
  AND is_suspicious = FALSE
  AND is_shared_workstation = FALSE
```

### Audit Logging
Every agent query is automatically logged to `profile_ops_audit.agent_query_log` with:
- User email
- Question asked
- Generated SQL
- Result row count
- Execution time
- PII fields returned (count + types)
- Timestamp

### Query Limits
- Max bytes scanned: 10 GB
- Max execution time: 600 seconds
- Max rows returned: 50,000
- Required date filter for `site_events`

### Pro-Tips for the Agent
1. When asked for "the list of...", prefer COUNT + segment breakdown over raw rows.
2. When asked for PII, offer the aggregate alternative first.
3. When asked an ambiguous question ("active users"), pick the most common interpretation
   and clearly state your interpretation in the response.
4. When asked about a long time window (multi-year), suggest a rolling window or pre-aggregated table.
5. When generating SQL, format it readably (line breaks, indentation) — users see the SQL.

---

## END OF DOCUMENT

**Document length**: ~1,400 lines | **Word count**: ~13,000 | **Designed for**: Single-source Gemini Agent reference

**For Vertex AI Agent Builder**: Upload this document as the agent's reference document. Use Section 1 as the system prompt, Section 7 SQL examples as few-shot training, and Sections 5-6 as the metadata layer for BigQuery schema enrichment.

**Owner**: Data Intelligence Team — `data-team@bionews.com`
**Last Updated**: 2026-05-13
