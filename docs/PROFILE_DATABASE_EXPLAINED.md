# Profile Database, Explained

**Dataset:** `bi-data-391216.profile_data` · **Schema:** v6.7
**Numbers verified against production 2026-08-18.**

How the profile database actually behaves, and the things that will produce a
confidently wrong answer if you do not know them.

| Document | Use it for |
|---|---|
| [PROFILE_DATABASE_DATA_DICTIONARY.md](PROFILE_DATABASE_DATA_DICTIONARY.md) | Every table, column and writer. **Auto-generated.** |
| **`profile_data.profile_metrics`** | **Start here for any audience number.** One row per bn_id with every headline metric as a boolean, and the traps below already solved. Slice it; do not redefine metrics. |
| [../sql/profile_database_query_cookbook.sql](../sql/profile_database_query_cookbook.sql) | Runnable queries, validated against production. |
| [IDENTITY_HUB_EXPLAINED.md](IDENTITY_HUB_EXPLAINED.md) | Who a person *is*. This document is what we know *about* them. |
| [PROFILE_DATABASE_COMPLETE_REFERENCE.md](PROFILE_DATABASE_COMPLETE_REFERENCE.md) | Row counts, `can_personalize`, known issues. |

---

## 1. What it is

One row per person in `profile_core`, keyed on `bn_id` -- the same key the
identity hub assigns. The hub decides *who someone is*; this database records
*what we know about them*, assembled from Mailchimp, WordPress, LimeSurvey, NPI,
GA4, AIM and the rest.

**6,122,846 profiles today.** But that number is nearly useless on its own, for
the reason in section 2.

---

## 2. tier1 vs tier2: read this before quoting any number

| Tier | Profiles | What it is |
|---|---:|---|
| **tier1** | **793,752** | has a durable anchor -- email, SSO key, NPI. A real person. |
| tier2 | 5,329,014 | cookie-only. Closer to a browser than a human. |

Almost everything that makes a profile useful only exists on tier1:

| Field | Populated |
|---|---:|
| `email` | 786,398 |
| any role flag set | 504,644 |
| `preferred_condition_normalized` | 3,165,865 |

Condition is the exception -- it reaches 3.17M because it can be inferred from
reading behaviour, not just self-report.

**5,618,202 profiles (92%) have no role flag at all.** That is not a data-quality
failure; it is what "anonymous browser" means. Any persona chart that includes
them is 92% "unspecified" and says nothing.

---

## 3. Roles are multi-valued, and they do not sum

v6.5 replaced a single `account_type` string with five independent BOOL flags.

| Flag | Profiles |
|---|---:|
| `is_hcp` | 387,107 |
| `is_patient` | 78,429 |
| `is_family_or_friend` | 20,010 |
| `is_other` | 11,971 |
| `is_caregiver` | 10,664 |
| **holds 2+ roles** | **3,479** |

Three consequences:

1. **The columns sum to more than the profile count.** A person can be both an
   HCP and a patient -- 3,479 are. Never present these as a pie chart.
2. **`NULL` is not `FALSE`.** NULL means no signal either way. `is_other`
   deliberately stays NULL until a positive signal arrives; it is not a default.
3. **For mutually exclusive buckets use `profile_roles.primary_role`**, which
   applies the priority `hcp > patient > caregiver > family_or_friend > other`.

Each flag carries a `_source`, `_confidence` and `_updated_at` triple, so you can
always ask why a flag is set. `is_hcp` is set only by `npi`,
`buddypress_xprofile`, or three Mailchimp paths -- **AIM never sets it.**

**HCPs are overwhelmingly the NPI registry, not signups.** 96.9% carry
`is_hcp_source = 'npi'`. This surfaces as an alarming spike -- 277,960 HCP
profiles share `created_at = 2026-06-20` -- which is simply the day the registry
was loaded, not a day a quarter of a million clinicians registered. Their real
dates are in `source_created_at` under basis `npi_enumeration` (304,015 of them),
which is the date the provider was enumerated federally. This is the clearest
illustration of why `created_at` must not be read as a signup date.

---

## 4. Dates: `created_at` is not when they joined

This is the single most expensive misunderstanding in the database.

| Column | What it means |
|---|---|
| `created_at` | when **our pipeline first observed** the profile. Re-stamped by backfills. |
| `source_created_at` | the **real acquisition date**, recovered from the source system. Write-once. |
| `source_created_basis` | **what kind of date it is.** The important one. |

Basis values in production:

| Basis | Profiles | Grade |
|---|---:|---|
| `cookie_creation` | 5,327,754 | **first sighting -- NOT a signup** |
| `mailchimp_signup` | 397,066 | registration |
| `npi_enumeration` | 304,039 | registration |
| `hub:wordpress/EMAIL_EXACT` | 32,677 | registration |
| `mailchimp_warehouse` | 21,836 | registration |
| `wordpress_registered` | 20,056 | registration |
| `hub:aim_clickstream/AIM_CLICKSTREAM` | 10,787 | registration |
| `survey_response` | 697 | registration |

**88% of dated profiles are `cookie_creation`**, which is a browser's first
appearance and is severely survivorship-biased: cookies get cleared, so old
months retain almost nothing while recent months retain most. Charting it
produced an apparent 396x rise in anonymous audience over a period when real GA4
traffic grew about 45%.

**Use `profile_growth_known`.** It enforces registration-grade only, structurally,
so the mistake cannot be made by forgetting a filter. `profile_growth_anonymous`
is hard-filtered to the window where cookie survival is reliable (from 2026-03).

---

## 5. Email engagement: raw opens are mostly not human

`profile_engagement` carries both `email_open_count` and
`human_email_open_count`. They are very far apart.

Measured 2026-08-18 across the 92.4% of opening profiles that carry a bot verdict:

| | Opens |
|---|---:|
| raw (`email_open_count`) | 72,704,503 |
| human (`human_email_open_count`) | 11,723,309 |
| **human share** | **16.1%** |

**About 84% of recorded opens are Apple MPP prefetch or scanner traffic.** Any
engagement metric built on `email_open_count` overstates reality by roughly 6x.

Two cautions:

- The "~50% of all opens" estimate that used to appear in four SQL files was the
  original guess. All four now carry the measured figure.
- `human_email_open_count` is **NULL** where Mailchimp issued no verdict
  (campaign not covered, or activity before 2025-03-03). NULL is not zero. Use
  `COALESCE(human_email_open_count, 0) > 0` deliberately, and know you are
  excluding 7.6% of opening profiles.

Clicks are not inflated this way, which makes `email_click_count` the more
trustworthy engagement signal.

Engagement tiers, for scale:

| Tier | Profiles |
|---|---:|
| inactive | 3,124,753 |
| low | 2,601,974 |
| medium | 232,964 |
| high | 130,472 |

---

## 6. Conditions: filter on the normalized column

`preferred_condition.label` carries both short codes (`ms`) and full names
(`Multiple Sclerosis`) for the same disease. Filtering on it splits audiences in
half without any error.

v6.6 added `preferred_condition_normalized`, which resolves every variant to one
`condition_key` through `conditions_dict`. **Filter on the normalized column;**
the original is kept for audit.

---

## 7. Build modes

| Mode | What it does |
|---|---|
| `rebuild` | recreates everything from source. The only mode that runs the `ddl` step. |
| `refresh` | nightly. MERGEs only profiles that changed in the lookback window. |
| `reenrich` | re-runs enrichment without re-extracting. |
| `views` | republishes views only. |

Two consequences that catch people out:

- **A column filled by rebuild enrichment is not necessarily recomputed nightly.**
  If a field looks stale, check which mode writes it -- the data dictionary lists
  the writers per table.
- **`rebuild` CTASes `profile_core`**, which drops columns added later by ALTER.
  The runtime schema sync re-adds them, and the DDL declares them, but this is why
  v6.7's acquisition columns needed both.

---

## 8. How a build actually runs

A rebuild is 33 ordered SQL steps, a refresh is 22 of them -- but the SQL is only
the middle. Three phases bracket it that are easy to miss because they are not in
the step list at all: a preflight gate before, and assertions plus a staged
publish after.

### Before the SQL: preflight

`run_preflight` refuses to start if the identity hub is not in a coherent state.
**Why:** `profile_core` is built entirely from the hub. If the hub is mid-run or
its `bn_id_xref` has collapsed, a rebuild would cheerfully produce a smaller,
wrong database and every downstream number would move. A broken hub fails
silently at the data layer, so this fails loudly at the plumbing layer first.

Three hard gates: the hub's most recent manifest row must be `PROMOTED`; xref row
count must be within +/-20% of the last rebuild; and for a refresh, no other
build may be running. Skipped for `reenrich` and `views`, which do not re-read
the hub. `--force` bypasses it, and should be rare.

### The SQL: six phases

Every step within a phase does the same kind of job, so the shape is learnable
without memorising 33 names.

| Phase | Steps | What it does | Why it exists / why here |
|---|---|---|---|
| **Protect** | `snapshot` | Copies app-written fields out to staging | The app owns fields the warehouse must not invent. A rebuild drops tables, so this runs FIRST or that data is gone. |
| **Create** | `ddl`, `maintenance` | Creates schema, seeds the dictionaries | Dictionaries must exist before anything resolves a condition against them. `ddl` is rebuild-only, which is why schema changes need a rebuild. |
| **Populate** | `populate_*` (5) | One row per person from the identity graph, plus satellites | Nothing can be enriched until the row exists. Order is deliberate: identity first, then engagement, survey, preferences, all keyed on `bn_id`. |
| **Fill gaps** | `fill_gaps_*` (4) | Pulls in what other systems already know: GA4, AIM, forum, ad attribution | Cheap broad coverage before expensive precise work. Low confidence by design -- later phases overwrite it with better evidence. |
| **Enrich** | `enrich_*` (11) | Each step derives one kind of meaning from one source | Separated so a single source can be fixed and re-run alone. Order matters: `enrich_condition_normalization` runs after every step that can set a condition, or it would normalise a value that changes afterwards. |
| **Classify** | `personas_*` (4) | Turns accumulated evidence into decisions: role flags, patient/caregiver detail, profile stage | Must run last among the writers -- a persona decided before enrichment finishes would be based on partial evidence. |
| **Publish** | `restore`, `views`, `snapshot_core` | Restores app fields, rebuilds views, snapshots for time travel | `restore` undoes what the rebuild would otherwise have overwritten. Views come last so consumers never see a half-built state. |

The rule in one line: **populate creates rows, fill_gaps and enrich add facts,
personas draw conclusions, publish exposes the result.**

### After the SQL: assertions, then a staged publish

This is the part missing from most mental models of the pipeline.

**Assertions run after the steps complete, not as a step.** That is why they do
not appear in the list above -- they are a gate over the whole build, described
in section 9.

**On a rebuild, nothing is published until the gate passes.** Tables are built in
`profile_data_candidate`, not `profile_data`. Only after the hard assertions pass
are they promoted, table by table, with each promotion recorded in
`profile_ops.profile_publish_manifest`. **Why:** a rebuild that fails halfway
would otherwise leave production half-old and half-new, and nobody could tell
which. With staged publish, a failed build leaves production untouched on the
last good release. `resume_publish` exists to finish a promotion that died
partway.

**Concurrency is handled with leases.** `profile_ops.profile_dataset_leases`
holds a lock with a heartbeat, so two builds cannot promote at once.

### Why a field might look stale

The useful question is which phase writes it, and which modes run that phase.
`populate_newsletter_forum` runs only in `rebuild`, so forum settings do not
refresh nightly. `enrich_source_created_at` runs in `rebuild`, `refresh` and
`reenrich`, so acquisition dates do. The data dictionary lists the writers for
every table.

---

## 9. The gates: what stops a bad build reaching production

**15 assertions run after every build** -- after the last SQL step, before
anything is promoted. They are the reason a broken build does not silently
become the thing everyone queries.

**Why a gate rather than a step:** a step can only check its own work. These run
over the finished database, so they catch damage no single step could see -- a
satellite orphaned by one step and its parent removed by another, or a refresh
that blanked values three steps apart. They come in two severities.

**Hard failures are a build acceptance gate.** The build is marked `failed_gate`,
the pipeline raises, and **production views are not published**. Candidate views
may exist in `profile_staging`, but consumers keep reading the last good release.
The hard checks are the ones that would corrupt meaning rather than merely look
odd:

| Assertion | Catches |
|---|---|
| `profile_core_unique_bn_id` | duplicate people |
| `profile_identifiers_unique_primary` | two primary identifiers of one type |
| `profile_current_unique_bn_id` | a view that fans out rows |
| `orphan_satellite` | engagement or preference rows with no parent profile |
| `missing_parent` | the reverse -- a profile whose satellites vanished |
| `restore_coverage` | app-written fields not restored after a rebuild |
| `refresh_safety_check` | a refresh that silently blanked existing values |

**Soft warnings log and continue.** Performance regressions, fill-rate drift,
lineage gaps -- things worth knowing that do not make the data wrong.

### Reading the result

Every build writes its outcome to `profile_ops.profile_build_runs.assertion_summary`:

```
{"passed": 15, "total": 15, "hard_failures": [], "soft_warnings": []}
```

A status of `completed_with_warnings` means soft warnings only -- the build is
good. `failed_gate` means a hard assertion fired and nothing was published.

Measured over the last 20 builds: **zero hard failures**, one historical
exception on 2026-08-07 (`fill_rate_drift_critical`) which has since cleared. The
current soft warnings are `build_duration_regression` and
`step_performance_regression` -- timing, not correctness.

To check the latest build yourself:

```sql
SELECT build_id, mode, status, assertion_summary
FROM profile_ops.profile_build_runs
ORDER BY started_at DESC
LIMIT 5
```

---

## 10. Things that will bite you

1. **Never present tier1 and tier2 together as "people".** 793,752 is the
   defensible number; 6.12M is browsers plus people.
2. **Role flags do not sum to the profile count** and NULL is not FALSE.
3. **`created_at` is not a signup date.** Use `source_created_at` with a
   registration-grade `source_created_basis`, or just use `profile_growth_known`.
4. **`email_open_count` is ~84% bots.** Use `human_email_open_count`, and treat
   its NULLs as "unknown", not "zero".
5. **Filter conditions on `preferred_condition_normalized`**, not on
   `preferred_condition.label`.
6. **`profile_current_safe` is the default surface.** `profile_current` exposes
   sensitive fields (`age_exact`, `ethnicity`, `veteran`) and is for approved use
   only.
7. **`hcp_status` and `is_hcp` are different columns.** `is_hcp` with its lineage
   triple is the v6.5 canonical surface.
8. **AIM inflates engagement, not HCP counts.** `fill_gaps_aim_attribution.sql`
   adds AIM page views into `total_pageviews` and moves `first_seen_web` /
   `last_seen_web`. It never sets `is_hcp`. Zero profiles are HCP solely because
   of AIM.
