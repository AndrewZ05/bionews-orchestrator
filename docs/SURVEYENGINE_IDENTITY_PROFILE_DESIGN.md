# SurveyEngine -> Identity Spine + Profile Database

**Status:** design note, pending confirmation. Not built.
**Date:** 2026-08-18
**Goal:** capture new registrants as they arrive, by getting SurveyEngine's core
columns into the identity spine and `profile_core`.

Survey *answer* storage (LimeSurvey-wide, one row per answer) is explicitly out
of scope for now and is expected to follow later.

---

## 1. Where things stand

SurveyEngine is Bionews' in-house Laravel forms engine
([configs/surveyengine.yaml](../configs/surveyengine.yaml)). It is becoming the
primary registration path -- one of its two live forms is titled "Signup".

| | State |
|---|---|
| Extraction to BigQuery | Running daily, COMPLETED. Lands in `surveyengine_data`. 18 tables declared; 13 materialise (5 are empty at source), including `se_users` added 2026-08-18. |
| Identity hub | Connector committed (`SURVEYENGINE_DIRECT`) but **on hold pending review**. See section 6. |
| Profile database | **Not wired.** Zero references in any profile SQL or config. |
| Documentation | This note is the first. |

The database is **not yet in full production**, so there is little real data:
213 rows across the four email-bearing tables, all 178 email values on the
`@bionews.com` domain across 6 internal accounts, since 2026-07-07.

Design against the shape, not the volume. Wiring it now, before it becomes the
primary registration path, is far cheaper than backfilling identity afterwards.

### Email coverage vs bnfpvid (2026-08-18)

| Table | Rows | Has email | Has bnfpvid | Email only |
|---|---:|---:|---:|---:|
| `form_impressions` | 81 | 81 | 0 | 81 |
| `question_responses` | 76 | 47 | 24 | 24 |
| `question_impressions` | 35 | 35 | 0 | 35 |
| `submissions` | 21 | 15 | 9 | 7 |
| **Total** | **213** | **178** | **33** | **147** |

84% of rows carry email; 15% carry `bnfpvid`. Any design that keys on `bnfpvid`
sees a sixth of the data. Keying on email sees nearly all of it.

**The 15% is historical, not structural.** `bnfpvid` was instrumented partway
through the pilot and every row since is populated:

| Week of | Rows | With `bnfpvid` | |
|---|---:|---:|---:|
| 2026-06-07 .. 2026-07-12 | 37 | 0 | 0% |
| 2026-07-19 | 28 | 1 | 4% |
| 2026-08-02 | 30 | 30 | **100%** |
| 2026-08-09 | 2 | 2 | **100%** |

So `email <-> bnfpvid` edge yield on *future* rows should approach 100%, and the
low edge count today reflects pre-instrumentation history rather than a ceiling.
Do not judge the connector's value on the current 2 edges.

Caveat: `form_impressions` and `question_impressions` have **no `bnfpvid` column
at all** -- it was never added to those two tables, so their 0% is structural,
not historical. If impressions are meant to carry it, that is an app-side gap.

---

## 2. Identity spine: which columns

**Email is the primary key of the SurveyEngine database** (confirmed
2026-08-18). It appears in `users`, `form_impressions`, `question_impressions`,
`submissions` and `question_responses`. That makes email the person key, and it
settles the spine design:

- email is the anchor; `bnfpvid` is the bridge from the email world to the web
  world. (An earlier draft framed `bnfpvid` as the anchor. Same edge, wrong
  emphasis.)
- SurveyEngine has **no separate per-user id** -- email *is* the id. So the only
  cross-identifier edge it can contribute is `email <-> bnfpvid`, which is what
  the connector emits. There is nothing further to add to the spine.
- An edge needs two identifiers. Rows carrying email but no `bnfpvid` (147 of
  213, see below) form no edge and are not meant to. They matter for the
  **profile database**, not the spine: those emails either resolve to an
  existing `bn_id` through the existing `EMAIL_EXACT` rule, or they are a new
  person and the profile row is the thing that should be created.

The spine stores identifiers and the edges between them. Nothing else. Attributes
belong in the profile database (section 3).

| Column | Table | Use |
|---|---|---|
| `email` | `se_submissions` | edge side A, type `email` (the PK) |
| `respondent_email` | `se_question_responses` | same, unioned in |
| `bnfpvid` | both | edge side B, type `bnfpvid` |
| `created_at` | both | `first_seen` / `last_seen` |
| `deleted_at` | both | filter only; never emitted |

`form_impressions` and `question_impressions` also carry email but never carry
`bnfpvid`, so they can produce no edge and are correctly absent from the spine
connector. They are still useful to the profile database as engagement signal.

`bnfpvid` is priority 0 in `source_priorities` -- the highest-ranked anchor in
the graph. The edge is **deterministic at confidence 1.0** because email and
`bnfpvid` are written on the same row: the link is directly observed, with no IP
or time correlation and no inference step.

### Deliberately excluded

| Column | Why |
|---|---|
| `pvid` | Page-level. 21 distinct values across 21 submissions. Emitting it as a person identifier would fuse everyone who shares a pageview into one cluster. |
| `guid` | Declared on four tables, **0% populated on all of them**. Selecting it would make the bridge look wired while emitting nothing. Origin unknown. |
| `se_forms.user_id` | The form author (`=1` for both forms), not a respondent. |

### Scope: email and bnfpvid, deliberately

**Confirmed 2026-08-19: email and `bnfpvid` are the primary keys SurveyEngine
contributes to the identity hub. `bionews_uk` is not expected from SurveyEngine
and its absence is a scope decision, not a gap.**

An earlier draft of this note recorded the missing SSO key as a defect to be
fixed app-side. That was wrong and has been retracted. The SSO key reaches the
graph through the acceptor's `bionews_uk` cookie capture
(`LOCALSTORAGE_COOCCURRENCE`), which is a separate path and already wired.

This makes the committed connector complete rather than provisional: email and
`bnfpvid` land on the same SurveyEngine row, so the edge between them is directly
observed, and there is no third identifier waiting to be added.

---

## 3. Profile database: proposed column mapping

The SurveyEngine `field_key` values line up with `profile_core` unusually well.

| SurveyEngine `field_key` | `profile_core` target | Notes |
|---|---|---|
| `persona_type` | the 5 role flags + lineage triples | mapping below |
| `content_preferences` | `content_preferences` | exact match; already `ARRAY<STRING>`, values arrive as JSON arrays |
| `first_name` / `last_name` | `first_name` / `last_name` | self-reported; ranks below NPI in the existing precedence |
| `email` | `email` | already the identity anchor |
| `send_emails` | `communication_opt_in` | |
| `birth_year` | `age_band` | derive the band; do not store the raw year |
| `diagnosis_year` | `diagnosis_timing_band` | derive `<6mo` / `6-24mo` / `>24mo` |
| `has_taken_<condition>_medication` | `treatments_current` | tri-state `yes` / `no` / `not_sure`, not a boolean |
| `treatment_satisfaction` | **no column exists** | open decision, section 5 |

Submission-level fields:

| Column | `profile_core` target | Notes |
|---|---|---|
| `created_at` | `source_created_at` | **registration grade** -- a real signup event, which is exactly what v6.7 wants for growth reporting |
| `source_domain` | `site_domain` | NULL on 12 of 21 rows; not reliable alone |
| condition (via `se_conditions`) | `preferred_condition` | see below |

### Condition

`conditions` is a key field. `se_conditions` is loading (4 rows: Myasthenia
Gravis Forums, Myasthenia Gravis Development, Myasthenia Gravis Forums Devel,
Multiple Sclerosis Staging).

Prefer the `se_conditions` link over deriving condition from `source_domain`,
because `source_domain` is NULL on 12 of 21 submissions. Whatever the source,
resolve through `conditions_dict` and write
`preferred_condition_normalized` -- the v6.6 column exists precisely so that
label variants (`ms` vs `Multiple Sclerosis`) do not split audiences.

### persona_type -> role flags

Observed values map cleanly onto the v6.5 flags:

| `persona_type` value | Flag |
|---|---|
| `person_living_with_<condition>` | `is_patient` |
| `caregiver_<condition>` | `is_caregiver` |
| `family_member_<condition>`, `family_member` | `is_family_or_friend` |
| `healthcare_provider` | `is_hcp` |
| `other` | `is_other` |
| `test` | **junk -- must be dropped** |

Note the values are condition-suffixed, so the mapping must match on prefix
rather than on an exact enum, or every new condition silently falls through to
unmapped. Self-reported answers warrant source `'surveyengine'` at confidence
1.0, consistent with how `app_confirmed` is treated today.

---

## 4. Blockers

### 4a. `users` -- extracted 2026-08-18 (resolved)

**Email is the primary key** (confirmed 2026-08-18), which supersedes an earlier
working assumption that `users.id` was the key.

`users` was not declared in `configs/surveyengine.yaml` and not present in
BigQuery. It is now extracted as `se_users` (commit `9d02564`) and will appear in
`surveyengine_data` after the next SurveyEngine run.

`password` and `remember_token` are deliberately **not** extracted: a credential
hash and a live session token, no analytical use, and a standing liability in a
warehouse many people can read. `password_reset_tokens`, `personal_access_tokens`
and `sessions` are left undeclared for the same reason.

**Still open.** As of 2026-08-18 all 12 rows are internal (11 `@bionews.com`, 1
vendor domain), none have `email_verified_at` set, and only 3 of the 12 appear in
`submissions`. They look like staff/admin logins rather than reader
registrations, which is consistent with `users` being Laravel's app-login table
and with `se_forms.user_id = 1` pointing at a form author.

Decide whether reader registration actually writes rows here before treating
"has a `users` row" as the definition of a registrant.

### 4b. Empty structure tables (resolved -- not a defect)

An earlier draft recorded 5 declared tables as "not loading". That was wrong.
Checked against the source Postgres 2026-08-18: `answer_sets`,
`answer_option_answer_set`, `form_segment`, `question_segment` and
`question_answer_rules` all hold **0 rows at source**. Nothing is broken; there
is simply nothing to load. They will appear once the app starts using them,
which is also when the LimeSurvey-wide answer build-out will need them.

### 4b2. Tables in source but never declared

The source database holds 39 tables; the config declares 18. Most of the
remainder are Laravel framework internals (`migrations`, `jobs`, `cache`,
`sessions`) and are correctly ignored. Three carry real data and may be worth a
decision later:

| Table | Rows | Why it might matter |
|---|---:|---|
| `activity_log` | 127 | audit trail of admin actions |
| `wordpress_sites` | 74 | which sites the engine is deployed on |
| `wordpress_site_jobs` / `wordpress_operation_logs` | 362 / 474 | deployment + operations history |

**Never extract:** `password_reset_tokens`, `personal_access_tokens`,
`sessions`, and the `password` / `remember_token` columns of `users`. These are
credentials and live session tokens with no analytical use.

### 4c. Data validation is required before any load

A primary-key filter will not catch these. Roughly a third of current rows are
test or invalid:

| Problem | Observed |
|---|---|
| `persona_type = 'test'` | 3 responses |
| `content_preferences = ["test"]` | 1 response |
| `birth_year = 1901` | 1 response (125 years old) |
| `birth_year` = 2023, 2019, 2006 | implausible for a diagnosed adult |
| `diagnosis_year = 0` | 2 responses |
| `source_domain` NULL | 12 of 21 submissions |

Without validation up front, this junk becomes `is_hcp = TRUE` rows and a
125-year-old patient in `profile_core`. Given how much damage NULL-overwrite and
bad-seed issues have already caused in this database, validation should be built
before the mapping, not after.

### 4d. Soft deletes

19 of 21 submissions carry a `deleted_at` timestamp, in batches that look like
pilot test-data cleanup. The committed identity connector honours soft deletes,
which drops 31 candidate pairs to 9. The reasoning: a deletion may be a privacy
request, and an identity edge outlives the row that created it. If these are
purely test cleanups, that policy is worth revisiting -- but it should be an
explicit decision, not a default.

---

## 5. Open decisions

1. **`treatment_satisfaction`** has no `profile_core` column. Add one, or leave
   it in survey storage only until the LimeSurvey-wide build?
2. **Should a SurveyEngine submission create a profile, or only enrich an
   existing one?** It is the registration path, so create is the expected
   answer -- but it changes who lands in `profile_core`.
3. **Junk gate:** is `users.id` presence the filter, or is per-field validation
   (4c) also required? Current data suggests both.
4. **Soft-delete policy** (4d).

---

## 6. Status of the committed identity connector

`SURVEYENGINE_DIRECT` was committed in `35ebfb1` / `8feb643` and is currently
`enabled: true` and wired into the run sequence, so an identity hub run will emit
its edges. Work was then put on hold pending the confirmations above.

Options, pending direction:

- set `enabled: false` -- keeps the code, emits nothing, one line to reverse
- revert both commits
- leave enabled (currently 2 edges, both from internal test accounts)

Invariants are locked by `tests/unit/test_surveyengine_identity_bridge.py`:
`pvid` is never emitted, `guid` is not relied on, soft deletes are honoured, the
connector is actually registered in the run sequence, and the edge stays
deterministic.
