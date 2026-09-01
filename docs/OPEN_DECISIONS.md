# Open Decisions

**As of 2026-08-19. Groups A-F are all closed.**

Kept as the record of what was decided and why, so settled questions are not
re-opened. Every group below is resolved; the only remaining items are listed
here.

## Still outstanding

| # | Item | Owner |
|---|---|---|
| 1 | **Deploy** -- the SurveyEngine enrichment, `profile_metrics`, the `guid` wiring and the fingerprint retirements are committed but not on the prod VM. Until it pulls, the nightly refresh will not run `enrich_surveyengine` and will keep emitting the retired edges. | you |
| 2 | Ask the app team to flag when their identity service goes live, so the `guid` edges are confirmed rather than discovered. | you |
| 3 | When a real audience dashboard is built: add *Known People* alongside Sessions and Users, and check nothing reports on raw `email_open_count`. See Group F. | you |

Nothing is blocked, and nothing needs building.

---

## Group A -- RESOLVED 2026-08-18

### A1. SurveyEngine identity connector -- DECISION: leave enabled

`SURVEYENGINE_DIRECT` stays `enabled: true`. No code change. The next identity
hub run will emit its edges: ~2 today (both internal test accounts), rising
toward 100% of new SurveyEngine rows, since `bnfpvid` has been 100% populated
since 2026-08-02.

### A2. Forum activity backfill -- DONE

`sql/backfill_forum_activity.sql` executed against production 2026-08-18. MERGE
affected 34,031 rows. Verified idempotent by running it twice; the second run
changed nothing.

| `profile_preferences` | Before | After |
|---|---:|---:|
| rows | 5,450 | **38,509** |
| `forum_registered_at` | 769 | **34,072** |
| `last_forum_activity` | 56 | **22,600** |
| `subscribed_group_ids` | 0 | **5,470** |
| `newsletter_preferences` | 4,495 | 4,495 (unchanged) |

**Active Member is now computable:** 1,692 in 90 days, 786 in 30 days, against
34,072 known members. It previously returned 4.

Note the source fix in `populate_newsletter_forum.sql` only takes effect on the
next `rebuild`, since that is the only mode the step runs in. Until then this
backfill is what keeps the data correct, and it is safe to re-run at any time.

## Group B -- RESOLVED 2026-08-18

### B1. "Active Member" -- DECISION: store both. DONE.

`forum_settings` now carries two clocks:

| Column | Meaning | Populated | 90d | 30d |
|---|---|---:|---:|---:|
| `last_forum_activity` | deliberate acts (post, reply, comment, update) -- **the Active Member metric** | 22,600 | **1,692** | 781 |
| `last_forum_presence` | any activity row, incl. login heartbeats and registration | 34,030 | 5,867 | 2,885 |

`last_forum_presence >= last_forum_activity` always; verified 0 violations in
production. Both writers populate both columns
(`populate_newsletter_forum.sql`, `backfill_forum_activity.sql`), and the DDL
struct documents the distinction.

### B2. Window -- DECISION: 90 days is the headline

Active Member = **1,692**. Active Email = **193,326**. 30-day figures stay
available as context (781 and lower respectively) but are not the headline.

### B3. SurveyEngine submission -- DECISION: create a profile

A submission with a valid email creates a `profile_core` row, with
`source_created_at` at registration grade. Not yet built -- this is the spec for
the SurveyEngine loader, which is gated on Group C.

### B4. Registrant definition -- DECISION: submissions define it, not `users`

`se_users` is extracted and useful as an account record, but having a row there
does not make someone a registrant. Evidence: all 12 rows are internal, none have
`email_verified_at` set, only 3 of 12 appear in `submissions`. They are Laravel
app logins.

## Group C -- RESOLVED 2026-08-18 (spec for the SurveyEngine loader)

These are the rules the loader must implement. **The loader is not built yet**;
this is its specification, agreed before writing it.

### C1. Soft deletes -- honour them, and record that you did

Skip rows carrying `deleted_at` for identity and profile purposes, because a
deletion may be an erasure request and an identity edge outlives the row that
created it. **Additionally write an audit row** recording that a deletion was
skipped, so a later "why did the count drop" question is answerable without
re-deriving it.

Currently costs ~2/3 of pilot volume (19 of 21 submissions), all internal test
data.

### C2. `treatment_satisfaction` -- survey storage only

Do not add a `profile_core` column. Leave it in survey storage until the
LimeSurvey-wide answer build, which is where per-answer data belongs. Avoids a
sparse column that only one form populates.

### C3. Validation -- all four rules enforced

The loader must reject or skip, before anything reaches identity or profile:

| Rule | Rejects |
|---|---|
| Drop test/junk sentinels | `persona_type='test'`, `content_preferences=['test']` |
| Range-check years | `birth_year` outside 1900-current; `diagnosis_year` = 0 or future |
| Require a resolvable condition | if `source_domain` is NULL and no `se_conditions` link, load the row but leave condition **unset** rather than guessing (12 of 21 today) |
| Require valid email | email is the primary key; format-check before use |

Note the third rule is "load, do not guess" -- not "reject the row".

---

## Group D -- RESOLVED 2026-08-18

### D1. `DEVICE_STAT_ID` -- RESOLVED 2026-08-19: disabled and removed

Connector set to `enabled: false`; the 2,365,271 existing edges deleted via
[../sql/identity_hub_remove_device_stat_id.sql](../sql/identity_hub_remove_device_stat_id.sql).

A targeted delete rather than a full rebuild, because `bn_id_hub` is only
rewritten wholesale by `--refresh full`; the default incremental run appends, so
disabling alone would have left the edges in place indefinitely. A full rebuild
would also have cleared them but would have re-derived every cluster from months
of accumulated data, producing unrelated merges and splits in the same window.

**No identity was affected.** Verified before and after:

| | Before | After |
|---|---:|---:|
| distinct `bn_id` in xref | 6,161,323 | 6,161,323 |
| `bn_id_xref` rows | 29,538,036 | 29,538,036 |
| `bn_id_neighbors` bn_ids | 6,058,220 | 6,058,220 |
| `bn_id_persistence` rows | 101,416 | 101,416 |
| `profile_core` rows | 6,154,343 | 6,154,343 |
| `bn_id_hub` rows | 64,744,278 | 62,379,007 |

Set-level comparison of every `bn_id`: **0 lost, 0 gained.** Counts alone could
hide an equal-sized swap, so the full set was captured beforehand and diffed.

Reversible: set `enabled: true` and the next run recomputes every edge from
`BN_Acceptor.acceptor_events`. Nothing was destroyed -- the edges are derived.

Note `stat_id` has no separate switch: `connect_device_stat_id` is its only
producer, so this turns the identifier type off entirely.

## Phase 0 -- DONE 2026-08-19

BI gets surfaces where the wrong answer is hard, rather than bespoke views per
question.

**`profile_data.profile_metrics`** -- one row per `bn_id`, every headline metric
as a boolean, with dimensions to slice by (condition, site, country, role,
acquisition month, tier). Consumers `GROUP BY`; they do not redefine metrics.

| Metric | Now |
|---|---:|
| `is_known_person` | 794,115 |
| `is_verified_hcp` | 387,021 |
| `is_mailable` | 412,707 |
| `is_active_email_90d` | 193,051 |
| `is_known_member` | 35,236 |
| `is_active_member_90d` | 1,688 |
| `is_logged_in_90d` | 5,854 |
| `has_sso_key` | 1,712 |

Seven logical invariants verified in production, all zero violations:
active_member implies logged_in, 30d implies 90d for both clocks, active_email
implies mailable, verified_hcp implies is_hcp, the registration-date flag
excludes cookie dates, and one row per `bn_id`.

**Also fixed a real gap:** `human_email_open_count` was not exposed on
`profile_current_safe` at all, so an analyst using the *governed* surface still
could not compute email engagement correctly -- they had to join
`profile_engagement` directly, which is exactly where the ~6x bot-inflation trap
lives. The safe view now exposes the bot-filtered count (and deliberately not the
raw one).

Registered in the manifest, documented in the generated dictionary, linked from
the explainer and cookbook, and locked by seven static tests.

---

## Group E -- CLOSED 2026-08-19. Nothing outstanding with the app team.

All three items resolved. Recorded in full so none is re-opened.
See [SURVEYENGINE_APP_REQUESTS.md](SURVEYENGINE_APP_REQUESTS.md).

### E1. `bionews_uk` from SurveyEngine -- not a gap

email and `bnfpvid` are the primary keys SurveyEngine contributes, by design. I
had escalated the missing SSO key to "a registration product that does not record
the registration identity"; that was wrong and is withdrawn. The SSO key reaches
the graph through the acceptor's cookie capture, already wired as a first-class
anchor (priority 1, permanent, 1,811 people).

### E2. `bnfpvid` on the impressions tables -- withdrawn twice, closed

Asked for on two different rationales, both wrong:

1. *"an impression cannot be tied to a person"* -- no: email is on 100% of
   impression rows and resolves directly through the graph.
2. *"anonymous impressions would be invisible"* -- moot: SurveyEngine cannot
   record an impression anonymously by design.

And the question underneath both -- how many saw a form without completing it --
is answerable from GA4, which sees the page regardless of identification. Over 90
days on the personalization survey: GA4 2,306 views from 185 visitors, against
119 SurveyEngine impressions from 6 people and 21 submissions. **The funnel is
GA4 for the top, SurveyEngine for the middle and bottom.** No app change needed.

### E3. `guid` -- answered, and wired to self-activate

Not dead. The app team confirmed it is the intended unique user identifier from
an identity service that is not live yet, which is why it is 0% populated and why
email carries identity meanwhile.

Wired on our side rather than left waiting: `surveyengine_guid` is registered as a
person-level anchor (priority 1, permanent, fanout 10), and the connector's
`email <-> guid` query filters on a non-empty guid, so it emits nothing today and
starts producing deterministic edges on the first run after the column populates.
No switch to remember -- the same pattern `bnfpvid` followed.

**Only outstanding courtesy:** ask them to tell us when the identity service goes
live, so the edges are confirmed rather than discovered.

---

## Group F -- BI / dashboard

### F1. Session count discrepancy -- WITHDRAWN 2026-08-19, not an issue

Recorded here as a blocking discrepancy between a dashboard reporting 14,294,152
sessions for Jan-Jun 2026 and the warehouse reporting 8,608,676.

**The dashboard was a mockup.** Those figures were placeholder data, not query
output, so there was never anything to reconcile. Withdrawn.

The warehouse figures gathered while chasing it are still sound and worth
keeping, since F2 uses them:

| Jan-Jun 2026, `df_warehouse_output` | |
|---|---:|
| sessions (`output_ga4_sessions`) | 8,608,676 |
| sessions independently via `output_ga4_events` | 8,608,177 |
| distinct GA4 "users" (browsers) | 6,219,015 |
| sessions per browser | 1.38 |
| distinct sites covered | 98 |

Nothing gates adding a People metric to a real dashboard. When one exists,
confirm what it queries before attaching `profile_metrics` to it -- but that is
ordinary diligence, not an open issue.

### F2. Add *Known People* as a third metric

**Keep the "Users" label.** An earlier version of this note recommended renaming
it to "Devices". That was wrong and is withdrawn: "Users" is GA4's own metric
name, it is precise and well understood in analytics, and renaming it would make
the dashboard non-standard and break reconciliation against the GA4 UI.

The concern behind that recommendation was real but misdiagnosed. GA4 Users is
accurate *as GA4 Users*; the risk is someone reading it as "distinct humans" and
computing a ratio against it. A rename does not fix that. A third metric does:

| Metric | Definition | Now |
|---|---|---:|
| Sessions | visits | 8,608,676 |
| Users | GA4 users, browser-scoped | 6,219,015 |
| **Known People** | identity-resolved, has a durable anchor | **794,115** |

With all three on the page the distinction is self-evident, and Known People
being roughly 8x smaller than Users is the point -- it is the addressable
audience, and the number that should climb as SSO and SurveyEngine registration
roll out. Removing Users would remove the comparison that gives Known People its
meaning.

`profile_metrics.is_known_person` supplies it directly; it is a `COUNTIF`.

Worth adding a one-line definition under each tile ("GA4 users, browser-scoped" /
"resolved individuals") so the difference lives on the page rather than in
someone's head.

**Why Users and Known People diverge -- corrected 2026-08-19.** An earlier
version of this note blamed cookie churn. That is wrong; the data says otherwise.

The GA4 cookie is persistent, and `bnfpvid` is already stitching what does churn:
5,267,617 people carry both a `client_id` and a `bnfpvid`, and of the 287,872
people holding more than one `client_id` (up to 49), 285,741 -- 99.3% -- also
have a `bnfpvid` that tied them together. Identity resolution is working.

The real reason is the session distribution over Jan-Jun 2026:

| Sessions in 6 months | Users | Sessions |
|---|---:|---:|
| **1 session** | **5,465,885** | 5,465,885 |
| 2 | 442,154 | 884,308 |
| 3-5 | 211,559 | 755,091 |
| 6-10 | 58,244 | 431,364 |
| 11+ | 41,173 | 1,072,028 |

**88% of GA4 users visited exactly once** -- one-time organic search traffic,
which is what condition-specific news that ranks well attracts. They have nothing
to anchor on: no email, no login, one pageview. The 1.38 average is dragged down
by that tail, not by identity fragmentation. The 41,173 people with 11+ sessions
generated 1.07M sessions between them; that is the returning audience.

So the gap between Users and Known People is **not** a resolution failure. It is
the share of traffic that never identified itself -- which makes Known People the
conversion opportunity, and exactly what SurveyEngine registration exists to
close.

### F3. Anything reporting on `email_open_count` is ~6x too high

Raw opens are ~84% Apple MPP and scanner traffic. Consumers should move to
`human_email_open_count`.

---

## Already handled -- no decision needed

| Item | State |
|---|---|
| Stale "~50% of opens are bots" claim | Corrected to the measured 84% in all four SQL files that carried it |
| v6.7 columns dropped by rebuild | Declared in DDL + runtime healer + snapshot self-heal |
| Snapshot INSERT missing 6 columns | Fixed; INSERT, SELECT and DDL now agree at 133 |
| `resume_rebuild` would fail on snapshot | Self-healing ALTERs added, test locks it |
| Forum pipeline SQL | Fixed in `populate_newsletter_forum.sql` (takes effect on next rebuild); backfill covers the interim -- see A2 |
| `users` not extracted | Added, `password`/`remember_token` deliberately excluded |
| Identity + profile documentation | Generated dictionaries, explainers, validated cookbooks; CI fails if the generated ones drift |

## Not mine, flagged

`02a62ff` on `main` changes `scripts/profile_health_snapshot.py` and was not
authored by this session. Worth reviewing independently before pulling.
