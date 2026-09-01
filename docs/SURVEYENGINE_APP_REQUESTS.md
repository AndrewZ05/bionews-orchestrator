# SurveyEngine: nothing outstanding

**For:** whoever owns the SurveyEngine Laravel app
**From:** data platform
**Date:** 2026-08-19

Nothing is being asked for. This records what was settled and what we have
prepared, so the next person does not re-open it.

---

## Scope, so nobody re-raises it

**email and `bnfpvid` are the primary keys SurveyEngine contributes to the
identity hub. That is by design and it is sufficient.**

`bionews_uk` (the SSO User Key) is **not** expected from SurveyEngine. An earlier
version of this document asked for it; that request has been withdrawn. The SSO
key reaches the identity graph through the acceptor's cookie capture, which is a
separate and already-working path.

The two keys SurveyEngine does provide land on the same row, which makes the
`email <-> bnfpvid` link directly observed rather than inferred -- the strongest
kind of identity edge we can get from a form. Nothing further is needed.

---

## No longer asking: `bnfpvid` on the impressions tables -- CLOSED

Two earlier versions of this document asked for `bnfpvid` on
`form_impressions` / `question_impressions`. Both are withdrawn. Recorded here so
it is not raised a third time.

**First ask, withdrawn:** "an impression cannot be tied to a person". Wrong --
email is on 100% of impression rows (84/84 and 35/35) and resolves to a person
directly through the identity graph. `bnfpvid` adds nothing for identity.

**Second ask, withdrawn:** "anonymous impressions would be invisible". Also moot
-- **SurveyEngine cannot record an impression anonymously by design** (confirmed
2026-08-19), so there is no anonymous impression to capture.

**And the underlying question is already answered elsewhere.** "How many people
saw this form and did not complete it" is measurable from GA4, which sees the
form page whether or not anyone identifies themselves. Measured over the last 90
days on the personalization survey:

| Source | What it measures | Volume |
|---|---|---|
| GA4 `page_view` on the form URL | saw the form, identified or not | 2,306 views, 185 visitors |
| `se_form_impressions` | form rendered for a known person | 119 rows, 6 people |
| `se_submissions` | completed | 21 rows |

So the registration funnel is: GA4 for the top, SurveyEngine for the middle and
bottom. Nothing needs to change in the app.

## `guid` -- ANSWERED 2026-08-19, nothing needed from you

The app team confirmed `guid` is the intended unique user identifier, coming from
an identity service that is not live yet. That is why it is 0% populated on all
four tables, and it explains why email carries identity on impressions,
submissions and question_responses in the meantime.

**No change requested.** We have wired `guid` on our side so it activates by
itself:

- `surveyengine_guid` is registered in the identity graph as a person-level
  anchor (priority 1, alongside email and the SSO key), permanent -- it never
  decays -- with the same fanout threshold as email.
- The SurveyEngine connector already contains the `email <-> guid` query. It
  filters on a non-empty `guid`, so today it returns nothing and emits nothing.
  The first run after the identity service populates the column will start
  producing deterministic edges with no deploy, no config change and nobody
  needing to remember.

This is the same pattern `bnfpvid` followed: 0% until 2026-08-02, 100% since,
with no intervention on either side.

**One thing that would help:** tell us when the identity service goes live, so we
can confirm the edges appear rather than discover it by accident.

## What we are not asking for

- **`bionews_uk`** -- out of scope, see above.
- **`pvid`** -- no change. We understand it is page-level and we do not use it as
  a person identifier.
- **Soft deletes** -- no change. We honour `deleted_at` deliberately, on the basis
  that a deletion may be an erasure request.
- **New tables.**

---

## Context

[SURVEYENGINE_IDENTITY_PROFILE_DESIGN.md](SURVEYENGINE_IDENTITY_PROFILE_DESIGN.md)
documents the full column mapping from SurveyEngine into the identity spine and
profile database, including which columns are deliberately excluded and why.
