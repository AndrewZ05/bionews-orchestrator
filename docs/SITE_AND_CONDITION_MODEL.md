# Site and condition coverage — how it works and how to verify it

Reference for `profile_core.site_domain`, `site_domains`, the parent/child site
model, and `preferred_condition`. Written to be checkable: every number below
comes from a query in this document, and the verification suite at the end
should return zeros.

---

## 1. Where we are

| Metric | Coverage |
|---|---|
| **Site** (`site_domain`) | **95.3%** — 7,397,971 of 7,760,409 |
| **Condition** (`preferred_condition`) | **95.4%** — 7,399,174 of 7,760,409 |

Site coverage was **5.55%** before this work. The rest of this document explains
how it got here and how to confirm the numbers are sound.

---

## 2. Why site coverage was 5.55%

The cause was structural, not a data gap. Only two things ever wrote
`site_domain`, and both derived it from Mailchimp list membership. So the column
did not mean *"which site is this person on"* — it meant *"which Mailchimp list
did they join."* Anyone who browsed for years without subscribing got nothing.

The evidence is symmetrical:

- 386,642 of the 422,581 profiles that had a site (91.5%) had a Mailchimp edge
- **6,834,649** profiles with **no** site had an acceptor edge and **no**
  Mailchimp edge — they were on our sites, the tracker recorded which site on
  every pageview, and nothing read it
- Only **1,619** profiles with no site had no identity-hub edge at all

The data was never missing. It was never read.

---

## 3. How `site_domain` is populated

`fill_gaps_site_domain.sql` reads six signals in confidence order. Each tier is
forward-only — it fills a NULL and never overwrites a stronger source.

| # | Tier | Confidence | Filled | What it is |
|---|---|---|---|---|
| 1 | `wp_user_id` | 0.95 | 976 | The site is inside the identifier: `bnsmaprd:3790` |
| 2 | `mailchimp_list` | 0.90 | — | Deliberate subscription to that site's list |
| 3 | `acceptor` | 0.80 | **6,433,843** | Our own on-site tracker |
| 3B | `acceptor_url` | 0.75 | 63,486 | URL fallback when the tracker omits the site |
| 4 | `aim_referrer` | 0.70 | 3,455 | AIM clickstream referrer URL |
| 5 | `site_events` | 0.60 | 388,817 | GA4-derived pageviews |
| 6 | `content_affinity` | 0.50 | 3,757 | What they read (weakest) |

`mailchimp_list` shows no rows because `fill_gaps_condition_site` runs earlier
and fills those people first. Both paths work; the earlier one wins.

**The safety rule:** every tier validates against `site_registry`. A code that is
not a registered site produces **no fill** rather than a guess. `site_domain`
gates access, so a wrong tenant is worse than a missing one.

---

## 4. How `preferred_condition` is populated

Same ladder pattern, four sources:

| Source | Confidence | Filled |
|---|---|---|
| `mailchimp_list` | 0.87 avg | 410,116 |
| `site_registration` | 0.69 avg | **4,163,732** |
| `mailchimp_tag` | 0.70 | 719 |
| `mailchimp_list_cleaned` | 0.60 | 1,211 |
| `content_affinity` | 0.50 | 2,741,126 |

### Condition is not a role — and the distinction is load-bearing

A **condition** says what a person is *oriented around*. A **role** says *who
they are* in relation to it. They are stored separately and inferred by
different rules, because conflating them produces confidently wrong audiences.

| | Column | Says | How it is set |
|---|---|---|---|
| Condition | `preferred_condition` | Which disease this person engages with | Inferred from behaviour — the site they registered on, the list they joined, what they read |
| Role | `is_patient`, `is_caregiver`, `is_family_or_friend`, `is_hcp`, `is_other` | Whether they have it, care for someone who does, or treat it | Only from what a person explicitly told us about themselves |

Someone reading the MS site is oriented around MS. That is all it means. A
condition site's audience contains patients, the spouse managing appointments,
an adult child researching for a parent, and clinicians following the science —
all reading the same articles, all indistinguishable from browsing alone.

So **no condition-inference step ever writes a role flag**, and none ever will:
the five role flags come only from a person's own self-description in a
registration form, survey, or profile. That is why a condition can be inferred at
confidence 0.50 from what someone read, while a role cannot be inferred from
behaviour at any confidence.

**What this means in practice.** `preferred_condition = 'Multiple Sclerosis'`
means "engages with MS content", not "has MS". Building a patient audience
requires filtering on `is_patient` as well — a condition filter alone will
include caregivers and clinicians, and a campaign written for patients will land
badly on both.

### The 361,235 profiles with no condition

Not a data gap — two populations that genuinely have none.

| Group | Count | Share |
|---|---|---|
| HCP | 286,549 | 79% |
| Non-HCP | 74,686 | 21% |

**The doctors (286,549).** Bulk-loaded from the public NPI registry — 283,000 of
them via `npi_enumeration`. They are tier1 because an NPI record carries a
verified name and email, but high identity confidence says nothing about
browsing: only 27 of them have a site at all. Checked for bots: 0 bots,
0 suspicious, 0 shared-workstation, average cluster health 99.9. Real people who
have never visited a property.

**The non-HCPs (74,686).** The less obvious group, and the one worth being ready
to answer on. 74,112 of them have no site either, so there is nothing to infer a
condition from. Their origins are covered in section 9 — they are email-only
records matched into the identity hub, never observed on a site.

In both cases, leaving the condition NULL is the honest answer. Assigning one
would place these people in condition audiences they have no connection to.

---

## 5. `site_domain` vs `site_domains`

Two different questions:

| Column | Type | Answers |
|---|---|---|
| `site_domain` | STRING | "Which site is this person's **tenant**?" — one value |
| `site_domains` | ARRAY | "Which sites has this person **touched**?" — the full set |

```
site_domain  = 'ms'
site_domains = ['ms', 'mya', 'sma']
```

The primary is the **most-visited** site, not the most recent. Calibrated against
441,498 profiles with a known Mailchimp-derived site: most-visited agreed 87.3%
of the time, most-recent only 82.1%.

| Sites touched | Profiles |
|---|---|
| 1 | 7,199,315 |
| 2 | 82,172 |
| 3 | 21,544 |
| 4+ | 13,618 |

**Use `site_domain`** for tenant partitioning and per-person counts.
**Use `site_domains`** for audience reach — everyone who touched a property.

---

## 6. The parent/child site model

Some properties are umbrella audiences that narrower sites roll up into.
`rarecancernews` covers multiple myeloma, glioma, pancreatic cancer.

**This is not `site_domains`.** That column is *behavioral* — the sites one
person happened to visit, which for a real profile reads
`['als','cf','hem','hyp','ms','mya','par','ph','sma']`: nine unrelated
conditions, not a hierarchy. The parent map is *structural* — identical for
everyone on a site, whether or not they ever visited the parent.

### Where the data lives

**In the database, as columns on `profile_lookup`.** Two of them, live in
production today on all 99 `site_registry` rows:

| Column | Meaning |
|---|---|
| `parent_site_key` | The site this one rolls up into. NULL = ungrouped. |
| `is_parent_site` | TRUE if other sites roll up into this one. |

**Most sites will never have a parent, and that is the intended end state.**
Grouping is the exception: a handful of umbrella properties with a few children
each. Only the *children* carry a `parent_site_key` — a parent's own is NULL,
because a parent has no parent. So a realistic configuration looks like this,
with the other ~95 sites untouched:

| `site_key` | `parent_site_key` | `is_parent_site` |
|---|---|---|
| `rc` | NULL | TRUE |
| `myl` | `rc` | FALSE |
| `lym` | `rc` | FALSE |
| `ms` | NULL | FALSE |
| …95 more | NULL | FALSE |

This is why `COALESCE(parent_site_key, site_key)` appears in every group query:
an ungrouped site self-parents, so it still appears exactly once and the
sparseness costs nothing. Adding three rollup rows changes three sites; the other
96 behave identically before and after.

Nothing needs to be run to read them — this works right now:

```sql
SELECT site_key, root_domain,
       COALESCE(parent_site_key, site_key) AS site_group,
       IFNULL(is_parent_site, FALSE)       AS is_parent
FROM `bi-data-391216.profile_data`.profile_lookup
WHERE lookup_type = 'site_registry' AND is_active;
```

<<red>>
Both columns are currently NULL on every row because no rollups have been
defined yet. That is a pending business decision, not a missing capability.
<</red>>

### How a grouping is defined

The *values* are seeded from `profile_database_maintenance.sql`, the same as
every other reference value in this database — `site_registry` itself,
`site_condition_mapping`, `conditions_dict`, gender, engagement tiers. All 315
rows come from that file.

This matters operationally: the maintenance step runs
`DELETE FROM profile_lookup WHERE TRUE` and reseeds.
**A row typed directly into BigQuery survives until the next maintenance run and then disappears** — true for
every lookup value, not just parents. Edit the SQL, then apply with
`python scripts/run_maintenance_only.py` (about a minute).

One line per child, in `_site_group`:

```sql
CREATE OR REPLACE TEMP TABLE _site_group AS
SELECT * FROM UNNEST([
    STRUCT('rc'  AS parent_site_key, 'myl' AS child_site_key),
    ('rc',  'lym'),
    ('amy', 'fap')
])
WHERE parent_site_key IS NOT NULL;
```

Stated once as parent → children. The registry join derives both
`parent_site_key` (on each child) and `is_parent_site` (on the parent), so the
two can never disagree.

### Both directions

**Parent → children** — everyone under a group:

```sql
WITH grp AS (
  SELECT site_key, COALESCE(parent_site_key, site_key) AS site_group
  FROM `bi-data-391216.profile_data`.profile_lookup
  WHERE lookup_type = 'site_registry' AND is_active
)
SELECT COUNT(DISTINCT pc.bn_id)
FROM `bi-data-391216.profile_data`.profile_core pc
JOIN grp g ON g.site_key = LOWER(pc.site_domain)
WHERE g.site_group = 'rc';
```

**Child → parent** — which group does this site belong to:

```sql
SELECT site_key, COALESCE(parent_site_key, site_key) AS site_group
FROM `bi-data-391216.profile_data`.profile_lookup
WHERE lookup_type = 'site_registry' AND is_active AND site_key = 'myl';
```

### Worked mockup

Simulated on live data with `rc` as parent of `myl` and `lym`:

```
group  site   child?  condition        profiles  group_total
rc     rc     No      Rare Cancer       225,386      233,006
rc     lym    Yes     Lymphoma            7,590      233,006
rc     myl    Yes     Myeloma                30      233,006
```

The group audience is **233,006** against 225,386 for `rc` alone — 7,620 people
a site-level query misses. Conditions covered: Lymphoma, Myeloma, Rare Cancer.

**Currently zero rollups are configured**, pending the business definition. Every
query above already works: `COALESCE(parent_site_key, site_key)` means an
ungrouped site self-parents, so the report is flat today and becomes a hierarchy
the moment rows are added — with no query change.

**One level only.** A child cannot itself be a parent; the resolver does a single
join, not a recursive walk. Enforced by test.

---

## 7. Demonstration queries

Every query below is **copy-paste ready for the BigQuery console**. Table names
are fully qualified with the project (`bi-data-391216`), so they run without
setting a default project, and each block carries its own explanation as SQL
comments — paste the whole block, comments included.

All queries are read-only.


### -- Overall coverage

```sql
-- Overall coverage. The two headline numbers.
-- Run this first -- everything else explains these two figures.
SELECT
  COUNT(*) AS profiles,
  COUNTIF(site_domain IS NOT NULL) AS with_site,
  ROUND(100 * COUNTIF(site_domain IS NOT NULL) / COUNT(*), 2) AS site_pct,
  COUNTIF(preferred_condition IS NOT NULL) AS with_condition,
  ROUND(100 * COUNTIF(preferred_condition IS NOT NULL) / COUNT(*), 2) AS condition_pct
FROM `bi-data-391216.profile_data`.profile_core;
```

### -- Site fill by tier, with confidence

```sql
-- Site fill by tier, strongest signal first.
-- confidence is how far the signal is from a deliberate act:
-- 0.95 wp_user_id (the site is IN the identifier) down to
-- 0.50 content_affinity (they read one article).
SELECT site_domain_source, COUNT(*) AS profiles,
       ROUND(AVG(site_domain_confidence), 2) AS avg_confidence
FROM `bi-data-391216.profile_data`.profile_core
WHERE site_domain_source IS NOT NULL
GROUP BY 1 ORDER BY profiles DESC;
```

### -- Condition fill by source

```sql
-- Condition fill by source, same ladder idea.
-- site_registration dominates: registering on a condition site is a
-- deliberate act, so it outranks what someone merely read.
SELECT preferred_condition_source, COUNT(*) AS profiles,
       ROUND(AVG(preferred_condition_confidence), 2) AS avg_confidence
FROM `bi-data-391216.profile_data`.profile_core
WHERE preferred_condition IS NOT NULL
GROUP BY 1 ORDER BY profiles DESC;
```

### -- Every site with condition, status and population

```sql
-- Every site with its condition, status and population.
-- NOTE: the condition comes from site_condition_mapping, NOT from the
-- registry row. Joining condition_key off site_registry returns NULL
-- for every site -- the registry says what a site IS, the mapping says
-- what it is ABOUT.
SELECT r.site_key, r.root_domain, r.site_status,
       cd.label AS condition_label,
       COUNT(pc.bn_id) AS profiles
FROM `bi-data-391216.profile_data`.profile_lookup r
LEFT JOIN `bi-data-391216.profile_data`.profile_lookup m
       ON m.lookup_type = 'site_condition_mapping' AND m.is_active
      AND m.site_key = r.site_key
LEFT JOIN `bi-data-391216.profile_data`.conditions_dict cd
       ON cd.condition_key = m.condition_key AND cd.is_active
LEFT JOIN `bi-data-391216.profile_data`.profile_core pc
       ON LOWER(pc.site_domain) = r.site_key
WHERE r.lookup_type = 'site_registry' AND r.is_active
GROUP BY 1, 2, 3, 4
ORDER BY profiles DESC;
```

The condition comes from `site_condition_mapping`, **not** the registry row.
Joining `condition_key` off the registry returns NULL for every site.

### -- Tenant count vs audience reach

```sql
-- Tenant count vs audience reach, for one site.
-- primary_ms  = people whose PRIMARY site is MS
-- touched_ms  = everyone who visited MS at all
-- The difference is people a site_domain-only audience silently misses.
SELECT
  (SELECT COUNT(*) FROM `bi-data-391216.profile_data`.profile_core
    WHERE LOWER(site_domain) = 'ms') AS primary_ms,
  (SELECT COUNT(DISTINCT bn_id) FROM `bi-data-391216.profile_data`.profile_core, UNNEST(site_domains) s
    WHERE LOWER(s) = 'ms') AS touched_ms;
```

MS: 1,219,019 primary vs 1,244,673 touched — **25,654** people a
`site_domain`-only query misses.

### -- Cross-site behavior

```sql
-- Cross-site behaviour: how many sites does one person touch?
-- Most touch exactly one, which is why a single tenant key works --
-- but the tail is real and site_domains is how you reach it.
SELECT ARRAY_LENGTH(site_domains) AS sites_touched, COUNT(*) AS profiles
FROM `bi-data-391216.profile_data`.profile_core
WHERE site_domain IS NOT NULL
GROUP BY 1 ORDER BY 1;
```

### -- Active vs dormant properties

```sql
-- Active vs dormant properties.
-- A hiatus site has stopped publishing but KEEPS its profiles. This
-- marks the PROPERTY dormant, not the people -- do not use it to
-- exclude an audience.
SELECT r.site_status,
       COUNT(DISTINCT r.site_key) AS sites,
       COUNT(pc.bn_id) AS profiles
FROM `bi-data-391216.profile_data`.profile_lookup r
LEFT JOIN `bi-data-391216.profile_data`.profile_core pc ON LOWER(pc.site_domain) = r.site_key
WHERE r.lookup_type = 'site_registry' AND r.is_active
GROUP BY 1;
```

33 sites are on hiatus and still carry ~37K profiles. The flag marks the
**property** dormant, not the people.

### -- Who has no condition, and why

```sql
-- Who has no condition, and why.
-- The answer is mostly doctors: bulk-loaded from the public NPI
-- registry, verified identity, never visited a property. Leaving them
-- NULL is honest; a condition would put them in audiences they have
-- no connection to.
SELECT
  COUNT(*) AS no_condition,
  COUNTIF(is_hcp) AS hcp,
  COUNTIF(source_created_basis = 'npi_enumeration') AS from_npi_registry,
  COUNTIF(last_active_at IS NULL) AS never_active
FROM `bi-data-391216.profile_data`.profile_core
WHERE preferred_condition IS NULL;
```

### -- Multi-site people

```sql
-- Multi-site people, as raw examples.
-- Read a few rows: the sites in one array are usually UNRELATED
-- conditions. That is why site_domains is not a hierarchy.
SELECT site_domain AS primary_site, site_domains AS all_sites
FROM `bi-data-391216.profile_data`.profile_core
WHERE ARRAY_LENGTH(site_domains) >= 4
LIMIT 10;
```

### -- Rollup groups (works today, flat until seeded)

```sql
-- Rollup groups. Works today and returns all 99 sites flat,
-- because no parent has been defined yet.
-- COALESCE is what makes that safe: an ungrouped site self-parents,
-- so every site appears exactly once either way. Seed a parent and
-- this becomes a hierarchy with NO change to this query.
SELECT COALESCE(parent_site_key, site_key) AS site_group,
       site_key, root_domain,
       IFNULL(is_parent_site, FALSE) AS is_parent,
       (parent_site_key IS NOT NULL) AS is_child
FROM `bi-data-391216.profile_data`.profile_lookup
WHERE lookup_type = 'site_registry' AND is_active
ORDER BY site_group, is_child;
```

### -- Group audience across parent and children

```sql
-- Group audience across parent and children.
-- Flat today (each group = one site). Once parents are seeded the
-- same query returns the combined audience per group.
WITH grp AS (
  SELECT site_key, COALESCE(parent_site_key, site_key) AS site_group
  FROM `bi-data-391216.profile_data`.profile_lookup
  WHERE lookup_type = 'site_registry' AND is_active
)
SELECT g.site_group,
       COUNT(DISTINCT pc.bn_id) AS group_audience,
       COUNT(DISTINCT g.site_key) AS sites_in_group
FROM `bi-data-391216.profile_data`.profile_core pc
JOIN grp g ON g.site_key = LOWER(pc.site_domain)
GROUP BY 1 ORDER BY group_audience DESC LIMIT 20;
```

### -- Condition distribution

```sql
-- Condition distribution -- the top conditions by population.
SELECT preferred_condition.label AS condition, COUNT(*) AS profiles
FROM `bi-data-391216.profile_data`.profile_core
WHERE preferred_condition IS NOT NULL
GROUP BY 1 ORDER BY profiles DESC LIMIT 25;
```

---

## 8. Verification suite — every check returns 0

Run all eight before trusting any number above.

```sql
SELECT
  -- 1. No site code left as a human-readable condition label.
  (SELECT COUNT(*) FROM `bi-data-391216.profile_data`.profile_core
    WHERE preferred_condition_source = 'content_affinity'
      AND REGEXP_CONTAINS(preferred_condition.label, r'^[a-z0-9]{2,6}$')) AS c1_site_codes_as_labels,

  -- 2. Same, in the snapshot the restore paths read. A dirty snapshot
  --    reintroduces bad labels on every nightly run.
  (SELECT COUNT(*) FROM `bi-data-391216.profile_ops`.profile_core_snapshot
    WHERE snapshot_run_id = (SELECT snapshot_run_id FROM `bi-data-391216.profile_ops`.profile_core_snapshot
                             ORDER BY snapshotted_at DESC LIMIT 1)
      AND preferred_condition_source = 'content_affinity'
      AND REGEXP_CONTAINS(preferred_condition.label, r'^[a-z0-9]{2,6}$')) AS c2_snapshot_site_codes,

  -- 3. Every site_domain is a registered site.
  (SELECT COUNT(*) FROM `bi-data-391216.profile_data`.profile_core pc
    LEFT JOIN `bi-data-391216.profile_data`.profile_lookup r
      ON r.lookup_type = 'site_registry' AND r.is_active AND r.site_key = LOWER(pc.site_domain)
    WHERE pc.site_domain IS NOT NULL AND r.site_key IS NULL) AS c3_off_registry_sites,

  -- 4. A profile with a site has a non-empty array.
  (SELECT COUNT(*) FROM `bi-data-391216.profile_data`.profile_core
    WHERE site_domain IS NOT NULL AND ARRAY_LENGTH(site_domains) = 0) AS c4_empty_array,

  -- 5. The primary is always a member of its own array.
  (SELECT COUNT(*) FROM `bi-data-391216.profile_data`.profile_core
    WHERE site_domain IS NOT NULL
      AND LOWER(site_domain) NOT IN UNNEST(site_domains)) AS c5_primary_not_in_array,

  -- 6. Every condition label resolves in the dictionary.
  (SELECT COUNT(*) FROM `bi-data-391216.profile_data`.profile_core pc
    LEFT JOIN `bi-data-391216.profile_data`.conditions_dict cd
      ON cd.label = pc.preferred_condition.label AND cd.is_active
    WHERE pc.preferred_condition IS NOT NULL AND cd.label IS NULL) AS c6_label_not_in_dict,

  -- 7. Confidence is a probability.
  (SELECT COUNT(*) FROM `bi-data-391216.profile_data`.profile_core
    WHERE preferred_condition_confidence IS NOT NULL
      AND (preferred_condition_confidence < 0
           OR preferred_condition_confidence > 1)) AS c7_confidence_out_of_range,

  -- 8. Every site_condition_mapping points at a real dictionary entry.
  (SELECT COUNT(*) FROM `bi-data-391216.profile_data`.profile_lookup m
    LEFT JOIN `bi-data-391216.profile_data`.conditions_dict cd
      ON cd.condition_key = m.condition_key AND cd.is_active
    WHERE m.lookup_type = 'site_condition_mapping' AND m.is_active
      AND cd.condition_key IS NULL) AS c8_mapping_without_dict;
```

### What each check protects against

**c1 / c2** — site codes appearing as condition labels. 2,463,480 profiles once
read `ms` or `lems` instead of `Multiple Sclerosis`. c2 exists because healing
the live table is not enough: the snapshot feeds a restore path and put them
back on the next nightly run.

**c3** — a tenant key naming no real site. Silently excludes those people from
every legitimate audience while looking like data.

**c4 / c5** — the array drifting out of step with the primary. Four steps write
`site_domain` and only one maintains `site_domains`, so this needs watching.

**c6 / c8** — a label or mapping pointing at a dictionary entry that does not
exist. Those profiles drop out of every join through `conditions_dict`.

**Timing note:** run the suite when no build is in progress. During a refresh,
c4 and c5 report transient in-flight drift that settles when the run completes.

---

## 9. Known limitations

### The 362,438 profiles with no site (4.7%)

Two distinct populations, neither recoverable from any signal we hold:

| Group | Count | What they are |
|---|---|---|
| HCP / NPI registry | 286,522 | Doctors bulk-loaded from the public NPI registry |
| Non-HCP, email-only | 74,112 | Known by email, never observed on a property |

<<red>>
The second group is the less obvious one and worth stating plainly, because
"they must have come from somewhere" is the natural objection. 63,561 of them
have an email address but no site. By origin:
<</red>>

| Origin | Count |
|---|---|
| `hub:wordpress/EMAIL_EXACT` | 31,763 |
| `mailchimp_warehouse` | 13,028 |
| `cookie_creation` | 10,433 |
| `mailchimp_signup` | 9,305 |
| `hub:npi_registry/...` | 8,295 |

These are records matched into the identity hub by email — from a warehouse
extract, a list import, or a WordPress account whose site was never captured.
They are real people; we simply have no observation of them on a property.

**Every tier has been tested against them and returns zero.** Not `wp_user_id`,
not Mailchimp at any subscription status, not `site_events`, not
`content_affinity`, and not the acceptor. 10,462 carry a `bnfpvid` but none of
those pvids appears in an acceptor event with a resolvable site prefix.

So 95.3% is the practical ceiling with today's instrumentation. The gap is not a
pipeline defect and no re-run will close it.

<<red>>
### The instrumentation gap — the one thing that would move the number

`site_prefix` is set by the BioNews WordPress plugin.
**Roughly 1.2M acceptor events a month arrive without it**, in two different ways:

**Plugin not installed (97% of the gap).** The field is never set on properties
that do not run the plugin — `bionews.com` (188K events in 30 days, and its
prefix has *never* been set), plus the `survey.*` and `hcp.*` subdomains, which
sit on a different stack. This is a deployment question, not a code defect.

**Plugin installed but the prefix is lost (3%).** On a site where
`postType='post'` correctly reports its prefix, forum templates
(`topic`, `forum`, `reply`) report `undefined` — 20,824 events on
`pulmonaryfibrosisnews` in 30 days. Present in both plugin 1.9.40 and 1.9.42, so
it is not fixed in the newer release. `site_prefix` should be site-global rather
than per-template.

**We already work around both.** The `acceptor_url` tier parses
`pageDetails.url` when the prefix is missing and has recovered 63,486 profiles.
But it is a workaround: it cannot help any consumer that reads `site_prefix`
directly, and it depends on URL patterns that could change. Fixing at source
improves every downstream consumer, not just this fill.

**Rollups not yet defined.** The mechanism is built and tested; the business
definition of which sites roll up into which is outstanding.
<</red>>
