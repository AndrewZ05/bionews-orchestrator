# profile_lookup — the reference vocabulary

`profile_data.profile_lookup` is one table holding **every enum, picklist, and
mapping** the profile database uses. It replaced 16 separate `ref_*` tables. If
you need to know what a code means — a site, a condition, a gender, an
engagement tier — this is where it is defined.

315 active rows across 13 `lookup_type` values.

---

## Shape

Every row is `(lookup_type, lookup_key)` plus a label and a set of typed
columns. Which columns are populated depends on the type; the rest are NULL.

| Column | Meaning |
|---|---|
| `lookup_type` | Which vocabulary this row belongs to. **Always filter on it.** The table is clustered by this column. |
| `lookup_key` | The machine value stored elsewhere (`'male'`, `'par'`, `'high'`). |
| `label` | Human-readable display text. |
| `description` | Optional longer explanation. |
| `site_key` | Canonical site code (`'par'`). This is the value `profile_core.site_domain` stores. |
| `root_domain` | Public domain without TLD (`'parkinsonsnewstoday'`). |
| `wp_code` | WordPress install prefix (`'bnpar'`). |
| `condition_key` | The `conditions_dict` key a site or list maps to. |
| `site_status` | `'active'` or `'hiatus'`. |
| `parent_site_key` | Rollup parent, NULL when ungrouped. |
| `is_parent_site` | TRUE if other sites roll up into this one. |
| `condition` | Parent condition for a subtype. |
| `mesh_id`, `snomed_code`, `snomed_id` | Medical coding. |
| `sort_order`, `is_active`, `created_at` | Housekeeping. |

Until 2026-08-31 these lived in a JSON `metadata` blob read via
`JSON_VALUE(metadata, '$.key')`. **That column no longer exists.** If you have a
saved query using it, it will fail — read the column directly instead.

The move was not cosmetic: `JSON_VALUE` on a misspelled key returns NULL rather
than erroring, so a typo read as "no mapping" and the row was silently skipped.
A real column makes that a query error.

---

## The 13 lookup types

### Site vocabulary — the three that matter most

**`site_registry`** (99 rows) — the authority on what a site *is*.
Populates `site_key`, `root_domain`, `wp_code`, `site_status`.

```
site_key='par'  root_domain='parkinsonsnewstoday'  wp_code='bnpar'  site_status='active'
```

Nothing may treat a code as a valid tenant unless it appears here. The site-fill
pipeline enforces this: an unrecognized code produces no fill rather than an
invented tenant, because `site_domain` gates access and a wrong value is worse
than a missing one.

**`site_condition_mapping`** (100 rows) — what a site is *about*.
Populates `site_key`, `condition_key`.

```
site_key='aadc'  condition_key='aadc_deficiency'
```

100 rows against 99 sites because some sites have spelling variants
(`anc`/`anca`).

**`condition_mailchimp_mapping`** (58 rows) — which site and condition a
Mailchimp list belongs to. `lookup_key` is the list abbreviation.

```
lookup_key='MS'  site_key='ms'  condition_key='multiple_sclerosis'
```

### Medical coding

**`condition_subtype`** (5) — `condition`, `mesh_id`, `snomed_code`.
**`hcp_specialty`** (9) — `snomed_id`.

### Plain picklists

`topic` (8), `caregiver_relationship` (6), `newsletter_type` (6),
`practice_setting` (6), `diagnosis_stage` (5), `years_in_practice` (5),
`engagement_tier` (4), `gender` (4). Label and description only.

---

## How to use it

**Always filter on `lookup_type` first.** The table is clustered on it, and
`lookup_key` is only unique *within* a type — `'als'` exists as both a site and
a condition mapping.

**Always filter on `is_active`.** Retired entries stay for historical rows.

**Join `profile_core.site_domain` to `site_key`, not `lookup_key`.** They happen
to match for `site_registry`, but `lookup_key` is a display identity and
`site_key` is the semantic one.

**Do not edit it in BigQuery.** It is deleted and reseeded from
`sql/profile_database_maintenance.sql` on every maintenance run — a manual edit
survives until the next run and then vanishes. Edit the SQL, then apply with:

```
python scripts/run_maintenance_only.py        # ~1 min, reference data only
```

A change here updates the *vocabulary*, not the profiles that consume it. A new
`site_condition_mapping` row does not fill anybody's condition until the
downstream enrichment steps run in a refresh.

---

## Sample queries

### What does this site code mean?

```sql
SELECT site_key, root_domain, wp_code, site_status
FROM profile_data.profile_lookup
WHERE lookup_type = 'site_registry' AND is_active
  AND site_key = 'par';
```

### Every site with its condition and population

```sql
SELECT
    r.site_key,
    r.root_domain,
    r.site_status,
    m.condition_key,
    cd.label AS condition_label,
    COUNT(pc.bn_id) AS profiles
FROM profile_data.profile_lookup r
LEFT JOIN profile_data.profile_lookup m
       ON m.lookup_type = 'site_condition_mapping' AND m.is_active
      AND m.site_key = r.site_key
LEFT JOIN profile_data.conditions_dict cd
       ON cd.condition_key = m.condition_key AND cd.is_active
LEFT JOIN profile_data.profile_core pc
       ON LOWER(pc.site_domain) = r.site_key
WHERE r.lookup_type = 'site_registry' AND r.is_active
GROUP BY 1, 2, 3, 4, 5
ORDER BY profiles DESC;
```

The condition comes from `site_condition_mapping`, **not** from the registry
row. The registry carries site identity; the mapping carries what the site is
about. Joining `condition_key` off the registry silently returns NULL for every
site.

### Turn stored codes into labels (decoding a profile)

```sql
SELECT
    pc.bn_id,
    g.label       AS gender_label,
    r.root_domain AS site,
    r.site_status
FROM profile_data.profile_core pc
LEFT JOIN profile_data.profile_lookup g
       ON g.lookup_type = 'gender' AND g.is_active AND g.lookup_key = pc.gender
LEFT JOIN profile_data.profile_lookup r
       ON r.lookup_type = 'site_registry' AND r.is_active
      AND r.site_key = LOWER(pc.site_domain)
LIMIT 100;
```

Not every `lookup_type` has a matching column on `profile_core` — `engagement_tier`
lives on `profile_engagement`, and several picklists (`topic`,
`caregiver_relationship`, `practice_setting`) are consumed by satellite tables or
by the application rather than stored on the core profile. Check
`INFORMATION_SCHEMA.COLUMNS` before assuming a code is stored where you expect.

### Which Mailchimp list belongs to which site?

```sql
SELECT lookup_key AS list_abbrev, site_key, condition_key
FROM profile_data.profile_lookup
WHERE lookup_type = 'condition_mailchimp_mapping' AND is_active
ORDER BY lookup_key;
```

### Active vs dormant properties

```sql
SELECT r.site_status, COUNT(DISTINCT r.site_key) AS sites,
       COUNT(pc.bn_id) AS profiles
FROM profile_data.profile_lookup r
LEFT JOIN profile_data.profile_core pc ON LOWER(pc.site_domain) = r.site_key
WHERE r.lookup_type = 'site_registry' AND r.is_active
GROUP BY 1;
```

33 sites are on hiatus and still carry ~37K profiles. `site_status` marks the
**property** dormant, not the people — filter on it when the recency of the site
matters, not to exclude the audience.

### Find codes in use that are NOT in the registry

```sql
SELECT LOWER(pc.site_domain) AS unregistered_code, COUNT(*) AS profiles
FROM profile_data.profile_core pc
LEFT JOIN profile_data.profile_lookup r
       ON r.lookup_type = 'site_registry' AND r.is_active
      AND r.site_key = LOWER(pc.site_domain)
WHERE pc.site_domain IS NOT NULL AND r.site_key IS NULL
GROUP BY 1 ORDER BY profiles DESC;
```

A data-quality check worth running after any site change. Some survivors are
legitimate spelling variants the fill step already resolves via its alias map;
others are genuine junk from a historical fallback that has since been removed.

### Rollup groups

```sql
SELECT
    COALESCE(parent_site_key, site_key) AS site_group,
    site_key,
    root_domain,
    IFNULL(is_parent_site, FALSE) AS is_parent,
    (parent_site_key IS NOT NULL)  AS is_child
FROM profile_data.profile_lookup
WHERE lookup_type = 'site_registry' AND is_active
ORDER BY site_group, is_child;
```

`COALESCE` matters: an ungrouped site has a NULL parent and rolls up to itself,
so every site appears exactly once whether or not any grouping is seeded. No
rollups are configured yet, so this is currently flat across all 99 sites and
becomes a hierarchy the moment parent rows are added — with no query change.

---

## Ready-made report

`sql/reporting/site_information.sql` returns one row per site with identifiers,
condition, status, rollup group, and two population counts.

`profiles_primary` is the tenant count — one profile, one site, sums to the
table. `profiles_any_site` is audience **reach** from `profile_core.site_domains`,
so a cross-site person is counted under every site they touched and the column
deliberately sums to more than the population. Do not SUM it expecting a
headcount. `secondary_reach` is the difference: people who touched a site but
whose primary is elsewhere, and whom a `site_domain`-only query misses entirely.
