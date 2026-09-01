# Identity hub: full rebuild runbook

**Status:** required reading before running `--refresh full` against prod
**Written:** 2026-08-20
**Why now:** the retention policy that governs rebuild behaviour was changed on
2026-08-07 and has never been exercised by a full rebuild.

---

## Read this first

**Every full rebuild on record that we have downstream data for caused a
profile build to fail its acceptance gate.** Not most. Both of them.

| Date | Mode | `bn_id_xref` | Change | Downstream |
|---|---|---:|---:|---|
| 2026-05-24 | `full` | 19,212,694 → 17,062,095 | −11.2% | `failed_gate` |
| 2026-08-06 | full rebuild | 27,153,725 → 15,710,972 | −42.1% | `failed_gate` 8/07 |

That is not an argument against rebuilding. A full rebuild is the only
operation that re-evaluates old edges, so it is the only way accumulated
configuration changes ever actually take effect. It is an argument for
treating it as a change with real blast radius rather than a maintenance
chore.

**The specific reason this one is different:** the 2026-08-07 fix (commit
`3735ee9`) moved three levers together -- `decay_schedule`'s 0.8 band from 90
to 400 days, `type_influence_days` for `bnfpvid`/`client_id` from 365 to 400,
and `edge_tiers.browser_expiry_days` from 365 to 400. Every run since has been
`incremental`, and **incrementals never re-evaluate old edges**. That is
precisely the blind spot that hid the original 90-day bug for months. The next
full rebuild is the first time those settings are actually tested.

---

## What the existing safeguards will and will not catch

There are two gates. Neither would have stopped 2026-08-06.

**The hub's shrink safeguard** (`shared/identity_hub.py:1856`,
`_check_shrink_safeguard`) aborts a write when the new row count falls below
`shrink_abort_threshold` of the previous count. That threshold is **0.5** and
is not set in `configs/identity_hub.yaml`, so it takes the code default.

On 2026-08-06 xref came in at 15,710,972 against 27,153,725 -- **57.9% of
previous**. Above the threshold. It wrote cleanly.

**The profile database's input gate** (`identity_source_row_delta`, added
2026-08-19) fails the build when the consumed snapshot shrinks more than 10%.
It would have caught 8/06 four times over. But note what it does and does not
protect: it stops bad data reaching *consumers*. By the time it fires, **the
hub tables have already been overwritten.** Recovering the hub itself is a
separate problem, and on 2026-08-06 it was only solvable because an operator
happened to have taken an ad-hoc backup the day before.

That is the gap this runbook exists to close.

---

## Before you run

### 1. Take a snapshot. This is the step that makes everything else recoverable.

```
cd /home/orchestrator
venv/bin/python scripts/snapshot_monthly_identity_profile.py --env prod --force
```

`--force` replaces the current month's snapshot, which is what you want -- a
snapshot from three weeks ago is a much worse comparison point than one from
five minutes ago. It covers `bn_id_xref`, `bn_id_hub`, `bn_id_node_index`,
`bn_id_manifest`, `bn_id_metrics` and `profile_core`.

BigQuery table snapshots are metadata-only at creation, so this costs
essentially nothing and takes about fifteen seconds.

### 2. Record the baseline

```sql
SELECT COUNT(*)                                    AS xref_rows,
       COUNT(DISTINCT bn_id)                       AS bn_ids,
       COUNT(DISTINCT IF(is_hcp, bn_id, NULL))     AS hcp_bn_ids
FROM `bi-data-391216.identity_hub_data.bn_id_xref`;

SELECT COUNT(*)                          AS profiles,
       COUNTIF(cluster_tier = 'tier1')   AS tier1,
       COUNTIF(cluster_tier = 'tier2')   AS tier2
FROM `bi-data-391216.profile_data.profile_core`;
```

As of 2026-08-20:

| Measure | Value |
|---|---:|
| `bn_id_xref` rows | 29,701,192 |
| distinct `bn_id` | 6,194,182 |
| HCP `bn_id`s | 376,856 |
| `profile_core` | 6,187,122 |
| tier1 | 794,304 |
| tier2 | 5,392,719 |

### 3. Dry run

```
python orchestrate.py --source identity_hub --env prod --refresh full --dry-run
```

(`--dry-run` is the flag the extractor maps to its no-writes mode; `--test` is
ambiguous to argparse and errors -- corrected 2026-08-20 after it failed live.)

Validates SQL and estimates bytes without writing. Note the limit honestly: a
dry run did **not** catch the unaliased UNION branch that broke a real run
earlier this year. It proves the SQL parses, not that the output is sane.

---

## The stop rule

Decide these before you look at the results, not after.

| Measure | Expected | Stop and investigate |
|---|---|---|
| `bn_id_xref` rows | within ±10% of baseline | **any drop >10%** |
| distinct `bn_id` | within ±5% | drop >5% |
| HCP `bn_id`s | within ±2% | **any drop >2%** |
| tier1 | within ±5% | drop >5% |
| tier2 | within ±15% | drop >15% |

**tier1 and HCP counts are the tight ones and that is deliberate.** In both
prior incidents the damage was almost entirely tier2 -- on 8/07 tier1 moved
only −1.5% while tier2 fell 40.9%. A rebuild that moves *tier1* is doing
something neither incident did, and the HCP population lives there.

A tier2 swing inside 15% is plausibly the retention change doing its job:
the 400-day window should now *retain* identities that the old 90-day
arithmetic evicted, so **tier2 growing is the expected direction.** If tier2
shrinks at all, the fix is not working as intended.

---

## Running it

```
python orchestrate.py --source identity_hub --env prod --refresh full
```

Do not pass `--force-overwrite` unless you have already decided, in advance and
in writing, that a shrink is intended. It bypasses the shrink safeguard
entirely.

Do not pass `--schema-prefix` or `--schema-suffix` expecting isolation. Those
flags were declared and never wired; they affix staging table names in the
Parquet path, and identity_hub is BigQuery-to-BigQuery. An operator sandboxing
a rebuild with them would have rebuilt production and been told it succeeded.
They now raise (commit `1f7a170`), but do not rely on that as your only
protection.

---

## After it finishes

### Re-run the baseline queries and compare against the stop rule.

### Confirm metrics were actually written

```sql
SELECT run_date, run_id, build_mode, git_sha, COUNT(*) AS metrics
FROM `bi-data-391216.identity_hub_data.bn_id_metrics`
WHERE run_date >= CURRENT_DATE() - 1
GROUP BY 1,2,3,4;
```

A healthy run writes 96 metric rows. **Check this explicitly** -- the run that
produced the 8/07 damage (`f97184b4`) wrote **no metrics rows at all**, and the
2026-08-06 full rebuild does not appear in `bn_id_metrics` with
`build_mode = 'full'` either. Full rebuilds are under-instrumented, so absence
of a bad signal is not evidence of a good run.

### Then let the profile build gate it

The next profile refresh consumes the new snapshot and runs
`identity_source_row_delta`. If the hub shrank more than 10%, the build stops
at `failed_gate` and production views keep serving the previous release. Check:

```sql
SELECT build_id, status, assertion_summary
FROM `bi-data-391216.profile_ops.profile_build_runs`
ORDER BY started_at DESC LIMIT 1;
```

**If you follow this with a profile database REBUILD, that query goes blind.**
`profile_database_extractor.py` sets `persist_ops_log = mode != "rebuild"`, so a
profile rebuild writes no row to `profile_build_runs` and no rows to
`profile_build_steps` -- by design since the blue-green runtime landed
(`0d092cf`, 2026-04-30). Six of the seven blue-green publishes on record have no
run row at all; the only one that does came from `resume_rebuild`.

So after a profile rebuild, verify from the data and from
`profile_ops.profile_publish_manifest` instead, which is written either way. A
rebuild also skips the `no_concurrent_rebuild` preflight for the same reason,
so nothing stops the 12:01 refresh starting underneath it -- do not run one
across that window.

---

## Incident record: 2026-08-21 rebuild, rolled back 2026-08-22

The rebuild itself passed every stop rule (xref +13.9%, bn_ids +20.1%, HCP
+0.8%, tier1 -1.8%, tier2 +23.4% -- the 400-day retention retaining identities
as intended; 113 metric rows; manifest promoted 23:19 UTC). The damage came the
next morning: the 12:01 UTC profile **refresh** consumed the rebuilt hub, created
1.24M new profile_core rows, but engagement rows are only written by the
rebuild path -- so core/engagement parity broke (16.5%), fill rates diluted
(condition 51.4% -> 42.4%), and in refresh mode `failed_gate` does NOT roll
back: the half-consistent population went live in every consumer view.

Rolled back the same day: hub tables to 2026-08-21 23:18:00 UTC (just before
the rebuild's promotion), profile_data tables to 2026-08-22 11:55:00 UTC (just
before the refresh). Verified: core 6,221,944 = pre-refresh, 0 orphans,
core/engagement gap 0.54%.

**Fixed the same day (so the next rebuild cannot repeat this):**
- `reconcile.sql` STEP 4 (engagement parity): every `profile_core` row gets a
  stub `profile_engagement` row if it lacks one, so the core/engagement gate
  holds by construction when the hub population jumps.
- The `fill_rate_drift_critical` gate now judges ABSOLUTE filled counts (real
  loss = filled count down >5%), not percentage points of a moving population;
  the pp rule remains the soft monitoring signal and the fallback when no
  previous total is stored. Dilution by retained cookie-only identities no
  longer fails the build.

**Lessons, now rules:**
1. A hub full rebuild must be followed immediately by a profile **rebuild** in
   the same window, before any nightly refresh can touch it. The refresh path
   cannot absorb a population jump (it creates core rows without engagement
   rows) -- fix that gap before the next attempt.
2. Restores: `CREATE ... CLONE` fails on tables with 3+ chained clones (the
   blue/green profile tables always will). Use a **copy job with a time-travel
   decorator** (`table@<epoch_ms>`, WRITE_TRUNCATE): preserves schema,
   descriptions, partitioning, clustering, and resets the chain.
3. This client interprets bare `FOR SYSTEM_TIME AS OF` literals in local time.
   Always write `TIMESTAMP '... UTC'`.
4. Snapshot the *current* state before rolling back, not just before rebuilding
   -- the post-rebuild graph is preserved in `*_prerollback_20260822` snapshots
   and can be cloned forward when the rebuild is redone properly.

### Redo, attempt 1 (2026-08-22): profile rebuild died five minutes in

After the hub was cloned forward from the `*_prerollback_20260822` snapshots,
`--build-mode rebuild` ran 14:51-14:55 UTC and stopped at
`populate_newsletter_forum`: "Cannot replace a table with a different
partitioning spec" on `profile_data_candidate.profile_preferences`. Commit
`bdc1980` (8/18) had removed the `DROP TABLE` ahead of that CTAS to make the
step atomic but never declared `CLUSTER BY bn_id` on it; the rebuild's `ddl`
step creates the clustered table in the candidate dataset first, and BigQuery
refuses to replace a clustered table with an unclustered one. Refresh mode
never runs the populate steps, so the first rebuild after 8/18 was the first
time the statement ran against a pre-existing clustered table.

Fixed in `384c3d8`: the CTAS now clusters like the DDL, and
`tests/unit/test_profile_ctas_matches_ddl.py` fails CI for any profile_data
CTAS whose partitioning or clustered-ness differs from the DDL (BigQuery does
tolerate a change of cluster *columns*, which three other populate steps rely
on). A rerun needs no candidate cleanup: `ddl` is `IF NOT EXISTS` and the CTAS
replace the half-built tables. Rule added: **after any change to a populate
or DDL statement, run `--build-mode rebuild` against the candidate dataset
before the next scheduled rebuild depends on it** -- refresh cannot catch
rebuild-only failures.

### Redo, attempt 2 (2026-08-22 23:11 UTC): died at the views step, 41 minutes in

Every table step succeeded; the `views` step failed on
`Not found: profile_data_candidate.profile_roles`. `profile_metrics` (62dbd5a,
8/19) joined `profile_data.profile_roles` with a hardcoded dataset. In rebuild
mode views are built in `{view_dataset}` while `profile_data.*` references are
rewritten to the candidate *table* dataset, so a view-to-view reference written
that way points at a dataset with no views. Refresh never notices because
production already has the view. Fixed in `dcaad15` (`{view_dataset}.profile_roles`
plus `tests/unit/test_profile_views_reference_view_dataset.py`, which also
rejects a view that depends on one defined later in the file). Recovery:
`--build-mode resume_rebuild` (restore -> views -> snapshot_core -> publish)
reuses the populated candidate tables; no need to redo the 41 minutes.

Pattern across both attempts: **refresh mode exercises neither the populate
CTAS nor a cold view dataset, so any change to those paths is untested until
the next rebuild.** Both guard tests now run in CI; a candidate-only rebuild
(`--build-mode rebuild` stops before publish on gate failure) remains the only
end-to-end check.

### Day after (2026-08-23 12:01 UTC refresh): new gate, old writer

With 7f46edc in place the scheduled refresh consumed the rebuilt hub cleanly:
reconcile backfilled the population to 7,454,259 with full engagement parity,
`missing_parent` and `fill_rate_drift_critical` passed. It then failed
`refresh_safety_check` on ONE sampled profile: `is_family_or_friend`
None -> true with no lineage. The writer is reconcile STEP 1b merge-forward,
which copied role flags from retired bn_ids onto survivors with no lineage and
no provenance (107 bare flags in core, all merge survivors); the rebuild's
merge volume put one in the 200-row sample. Fixed in the commit after
`ed70bbf`: lineage before the MERGE, provenance carried through it, and a
STEP 1c heal for the existing bare flags. Because the refresh had already
landed the population, the `profile_data_candidate` tables from the 23:11 UTC
rebuild became stale -- do NOT run `resume_rebuild` after a refresh has
consumed the rebuilt hub; it would publish older tables over production.

### Afternoon of 2026-08-23: a second profile rebuild, and what it proved

A profile `--build-mode rebuild` ran end to end (all 32 steps -- the two
earlier fixes hold) and was correctly refused by two gates:
`restore_coverage` (16,112 of 670,521 app-field snapshot rows unmapped, 2.4%)
and `fill_rate_drift_critical` (`site_domain` filled 420,007 -> 389,662).
Production was untouched and is the better dataset.

Both point at the hub, not the profile pipeline. Production's
`profile_identifiers` holds **21,087 email identifiers the rebuilt hub no
longer has; 19,551 are currently subscribed Mailchimp members and 16,979 had
been seen by the hub within 400 days.** Not retention (119 rows older than
400 days), not email normalization (4 rows). By pre-rebuild `source_profile`:
browser_only lost 14,943 of 104,061 (14%), browser+ga4 15%, ga4_only 20%,
offline_only 0.9%. The full rebuild only re-derives identities from events
inside its source window (`default_lookback_days: 548`); an email that entered
the graph through a browser event and never through an offline anchor is not
re-found. The incremental hub had simply kept them. Everything downstream that
depends on those identities (app-written fields, site_domain, some emails)
cannot be rebuilt from scratch.

Consequences and rules:
- **A profile rebuild is only as complete as the hub it rebuilds from.** It
  is not a lossless operation. The daily refresh carries attributes forward
  through merges; a from-scratch candidate does not. Do not `--force` a
  candidate past these gates.
- The restore remap now falls back to the stable bn_id (commit after
  `6602b26`), which closes the restore_coverage gap for this failure mode.
  The `site_domain` gap is real data the candidate cannot derive.
- Open hub item (decide before the next hub full rebuild): tier1 anchors that
  are current Mailchimp members must survive a rebuild regardless of browser
  history -- seed them from `mailchimp_data.members`, or carry forward tier1
  nodes from the previous xref. Evidence queries are in the 2026-08-23
  session; the pre-rebuild graph is `platform_monthly_snapshots.bn_id_xref_202608`.

### Close-out (2026-08-24): why quiet members were never in the graph

`mailchimp_data.members.subscriber_hash` is NULL for every row (438,120 of
438,120), so the email_bridge members source produced zero anchor edges and
fell to a standalone-node branch that does not survive to the graph. Members
only got anchored when they CLICKED something (campaign_email_activity's
email_id is MD5(lower(email))). That is why 3,529 subscribed members were
never in any generation of the graph, and why the fan-out incident hit
engaged subscribers hardest -- engagement was the only path in.

Fix: the members source now computes the hash itself
(`id_computed: TO_HEX(MD5(LOWER(TRIM(email_address))))` -- verified the
computed values land in the existing subscriber_hash identifier space) and
reads the whole members table on incremental runs (`window: false`, 438K
rows). The 3,529 backfill happens automatically on the first incremental
after deploy; no repair pass, no rebuild.

Also armed: the monthly snapshot cron is installed (13:30 EDT on the 1st);
the script now verifies each snapshot's row count and emails the standard
pipeline failure alert on any failure; and the daily profile preflight
carries a soft `monthly_snapshot_freshness` check (<= 35 days) so a silent
cron failure surfaces the next morning, not during the next incident.

### Recovery namespace separated from the monthly cron (2026-08-25)

The pre-rebuild state now lives under its own incident names --
`platform_monthly_snapshots.{bn_id_xref,bn_id_hub,bn_id_node_index,
bn_id_manifest,bn_id_metrics,profile_core}_prerebuild_20260820` (90-day
expiry) -- cloned from the `*_202608` monthly set. The monthly cron owns the
`_YYYYMM` namespace outright: running the snapshot script with `--force`
replaces a monthly copy but can no longer destroy a recovery copy. Recovery
inventory naming convention: `*_prerebuild_20260820`, `*_prerollback_20260822`,
`*_prerepair_20260823`, `*_prearchive_20260825`.

## If it goes wrong

The hub tables are already overwritten. Restore from the snapshot taken in
step 1:

```sql
CREATE OR REPLACE TABLE `bi-data-391216.identity_hub_data.bn_id_xref`
CLONE `bi-data-391216.platform_monthly_snapshots.bn_id_xref_YYYYMM`;
```

Repeat for `bn_id_hub`, `bn_id_node_index` and `bn_id_manifest`. Restoring the
manifest matters -- it is what points the profile build at a promoted run.

`profile_data` can also be recovered through BigQuery time travel within 7
days, which is how `profile_data` was restored on 2026-08-06. Do not rely on
time travel for the hub if more than a week has passed.

After restoring, verify the population contract exactly rather than
approximately. The 2026-08-06 recovery was confirmed at `profile_core =
5,681,415` on the nose, with zero orphans.

---

## Recommended before the next rebuild

**DONE 2026-08-20: `shrink_abort_threshold: 0.90` is now set explicitly in
`configs/identity_hub.yaml`, matching this runbook's xref stop rule.** A >10%
shrink now aborts the write unless `--force-overwrite` is passed deliberately.
The original reasoning is kept below for the record.

**Set `shrink_abort_threshold` explicitly in `configs/identity_hub.yaml`.** It
previously fell through to the 0.5 code default, which is tuned for
catastrophic loss and missed a 42% one. Tightening it is a real decision with a
real trade-off, not an obvious win: at 0.90 the 2026-05-24 rebuild would also
have aborted, and that −11.2% step-down was evidently accepted at the time
since it was never reverted. Pick the number deliberately and record why.

Whatever it is set to, it belongs in the config where it is visible, not
implicit in a function default.

---

## Related

- `scripts/snapshot_monthly_identity_profile.py` -- the snapshot tool
- `plugins/profile_database_extractor.py` -- `identity_source_row_delta`
- commit `3735ee9` -- the retention policy and the incident that caused it
- commit `1f7a170` -- the isolation flags that silently rebuilt production
- `docs/IDENTITY_HUB_EXPLAINED.md` -- how union-find and decay actually work
- `docs/IDENTITY_HUB_PROMOTE_RUNBOOK.md` -- a different failure mode: the
  rebuild succeeded but promotion stuck partway. If the manifest is not
  stamped after this runbook's step 4, go there.
