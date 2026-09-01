# Identity Hub, Explained

**Dataset:** `bi-data-391216.identity_hub_data`
**Numbers verified against production 2026-08-18.**

The single entry point for how the identity hub works. Companions:

| Document | Use it for |
|---|---|
| [IDENTITY_HUB_DATA_DICTIONARY.md](IDENTITY_HUB_DATA_DICTIONARY.md) | Every table, every column, every connector. **Auto-generated** -- it cannot go stale. |
| [../sql/identity_hub_query_cookbook.sql](../sql/identity_hub_query_cookbook.sql) | Runnable queries, all validated against production. |
| [PROFILE_DATABASE_DATA_DICTIONARY.md](PROFILE_DATABASE_DATA_DICTIONARY.md) | What we know *about* a person, once identity has resolved who they are. |

This supersedes `IDENTITY_HUB_GUIDE.md`, `IDENTITY_HUB_SPEC.md` and
`Identity_Profile_Query_Showcase.md`, all of which date from April 2026.

---

## 1. The problem it solves

One person shows up as many identifiers. They read on a phone and a laptop, click
a Mailchimp link, fill in a form, clear their cookies, log in once. Every one of
those events produces a different id, and no single system sees more than a slice.

GA4 will happily tell you that is five "users". The identity hub exists to say it
is one person.

**Today it collapses 29.4M identifiers into 6.13M clusters.**

---

## 2. How it works

Three stages, in order.

### Stage 1 -- connectors emit edges

Each connector reads one source system and looks for two identifiers that
provably, or probably, belong to the same person. It writes an **edge** into
`bn_id_hub`:

```
email:jane@example.com  <->  bnfpvid:1771867...   rule=SURVEYENGINE_DIRECT  conf=1.0
```

There are 25 connectors, of which two -- the raw device-fingerprint rules
`device_stat_id` and `ip_device_time` -- were disabled in August 2026 after
audits showed they had never stitched anyone (see section 4). Connectors run
in a fixed order, and order matters: a later
connector can only merge clusters that earlier ones already created. The full
ordered list is in the data dictionary, generated from the code itself.

Edges are either **deterministic** (both identifiers observed on the same row --
a WordPress user record, a SurveyEngine submission) or **probabilistic** (inferred
from IP plus timestamp within a window, because the source never saw both ids at
once).

### Stage 2 -- union-find collapses edges into clusters

Once every connector has run, the edge set is treated as a graph and union-find
assigns each connected component a single `bn_id`. That component is the person.

**Only edges with `effective_confidence >= 0.80` participate.** Weaker edges are
still written -- they are evidence, and visible when you debug -- but they do not
merge anyone.

#### What union-find actually is

Union-find is a simple way to group records that are connected to each other.

**The important part is that the connection can be indirect.** Given:

```
A is connected to B
B is connected to C
```

union-find determines that A, B and C all belong to the same group, even though
A was never directly connected to C.

As new connections arrive, it combines the related groups automatically. Start
with two separate pairs:

```
A -- B          C -- D
```

That is two groups: `{A, B}` and `{C, D}`. Later you discover `B is connected to
C`. Union-find combines everything into one group:

```
A -- B -- C -- D          ->  {A, B, C, D}
```

The purpose is to take a large number of individual relationships and work out
the final groups of things that are connected.

For identity resolution that means different identifiers collapsing into one
person:

```
email:jane@example.com  --  bnfpvid:1771867...
bnfpvid:1771867...      --  client_id:GA1.2.884...
client_id:GA1.2.884...  --  phone:5551234567
```

Union-find determines all four identifiers belong to the same identity, even
though the email was never seen alongside the phone number.

In simple terms it answers two questions:

1. Are these two things already in the same group?
2. If not, should their groups be combined?

It is useful because it handles chains of relationships very efficiently, even
across millions of records -- which is the situation here: roughly 29 million
identifiers and 62 million edges.

#### The one twist in our version: the root is chosen, not arbitrary

Textbook union-find picks whichever root is convenient. Ours is
`PriorityUnionFind`, and it deliberately chooses the *best* identifier in the
group as the root, using `source_priorities` from `identity_hub.yaml` --
`bnfpvid` (0) outranks `email` (1), which outranks `client_id` (5), and so on.

That matters because `bn_id` is derived from the root:

```
bn_id = "BN_" + base64url(SHA256(canonical_root))[:16]
```

So the identity of a cluster is a deterministic function of its membership. Two
runs over the same members produce the same `bn_id` -- which is why removing
edges that never stitched (see section 4) provably could not change any `bn_id`.

#### How this runs in Python without exhausting memory

The naive approach -- load 62 million edges into a Python list, build the graph,
then materialise every component -- would need tens of gigabytes and fall over.
Four techniques avoid that.

**1. The heavy work happens in BigQuery, not Python.** Connectors write edges
into staging tables (`_staging_edges_*`, then `_staging_agg_*`, then
`_staging_filt_*`). Deduplication, confidence aggregation and quality filtering
all run as SQL. Python never sees the raw edge set -- only what survives the
`>= 0.80` filter.

**2. Edges are streamed, never collected.** The edge reader is a generator that
pages through results 50,000 rows at a time and yields one edge at a time:

```python
for row in job.result(page_size=50_000):
    yield row["key_a"], row["key_b"], row["effective_confidence"]
```

Peak memory is one page, not the whole result.

**3. The data structure is three flat dicts, not a graph of objects.**
`PriorityUnionFind` holds `parent`, `rank` and `best_priority` -- string keys to
string or int values. There are no node objects, no adjacency lists, no edge
objects retained after processing. An edge is consumed by a single `union()` call
and then discarded.

The two classic optimisations keep it fast as well as small:

- **Path compression** -- `find()` walks up to the root once, then rewrites every
  node it passed to point straight at the root, so the next lookup is one hop.
- **Union by rank** -- the shallower tree is attached under the deeper one, which
  stops long chains forming.

Together these give near-constant time per operation, so 62 million unions stay
tractable.

**4. Incremental runs load only the affected neighbourhood.** A nightly
incremental build does not reprocess the whole graph. `_run_subset_union_find`
collects the identifiers touched by new edges, looks up which clusters they
belong to, expands one hop to pull in every identifier of those clusters, and
runs union-find on that subset alone. Correctness holds because any cluster a new
edge could merge is loaded in full. This is the difference between roughly 4 GB
peak and 17+ GB.

Results go back out the same way they came in: assignments are uploaded to
BigQuery in chunks and joined there, rather than being held in Python and written
in one piece.

### Stage 3 -- publish

The identifier-to-`bn_id` mapping is published to `bn_id_xref`, the surface
everything downstream joins to. `profile_data.profile_core` is keyed on the same
`bn_id`, which is what lets the profile database say anything about a person at
all.

---

## 3. tier1 vs tier2 -- the distinction that matters most

Every cluster is graded, and confusing the two grades is the most common way to
produce a confidently wrong number.

| | Clusters | Identifiers | Per person |
|---|---:|---:|---:|
| **tier1** -- has a durable anchor | **795,482** | 3,451,781 | 4.34 |
| **tier2** -- cookie-only | 5,334,424 | 25,926,096 | 4.86 |

**tier1** clusters contain at least one identifier that survives a cache clear:
email, `bionews_uk` (SSO), `npi_number`, `mc_euid`. These are real people. You can
name them, segment them, and reach them.

**tier2** clusters are built only from cookies and device ids. They are closer to
*browsers* than to humans: the same person on a phone and a laptop, with no shared
anchor, is two tier2 clusters and nothing can merge them.

So when someone asks how big the audience is:

- **795,482** is the defensible "people" number.
- 6.13M is the cluster count, and quoting it as humans overstates reality by
  roughly 7x.

---

## 4. Confidence: three columns, one decision

`bn_id_hub` carries three confidence values and they are not interchangeable.

| Column | Meaning |
|---|---|
| `base_confidence` | one observation, set by connector config |
| `confidence` | aggregated across repeated observations (log2) |
| `effective_confidence` | `confidence` x time decay -- **this is the one that decides** |

The threshold is **0.80**. Below it the edge exists but does not stitch.

This is not academic. Measured 2026-08-18 on active edges:

| Match rule | Edges | Avg effective | % that stitch |
|---|---:|---:|---:|
| `LOCALSTORAGE_COOCCURRENCE` | 47,592,126 | 0.918 | 97.3% |
| `AIM_PAYLOAD_COOCCURRENCE` | 7,546,174 | 0.933 | 97.4% |
| `BIO_ACCEPTOR_FLAT` | 3,649,337 | 0.916 | 93.5% |
| **`DEVICE_STAT_ID`** | **2,348,536** | **0.204** | **0.0%** |
| `GA4_SESSION_COOCCURRENCE` | 1,293,222 | 0.921 | 98.6% |
| `EMAIL_EXACT` | 478,463 | 1.000 | 100.0% |
| `MC_EUID_BRIDGE` | 337,814 | 1.000 | 100.0% |
| `NPI_REGISTRY_EMAIL` | 305,883 | 0.999 | 99.8% |

Two things to take from that table.

`LOCALSTORAGE_COOCCURRENCE` alone is 47.6M of the ~64M edges, so it shapes the
graph more than every deterministic rule combined.

And `DEVICE_STAT_ID` writes 2.35M edges of which **none** stitch -- but that is
deliberate, not broken. Browser fingerprints are shared between people, so the
rule is capped below the threshold in three places at once: connector confidence
0.35, `confidence_caps_by_rule` 0.50, and `observation_to_identity_ratio` 0.50.
Maximum achievable effective confidence is 0.25 against a 0.80 threshold.

`IP_DEVICE_TIME` was the same pattern and followed the same path out: disabled
2026-08-20 and its 13 edges deleted (`688317c`). It was superseded by
`MC_CLICK_IP_TIME`, which covers the same Mailchimp-click inference with enough
confidence to actually stitch. Its cap stays in the config with a guard test, so
re-enabling it cannot silently start merging people.

**Disabled 2026-08-19**, and the 2,365,271 existing edges deleted. Verified
before and after at set level: not one `bn_id` was lost or created, and
`bn_id_xref`, `bn_id_neighbors`, `bn_id_persistence` and `profile_core` all held
identical row counts. That is the expected result -- a row that was never an
input to Union-Find cannot move a cluster boundary.

The connector config block is deliberately retained. `connect_compound_device_ip`
-- which DOES stitch -- recomputes the same fingerprint from `hash_fields` in
that block, so deleting it would silently fall back to hardcoded defaults.

Investigated 2026-08-18: nothing consumes those edges. They do not reach
`bn_id_neighbors` (0 rows -- non-stitching edges never co-cluster), no `stat_id`
is published to `bn_id_xref`, and the rule name appears exactly once in
`shared/identity_hub.py`, at the write. `connect_compound_device_ip` derives
`stat_id` from source independently rather than reading these edges. They are
readable only by querying `bn_id_hub` directly.

The general lesson: **a rule producing millions of edges is not necessarily
merging anyone**, and a rule merging nobody is not necessarily broken. Check the
caps before concluding either way.

---

## 5. bn_id is stable, not immutable

When evidence arrives showing two clusters are one person, they merge: one `bn_id`
survives, the other is retired.

- `bn_id_persistence` is the **redirect registry** -- 100,333 rows, every one a
  `MERGE`, mapping a retired id to its survivor.
- `bn_id_identity_changes` is the **full audit trail** -- 111,030 merges and
  157,721 splits.

**If you store a `bn_id` anywhere, you must resolve it through the registry before
using it**, or you will silently miss people whose id changed. Query 6 in the
cookbook does this safely.

Splits outnumber merges, which is worth internalising: identity is corrected in
both directions, not just accumulated.

---

## 6. Things that will bite you

1. **`identifier_value` is normalised on write.** Emails are lowercased and
   trimmed; `wp_user_id` is site-qualified as `{site}:{id}` because WordPress
   reuses numeric ids across sites. Join on the normalised form.
2. **`bn_id_hub` is append-only.** Retired edges have `is_active = FALSE`, not
   deletion. An unfiltered `COUNT(*)` counts history.
3. **`is_bot`, `is_suspicious`, `is_shared_workstation` are flags, not filters.**
   Nothing removes those rows for you. `is_suspicious` fires at health < 60.
4. **`bn_id_node_index` and `bn_id_metrics` hold every run.** Filter to
   `bn_id_manifest.active_run_id` or your counts multiply by the run count.
5. **`bn_id_neighbors` is 76.6M rows** -- the cross-product inside each cluster,
   growing quadratically with cluster size. Always filter by `bn_id`.
6. **`bn_id` in `bn_id_hub` is an output, not an input.** It is the cluster the
   edge landed in after union-find, not something the connector chose.
7. **Only an allowlist of `match_rule` values may feed acquisition dates.**
   `profile_core.source_created_at` uses `bn_id_hub.first_seen` from specific
   rules; the rest are pipeline-stamped at build time and would corrupt any
   growth chart. See `sql/enrich_source_created_at.sql`.

---

## 7. Query cost

The cookbook's 16 queries total ~41 GB if you run every one. `bn_id_hub` (64.4M
rows) and `bn_id_neighbors` (76.6M) are the expensive tables. The per-person
lookups are cheap because both are clustered by `bn_id` -- which is exactly why
filtering by `bn_id` first matters for cost as well as correctness.
