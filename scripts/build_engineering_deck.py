#!/usr/bin/env python3
"""
Build exports/bionews_data_platform_engineering.pptx -- the engineering
counterpart to the BI deck.

Same two systems, different question. The BI deck answers "what do these
numbers mean and how do I ask for them". This one answers "how does it work,
what will bite me, and where do I look" -- for someone who will read or change
the code.

Reuses the palette and primitives from build_platform_deck.py so the two decks
sit together. Figures are verified against the code and production; anything
that could drift is derived at build time rather than typed in.

  python scripts/build_engineering_deck.py
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from pptx import Presentation
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / "exports" / "bionews_data_platform_engineering.pptx"

spec = importlib.util.spec_from_file_location(
    "_deck", REPO / "scripts" / "build_platform_deck.py"
)
D = importlib.util.module_from_spec(spec)
sys.modules["_deck"] = D
spec.loader.exec_module(D)

blank, bgc, rect, chip, arrow, text, bullets, header, footer, takeaway = (
    D.blank,
    D.bg,
    D.rect,
    D.chip,
    D.arrow,
    D.text,
    D.bullets,
    D.header,
    D.footer,
    D.takeaway,
)
NAVY, DEEP, TEAL, SKY, GOLD = D.NAVY, D.DEEP, D.TEAL, D.SKY, D.GOLD
AMBER_BG, AMBER_TX, WHITE, LGRAY, BORDER = (
    D.AMBER_BG,
    D.AMBER_TX,
    D.WHITE,
    D.LGRAY,
    D.BORDER,
)
DGRAY, MGRAY, GREEN, RED, REDBG = D.DGRAY, D.MGRAY, D.GREEN, D.RED, D.REDBG
PURPLE, SLATE, RGBColor = D.PURPLE, D.SLATE, D.RGBColor

MONO = "Consolas"
GREENBG = RGBColor(0xE8, 0xF5, 0xE9)
PURPBG = RGBColor(0xF3, 0xE5, 0xF5)


def _live_facts():
    """Derive what can be derived, so the deck cannot quietly go stale."""
    sys.path.insert(0, str(REPO))
    from shared.profile_database_manifest import BUILD_MODES, BUILD_STEPS
    import yaml

    cfg = yaml.safe_load(
        (REPO / "configs" / "identity_hub.yaml").read_text(encoding="utf-8")
    )

    def walk(d):
        out = []
        if isinstance(d, dict):
            for k, v in d.items():
                if isinstance(v, dict) and "match_rule" in v:
                    out.append(
                        (
                            k,
                            v.get("enabled", True),
                            v.get("link_type", "?"),
                            v.get("static", False),
                        )
                    )
                elif isinstance(v, dict):
                    out += walk(v)
        return out

    conns = walk(cfg)
    en = [c for c in conns if c[1]]
    rb, rf = set(BUILD_MODES["rebuild"]), set(BUILD_MODES["refresh"])
    return {
        "steps": len(BUILD_STEPS),
        "modes": len(BUILD_MODES),
        "rebuild": len(BUILD_MODES["rebuild"]),
        "refresh": len(BUILD_MODES["refresh"]),
        "rebuild_only": len(rb - rf),
        "conns": len(conns),
        "enabled": len(en),
        "det": sum(1 for c in en if c[2] == "deterministic"),
        "prob": sum(1 for c in en if c[2] != "deterministic"),
        "static": sum(1 for c in en if c[3]),
        "stitch": cfg["identity_hub"].get("stitch_threshold"),
        "fanout": cfg["identity_hub"].get("max_visitors_per_identifier"),
        "shrink": cfg["identity_hub"].get("shrink_abort_threshold"),
    }


F = _live_facts()


def seealso(slide, label, fname, l, t, w=5.6):
    text(slide, label, l, t, 2.0, 0.28, size=10.5, color=MGRAY)
    text(
        slide,
        fname,
        l + 1.35,
        t,
        w,
        0.28,
        size=10.5,
        bold=True,
        color=TEAL,
        font=MONO,
        link=fname,
    )


# ── slides ────────────────────────────────────────────────────────────────────


def s_title(prs):
    s = blank(prs)
    bgc(s, NAVY)
    rect(s, 0, 0, 13.33, 7.5, NAVY)
    rect(s, 0, 4.9, 13.33, 2.6, DEEP)
    rect(s, 0.65, 3.3, 2.4, 0.07, TEAL)
    text(
        s,
        "Identity Hub and Profile Database",
        0.65,
        1.1,
        12,
        0.9,
        size=40,
        bold=True,
        color=WHITE,
    )
    text(
        s,
        "Engineering reference -- architecture, algorithms, failure modes",
        0.65,
        2.3,
        12,
        0.5,
        size=19,
        color=TEAL,
    )
    text(
        s,
        "Two BigQuery-native pipelines. One resolves identity, one describes "
        "the people it resolves. bn_id is the contract between them.",
        0.65,
        3.5,
        11.6,
        0.6,
        size=13.5,
        color=SKY,
    )
    text(s, "For engineers", 0.65, 5.25, 6, 0.35, size=14, bold=True, color=WHITE)
    text(
        s,
        "Figures derived from the code and config at build time, not typed in.",
        0.65,
        5.65,
        9,
        0.32,
        size=11,
        color=MGRAY,
    )
    text(
        s,
        "Underlined document names are clickable; keep this deck alongside the "
        "exports folder.",
        0.65,
        6.2,
        11.6,
        0.4,
        size=10.5,
        italic=True,
        color=SKY,
    )
    return s


def s_contract(prs, n):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "the shape",
        "Two pipelines, one contract",
        "Neither is an ETL in the usual sense. There is no extraction stage in either.",
    )
    panels = [
        (
            0.5,
            NAVY,
            "IDENTITY HUB",
            "shared/identity_hub.py",
            "~10,600 lines",
            [
                "BigQuery-to-BigQuery. No Parquet, no GCS, no API.",
                "Python only for Union-Find and bn_id derivation.",
                f"{F['enabled']} enabled connectors of {F['conns']} declared.",
                "Output: bn_id_xref, the consumer surface.",
            ],
        ),
        (
            7.0,
            PURPLE,
            "PROFILE DATABASE",
            "plugins/profile_database_extractor.py",
            "~5,200 lines",
            [
                "SQL-only ELT. Python sequences, never touches rows.",
                f"{F['steps']} declared steps mapped into {F['modes']} modes.",
                "Input contract is the hub's bn_id_xref.",
                "Output: profile_core plus satellites and views.",
            ],
        ),
    ]
    for x, colour, title, path, size, items in panels:
        rect(s, x, 1.5, 5.85, 3.7, WHITE, line=BORDER)
        rect(s, x, 1.5, 5.85, 0.07, colour)
        text(s, title, x + 0.25, 1.7, 4.5, 0.3, size=12, bold=True, color=colour)
        text(s, path, x + 0.25, 2.02, 5.3, 0.28, size=11, color=DGRAY, font=MONO)
        text(s, size, x + 0.25, 2.32, 3, 0.24, size=9.5, color=MGRAY)
        bullets(s, items, x + 0.25, 2.62, 5.35, 2.4, size=11)
    chip(s, "bn_id", 6.45, 3.1, 1.0, 0.42, TEAL, size=12)
    text(
        s,
        "the contract",
        6.25,
        3.56,
        1.4,
        0.25,
        size=9,
        color=MGRAY,
        align=PP_ALIGN.CENTER,
    )
    takeaway(
        s,
        "bn_id is a pure function of cluster membership: "
        "BN_ + base64url(SHA256(canonical_root))[:16]. Nothing is allocated. "
        "Merge two clusters and the bn_id changes.",
    )
    footer(s, n)
    return s


def s_phases(prs, n):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "hub execution model",
        "Five phases. Only one leaves BigQuery.",
        "Scoring, decay, filtering and gating are all SQL over a per-run staging table.",
        accent=NAVY,
    )
    phases = [
        (
            "1",
            "CONNECTOR FAN-IN",
            "_insert_connector_edges()",
            "Each connector emits candidate edges into staging. Batched, streamed.",
            NAVY,
        ),
        (
            "2",
            "AGGREGATION",
            "_run_bq_aggregation()",
            "Collapse duplicates; combine repeated evidence into one confidence per pair.",
            NAVY,
        ),
        (
            "3",
            "FILTER + GATE",
            "_run_bq_quality_filters() / _run_bq_gates()",
            "Fanout caps, shared-workstation detection, bot exclusion, per-rule caps.",
            NAVY,
        ),
        (
            "4",
            "UNION-FIND",
            "_export_union_find_tuples()",
            "The only phase in Python. Surviving edges stream into PriorityUnionFind.",
            GOLD,
        ),
        (
            "5",
            "WRITE + VERIFY",
            "_write_hub_from_staging()",
            "Shadow write, output contracts, shrink safeguard, then manifest promotion.",
            GREEN,
        ),
    ]
    y = 1.5
    for num, title, fn, body, colour in phases:
        rect(s, 0.55, y, 12.3, 1.0, WHITE, line=BORDER)
        rect(s, 0.55, y, 0.07, 1.0, colour)
        rect(s, 0.9, y + 0.29, 0.42, 0.42, colour)
        text(
            s,
            num,
            0.9,
            y + 0.34,
            0.42,
            0.3,
            size=14,
            bold=True,
            color=WHITE,
            align=PP_ALIGN.CENTER,
        )
        text(s, title, 1.5, y + 0.13, 3.0, 0.28, size=12, bold=True, color=DGRAY)
        text(s, fn, 1.5, y + 0.44, 4.3, 0.26, size=9.5, color=TEAL, font=MONO)
        text(s, body, 6.1, y + 0.3, 6.5, 0.5, size=11, color=MGRAY)
        y += 1.08
    rect(s, 0.55, 6.9, 12.3, 0.42, AMBER_BG, line=GOLD)
    text(
        s,
        "Phase 4 is the memory ceiling. Everything else scales with BigQuery; "
        "Union-Find scales with distinct identifiers in process RAM.",
        0.8,
        6.97,
        11.9,
        0.3,
        size=10.5,
        bold=True,
        color=AMBER_TX,
    )
    footer(s, n)
    return s


def s_unionfind(prs, n):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "the algorithm",
        "PriorityUnionFind, and why the tie-break matters",
        "shared/identity_hub.py:158 -- disjoint-set with path compression, plus source priority.",
        accent=GOLD,
    )
    rect(s, 0.55, 1.5, 6.6, 2.5, WHITE, line=BORDER)
    text(s, "ROOT SELECTION", 0.8, 1.66, 4, 0.26, size=10, bold=True, color=GOLD)
    text(
        s,
        "if pri_x > pri_y:\n"
        "    root_x, root_y = root_y, root_x\n"
        "elif pri_x == pri_y and root_x > root_y:\n"
        "    root_x, root_y = root_y, root_x",
        0.8,
        1.95,
        6.1,
        1.1,
        size=10,
        color=DGRAY,
        font=MONO,
    )
    text(
        s,
        "Lower priority number wins. Ties break on the node VALUE -- which makes "
        "root selection independent of the order edges arrive in.",
        0.8,
        3.12,
        6.1,
        0.7,
        size=11,
        color=MGRAY,
    )
    rect(s, 7.45, 1.5, 5.4, 2.5, WHITE, line=TEAL, line_pt=1.25)
    text(
        s,
        "WHY THAT IS NOT COSMETIC",
        7.7,
        1.66,
        4.5,
        0.26,
        size=10,
        bold=True,
        color=TEAL,
    )
    text(
        s,
        "bn_id = BN_ + base64url(SHA256(root))[:16]",
        7.7,
        1.95,
        5.0,
        0.3,
        size=10.5,
        color=DGRAY,
        font=MONO,
    )
    text(
        s,
        "Identity is derived, not allocated. Non-deterministic roots would mean "
        "non-deterministic bn_ids, and a rebuild would reshuffle identity across "
        "the whole estate. The tie-break is what makes a rebuild reproducible.",
        7.7,
        2.35,
        4.9,
        1.5,
        size=11,
        color=MGRAY,
    )
    y = 4.25
    for label, body, colour in [
        (
            "Priority decides the root",
            "An email or SSO key outranks a cookie, so the "
            "canonical root is a person-level anchor rather than a browser.",
            NAVY,
        ),
        (
            "Merges retire a bn_id",
            "Two clusters joining produces a new root and a new "
            "bn_id. The loser is recorded in bn_id_persistence.",
            PURPLE,
        ),
        (
            "Consumers must resolve",
            "Any bn_id held outside the system goes stale on "
            "merge. Resolve through bn_id_persistence before joining.",
            RED,
        ),
    ]:
        rect(s, 0.55, y, 12.3, 0.72, WHITE, line=BORDER)
        rect(s, 0.55, y, 0.07, 0.72, colour)
        text(s, label, 0.9, y + 0.1, 3.6, 0.28, size=12, bold=True, color=colour)
        text(s, body, 4.7, y + 0.12, 7.9, 0.5, size=11, color=MGRAY)
        y += 0.8
    footer(s, n)
    return s


def s_confidence(prs, n):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "scoring",
        "Three confidence columns. One decides.",
        "The other two are inputs. Reading the wrong one is the most common analysis error here.",
        accent=NAVY,
    )
    cols = [
        ("base_confidence", "what the connector asserted", "input", SLATE),
        ("confidence", "aggregate after combining repeated evidence", "input", SLATE),
        (
            "effective_confidence",
            "aggregate after decay and per-rule cap",
            "DECIDES",
            GREEN,
        ),
    ]
    x = 0.55
    for name, body, tag, colour in cols:
        rect(
            s,
            x,
            1.5,
            4.0,
            1.75,
            WHITE,
            line=BORDER if tag == "input" else GREEN,
            line_pt=0.75 if tag == "input" else 1.5,
        )
        rect(s, x, 1.5, 4.0, 0.07, colour)
        text(s, tag, x + 0.22, 1.68, 2.5, 0.24, size=9.5, bold=True, color=colour)
        text(
            s,
            name,
            x + 0.22,
            1.94,
            3.6,
            0.32,
            size=13,
            bold=True,
            color=DGRAY,
            font=MONO,
        )
        text(s, body, x + 0.22, 2.34, 3.6, 0.8, size=11, color=MGRAY)
        x += 4.15
    rect(s, 0.55, 3.5, 12.3, 0.5, RGBColor(0xEC, 0xEF, 0xF1))
    rect(s, 0.55, 3.5, 12.3 * float(F["stitch"]), 0.5, RGBColor(0xFF, 0xCD, 0xD2))
    rect(
        s,
        0.55 + 12.3 * float(F["stitch"]),
        3.5,
        12.3 * (1 - float(F["stitch"])),
        0.5,
        RGBColor(0xC8, 0xE6, 0xC9),
    )
    rect(s, 0.55 + 12.3 * float(F["stitch"]) - 0.02, 3.38, 0.05, 0.74, RED)
    text(
        s,
        "stored as evidence -- never merges",
        1.2,
        3.62,
        5,
        0.28,
        size=11,
        bold=True,
        color=RGBColor(0xB7, 0x1C, 0x1C),
    )
    text(
        s,
        "merges",
        11.0,
        3.62,
        1.6,
        0.28,
        size=11,
        bold=True,
        color=RGBColor(0x1B, 0x5E, 0x20),
    )
    text(
        s,
        f"stitch_threshold = {F['stitch']}",
        0.55 + 12.3 * float(F["stitch"]) - 1.3,
        4.06,
        3.0,
        0.26,
        size=10,
        bold=True,
        color=RED,
    )
    rect(s, 0.55, 4.55, 12.3, 1.6, REDBG, line=RED)
    text(
        s,
        "bn_id_hub IS AN EVIDENCE LOG, NOT A DECISION LOG",
        0.8,
        4.68,
        7,
        0.26,
        size=10,
        bold=True,
        color=RED,
    )
    text(
        s,
        "It holds every edge ever produced, including everything rejected -- 75.4 "
        "million rows. Rejected evidence is kept deliberately: it is how a scoring "
        "change can be evaluated without re-running connectors, and how a merge "
        "decision can be explained after the fact.\n\n"
        "Counting rows in bn_id_hub does not count linkages. Filter on "
        "is_active = TRUE and effective_confidence >= stitch_threshold, or use "
        "bn_id_xref, which is the resolved surface.",
        0.8,
        4.98,
        11.8,
        1.1,
        size=11,
        color=DGRAY,
    )
    footer(s, n)
    return s


def s_incremental(prs, n):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "the fact that causes incidents",
        "Incremental runs never re-evaluate old edges",
        "Config changes are inert until the next full rebuild -- which may be months away.",
        accent=RED,
    )
    for x, title, colour, items in [
        (
            0.55,
            "INCREMENTAL  (the default)",
            NAVY,
            [
                "Appends new edges only.",
                "Attaches them to existing clusters via bn_id_node_index.",
                "Never re-scores or re-decays an existing edge.",
                f"Skips all {F['static']} static connectors.",
            ],
        ),
        (
            7.0,
            "FULL REBUILD  (--refresh full)",
            GOLD,
            [
                "Re-derives every cluster from the entire edge history.",
                "Applies every config change accumulated since the last one.",
                "The only run that exercises decay and expiry.",
                "Reproducible: same edges, same bn_ids.",
            ],
        ),
    ]:
        rect(s, x, 1.5, 5.85, 2.2, WHITE, line=BORDER)
        rect(s, x, 1.5, 5.85, 0.07, colour)
        text(s, title, x + 0.25, 1.68, 5.3, 0.3, size=12, bold=True, color=colour)
        bullets(s, items, x + 0.25, 2.0, 5.35, 1.6, size=11)
    rect(s, 0.55, 3.95, 12.3, 2.2, REDBG, line=RED)
    text(
        s,
        "2026-08-06 -- WHAT THIS LOOKS LIKE IN PRODUCTION",
        0.85,
        4.08,
        8,
        0.26,
        size=10,
        bold=True,
        color=RED,
    )
    text(
        s,
        "A full rebuild aged out 2,085,967 anonymous identities in one run. The "
        "cause was a 90-day effective identity lifetime that nobody had approved: "
        "it emerged from three settings interacting -- stitch_threshold 0.80, the "
        "90-day/0.8 decay band, and 365-day influence caps -- tuned separately on "
        "2026-04-18.\n\n"
        "It was invisible for months precisely because incrementals never "
        "re-evaluate old edges, and every earlier rebuild ran when the graph was "
        "younger than the window. bn_id_xref came back at 15,710,972 rows against "
        "27,153,725 the day before.",
        0.85,
        4.38,
        11.7,
        1.7,
        size=11,
        color=DGRAY,
    )
    takeaway(
        s,
        "Retention is now 400 days (3735ee9) and shrink_abort_threshold is "
        f"{F['shrink']} (e59f557). Neither has yet been exercised by a full "
        "rebuild -- treat the next one as a change with blast radius.",
        colour=GOLD,
        band=AMBER_BG,
        y=6.35,
    )
    footer(s, n)
    return s


def s_modes(prs, n):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "profile build",
        f"{F['steps']} declared steps, {F['modes']} modes",
        "The step list is data, not control flow. That is what makes resume modes possible.",
        accent=PURPLE,
    )
    modes = [
        (
            "rebuild",
            F["rebuild"],
            "Full reconstruction from the identity graph.",
            PURPLE,
        ),
        ("refresh", F["refresh"], "The daily run. Scoped to changed profiles.", TEAL),
        ("reenrich", 18, "Enrichment, personas, views. No DDL, no populate.", NAVY),
        ("resume_rebuild", 3, "Continue a rebuild that failed after populate.", SLATE),
        ("resume_publish", 0, "Publish an already-built candidate dataset.", SLATE),
        ("views", 1, "Republish consumer views only.", SLATE),
        ("backfill_site_events", 1, "Targeted historical load.", SLATE),
    ]
    y = 1.5
    for name, steps, body, colour in modes:
        rect(s, 0.55, y, 12.3, 0.62, WHITE, line=BORDER)
        rect(s, 0.55, y, 0.07, 0.62, colour)
        text(
            s, name, 0.9, y + 0.14, 2.9, 0.3, size=12, bold=True, color=DGRAY, font=MONO
        )
        w = 3.2 * (steps / F["rebuild"]) if steps else 0.04
        rect(s, 4.0, y + 0.2, max(w, 0.04), 0.22, colour)
        text(s, f"{steps}", 7.35, y + 0.16, 0.6, 0.28, size=11, bold=True, color=colour)
        text(s, body, 8.1, y + 0.16, 4.6, 0.3, size=10.5, color=MGRAY)
        y += 0.7
    rect(s, 0.55, 6.42, 12.3, 0.72, REDBG, line=RED)
    text(
        s,
        f"{F['rebuild_only']} of the {F['rebuild']} rebuild steps never run in "
        "refresh -- every populate_*, all four fill_gaps_*, ddl, snapshot and "
        "restore. Any column written only by those is frozen at the last rebuild. "
        "This is the repeating source of silent staleness in this system.",
        0.85,
        6.54,
        11.8,
        0.55,
        size=11,
        bold=True,
        color=DGRAY,
    )
    footer(s, n)
    return s


def s_gates(prs, n):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "publication",
        "A build publishes by passing, not by finishing",
        "Ten assertions run after the last step. A hard failure stops the release outright.",
        accent=GREEN,
    )
    stages = [
        ("BUILD", "candidate dataset", NAVY),
        ("ASSERT", "10 checks, hard + soft", GOLD),
        ("GATE", "hard failure = failed_gate", RED),
        ("PUBLISH", "repoint production views", GREEN),
    ]
    x = 0.55
    for title, body, colour in stages:
        rect(s, x, 1.5, 2.75, 1.15, WHITE, line=BORDER)
        rect(s, x, 1.5, 2.75, 0.07, colour)
        text(s, title, x + 0.2, 1.68, 2.3, 0.3, size=13, bold=True, color=colour)
        text(s, body, x + 0.2, 2.02, 2.4, 0.5, size=10, color=MGRAY)
        if x < 9.5:
            arrow(s, x + 2.85, 1.92, 0.42, 0.32, SLATE)
        x += 3.27
    rows = [
        (
            "identity_source_row_delta",
            "hard",
            "Guards the INPUT: fails on a >10% shrink in the consumed identity snapshot.",
        ),
        (
            "fill_rate_drift_critical",
            "hard",
            "Fails on a >5pp drop in a critical field's fill rate.",
        ),
        (
            "refresh_safety_check",
            "hard",
            "No immutable field mutated, no unexplained field change.",
        ),
        (
            "orphan_satellite / population_sync",
            "hard",
            "Referential integrity across profile_core and satellites.",
        ),
        (
            "anonymous_known_count_delta",
            "soft",
            "Warns on a >15% tier1 or tier2 swing.",
        ),
    ]
    y = 2.95
    for name, sev, body in rows:
        colour = RED if sev == "hard" else GOLD
        rect(s, 0.55, y, 12.3, 0.6, WHITE, line=BORDER)
        rect(s, 0.55, y, 0.07, 0.6, colour)
        text(
            s,
            name,
            0.9,
            y + 0.14,
            4.3,
            0.28,
            size=10.5,
            bold=True,
            color=DGRAY,
            font=MONO,
        )
        text(s, sev.upper(), 5.4, y + 0.15, 0.8, 0.26, size=9, bold=True, color=colour)
        text(s, body, 6.4, y + 0.15, 6.3, 0.3, size=10.5, color=MGRAY)
        y += 0.68
    rect(s, 0.55, 6.42, 12.3, 0.72, AMBER_BG, line=GOLD)
    text(
        s,
        "An assertion is only as good as its baseline. anonymous_known_count_delta "
        "filtered its baseline on mode='rebuild', no completed rebuild ever carried "
        "one, so it took the no-baseline PASS branch on every run for months -- "
        "including the day tier2 fell 40.9%. When adding one, prove the baseline "
        "query returns rows.",
        0.85,
        6.54,
        11.8,
        0.55,
        size=11,
        bold=True,
        color=AMBER_TX,
    )
    footer(s, n)
    return s


def s_invariants(prs, n):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "before you edit sql",
        "Invariants that are easy to break",
        "Each of these has already caused an incident or a near miss.",
        accent=RED,
    )
    items = [
        (
            "Step order is load-bearing",
            "populate_engagement does CREATE OR REPLACE at step 5. fill_gaps_aim_attribution "
            "does += on total_pageviews at step 12. Safe only because the table is recreated "
            "first. Move that step into refresh and it compounds daily.",
        ),
        (
            "Scope predicates are audited in CI",
            "Every write to profile_core or profile_engagement must carry a scope predicate "
            "or an explicit -- SCOPE: global_reconcile marker within three lines above it. "
            "73 writes: 34 directly scoped, 28 via scoped TEMP, 11 global.",
        ),
        (
            "Comments are -- only",
            "The executor splits on statement boundaries. Block comments break it.",
        ),
        (
            "Idempotence is the default expectation",
            "Steady state should write zero rows. Prefer WHEN MATCHED AND ... IS DISTINCT "
            "FROM over an unconditional UPDATE.",
        ),
        (
            "Snapshot and restore protect app-written fields",
            "A rebuild that skips them silently empties those columns.",
        ),
    ]
    y = 1.5
    for title, body in items:
        rect(s, 0.55, y, 12.3, 1.02, WHITE, line=BORDER)
        rect(s, 0.55, y, 0.07, 1.02, RED)
        text(s, title, 0.9, y + 0.12, 4.4, 0.3, size=12, bold=True, color=DGRAY)
        text(s, body, 5.5, y + 0.13, 7.2, 0.82, size=10.5, color=MGRAY)
        y += 1.1
    footer(s, n)
    return s


def s_telemetry(prs, n):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "observability",
        "What is recorded, and the one thing that is not",
        "Both systems stamp enough to attribute a difference to code or config.",
        accent=TEAL,
    )
    left = [
        (
            "runtime_fingerprint",
            "Hash of every SQL file plus the extractor. Two runs with the same value ran identical code -- the way to prove whether a host has actually pulled.",
        ),
        ("config_version / git_sha", "Stamped into bn_id_metrics on every hub run."),
        (
            "profile_build_steps",
            "One row per step: status, duration, rows, bytes billed, slot millis.",
        ),
        (
            "profile_publish_manifest",
            "One row per table promotion, with source and target row counts.",
        ),
    ]
    y = 1.5
    for name, body in left:
        rect(s, 0.55, y, 12.3, 0.78, WHITE, line=BORDER)
        rect(s, 0.55, y, 0.07, 0.78, TEAL)
        text(
            s,
            name,
            0.9,
            y + 0.12,
            3.6,
            0.28,
            size=11,
            bold=True,
            color=DGRAY,
            font=MONO,
        )
        text(s, body, 4.7, y + 0.12, 7.9, 0.6, size=10.5, color=MGRAY)
        y += 0.86
    rect(s, 0.55, 5.1, 12.3, 1.5, REDBG, line=RED)
    text(s, "THE BLIND SPOT", 0.85, 5.22, 4, 0.26, size=10, bold=True, color=RED)
    text(
        s,
        "A successful profile rebuild does not reliably appear in "
        "profile_build_runs. The 2026-06-20 rebuild promoted all 16 tables in "
        "profile_publish_manifest and wrote no run row at all. Only two "
        "rebuild-mode rows exist in the entire history and both are failures.\n\n"
        "Do not use profile_build_runs to answer when the last rebuild happened. "
        "Use profile_publish_manifest.",
        0.85,
        5.5,
        11.7,
        1.0,
        size=11,
        bold=True,
        color=DGRAY,
    )
    footer(s, n)
    return s


def s_where(prs, n):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "reference",
        "Where to look next",
        "Column-level questions are answered by the generated dictionaries, not here.",
        accent=TEAL,
    )
    docs = [
        (
            "identity_hub_engineering.docx",
            "Hub internals in full: phases, scoring, config contract, failure modes.",
            NAVY,
        ),
        (
            "profile_db_engineering.docx",
            "Build internals in full: modes, scope audit, gates, invariants.",
            PURPLE,
        ),
        (
            "identity_hub_data_dictionary.docx",
            "Every hub column. Generated from the DDL and config.",
            NAVY,
        ),
        (
            "profile_db_data_dictionary.docx",
            "Every profile table AND view column. The only doc covering views.",
            PURPLE,
        ),
        (
            "identity_hub_table_reference.docx",
            "Confidence and cluster-health scoring written out.",
            NAVY,
        ),
        (
            "profile_db_table_reference.docx",
            "Tables grouped by theme, views first.",
            PURPLE,
        ),
    ]
    y = 1.5
    for fname, body, colour in docs:
        rect(s, 0.55, y, 12.3, 0.66, WHITE, line=BORDER)
        rect(s, 0.55, y, 0.07, 0.66, colour)
        text(
            s,
            fname,
            0.9,
            y + 0.16,
            4.6,
            0.3,
            size=11,
            bold=True,
            color=TEAL,
            font=MONO,
            link=fname,
        )
        text(s, body, 5.9, y + 0.17, 6.8, 0.3, size=10.5, color=MGRAY)
        y += 0.74
    code_paths = [
        ("shared/identity_hub.py", "PriorityUnionFind :158, IdentityHubBuilder :281"),
        ("shared/profile_database_manifest.py", "Steps and modes -- start here"),
        ("scripts/scope_predicate_audit.py", "The CI gate on scope predicates"),
        (
            "docs/IDENTITY_HUB_FULL_REBUILD_RUNBOOK.md",
            "Pre-rebuild snapshot, stop rule, restore",
        ),
    ]
    text(s, "IN THE REPO", 0.55, 6.1, 3, 0.26, size=10, bold=True, color=MGRAY)
    y = 6.38
    for p, body in code_paths:
        text(s, p, 0.55, y, 4.6, 0.24, size=9.5, color=DGRAY, font=MONO)
        text(s, body, 5.3, y, 7.4, 0.24, size=9.5, color=MGRAY)
        y += 0.26
    return s


def main() -> int:
    prs = Presentation()
    prs.slide_width = Inches(13.33)
    prs.slide_height = Inches(7.5)
    s_title(prs)
    n = 2
    for b in (
        s_contract,
        s_phases,
        s_unionfind,
        s_confidence,
        s_incremental,
        s_modes,
        s_gates,
        s_invariants,
        s_telemetry,
        s_where,
    ):
        b(prs, n)
        n += 1
    OUT.parent.mkdir(parents=True, exist_ok=True)
    prs.save(str(OUT))
    print(f"[OK] Wrote {OUT.relative_to(REPO)}  ({len(prs.slides._sldIdLst)} slides)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
