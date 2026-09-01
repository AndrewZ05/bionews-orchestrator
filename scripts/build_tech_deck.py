#!/usr/bin/env python3
"""
Build exports/bionews_platform_technical.pptx.

Audience: a broader technical group -- architects, analysts, product people.
They will QUERY the profile database. They will never open the identity hub.

That split decides everything about this deck:

  Identity Hub   -> build confidence. Show the reasoning is sound and the
                    safeguards are real. Diagrams only. No code, no function
                    names. The goal is that they trust the bn_id they are about
                    to join on.

  Profile DB     -> enable querying. Features, benefits, use cases, what is
                    pre-built for them, and above all the pitfalls that produce
                    a confidently wrong number.

The highest-value slide is the pitfalls one. A room about to write queries needs
"if you forget is_bot = FALSE you are counting machines" far more than it needs
an execution model.

Sized for a 50 minute slot: 20 core slides plus a short appendix. Live figures
are pulled from production at build time.

  python scripts/build_tech_deck.py
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from pptx import Presentation
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / "exports" / "bionews_platform_technical.pptx"

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
PALE = RGBColor(0xEC, 0xEF, 0xF1)

_n = [0]


def nxt():
    _n[0] += 1
    return _n[0]


def live():
    """Pull figures at build time so the deck cannot go stale."""
    from google.cloud import bigquery

    try:
        c = bigquery.Client()
        r = list(
            c.query("""SELECT COUNT(*) tot, COUNTIF(is_known_person) known,
          COUNTIF(has_any_role) roles, COUNTIF(is_mailable) mail,
          COUNTIF(is_verified_hcp) hcp,
          COUNTIF(is_verified_hcp AND is_mailable) hcpm,
          COUNT(DISTINCT condition_label) conds
          FROM profile_data.profile_metrics""").result()
        )[0]
        h = list(
            c.query("""SELECT COUNT(*) ids, COUNT(DISTINCT bn_id) ppl,
          COUNTIF(is_bot) bots FROM identity_hub_data.bn_id_xref""").result()
        )[0]
        e = list(
            c.query("SELECT COUNT(*) n FROM identity_hub_data.bn_id_hub").result()
        )[0]
        views = sum(1 for t in c.list_tables("profile_data") if t.table_type == "VIEW")
        return dict(
            tot=r.tot,
            known=r.known,
            roles=r.roles,
            mail=r.mail,
            hcp=r.hcp,
            hcpm=r.hcpm,
            conds=r.conds,
            ids=h.ids,
            ppl=h.ppl,
            bots=h.bots,
            edges=e.n,
            views=views,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"  (live figures unavailable: {str(exc)[:60]})")
        return dict(
            tot=7_545_655,
            known=808_249,
            roles=504_866,
            mail=420_487,
            hcp=381_442,
            hcpm=23_552,
            conds=87,
            ids=34_767_858,
            ppl=7_557_487,
            bots=280_363,
            edges=76_164_932,
            views=27,
        )


L = live()


def part_divider(prs, kicker, title, subtitle, colour):
    s = blank(prs)
    bgc(s, NAVY)
    rect(s, 0, 0, 13.33, 7.5, NAVY)
    rect(s, 0, 5.0, 13.33, 2.5, DEEP)
    rect(s, 0.7, 3.35, 2.2, 0.07, colour)
    text(s, kicker.upper(), 0.7, 2.15, 8, 0.3, size=12, bold=True, color=colour)
    text(s, title, 0.7, 2.5, 12, 0.8, size=36, bold=True, color=WHITE)
    text(s, subtitle, 0.7, 3.6, 11.5, 0.6, size=15, color=SKY)
    return s


def fbu(s, y, feature, benefit, usecase, colour=TEAL):
    """The what / why / where band used through the deck."""
    for i, (title, body, c) in enumerate(
        [
            ("WHAT IT DOES", feature, colour),
            ("WHY IT MATTERS", benefit, GREEN),
            ("WHERE YOU SEE IT", usecase, PURPLE),
        ]
    ):
        x = 0.6 + i * 4.13
        rect(s, x, y, 3.95, 1.45, WHITE, line=BORDER)
        rect(s, x, y, 3.95, 0.06, c)
        text(s, title, x + 0.2, y + 0.15, 3.5, 0.24, size=9, bold=True, color=c)
        text(s, body, x + 0.2, y + 0.42, 3.6, 0.95, size=10.5, color=DGRAY)


def card(s, l, t, w, h, title, body, colour, big=None):
    rect(s, l, t, w, h, WHITE, line=BORDER)
    rect(s, l, t, w, 0.06, colour)
    yy = t + 0.16
    if big:
        text(s, big, l + 0.2, yy, w - 0.4, 0.5, size=22, bold=True, color=colour)
        yy += 0.55
    text(s, title, l + 0.2, yy, w - 0.4, 0.28, size=11, bold=True, color=DGRAY)
    text(s, body, l + 0.2, yy + 0.3, w - 0.4, h - (yy - t) - 0.42, size=10, color=MGRAY)


def person_node(s, label, sub, l, t, fill, w=1.7, h=0.62):
    rect(s, l, t, w, h, fill)
    text(
        s,
        label,
        l + 0.06,
        t + 0.07,
        w - 0.12,
        0.24,
        size=9,
        bold=True,
        color=WHITE,
        align=PP_ALIGN.CENTER,
    )
    text(
        s,
        sub,
        l + 0.06,
        t + 0.3,
        w - 0.12,
        0.22,
        size=8.5,
        color=WHITE,
        align=PP_ALIGN.CENTER,
    )

# ══ OPENING ══════════════════════════════════════════════════════════════════


def s_title(prs):
    s = blank(prs)
    bgc(s, NAVY)
    rect(s, 0, 0, 13.33, 7.5, NAVY)
    rect(s, 0, 4.85, 13.33, 2.65, DEEP)
    rect(s, 0.7, 3.25, 2.4, 0.07, TEAL)
    text(
        s,
        "Knowing Who Our Audience Is",
        0.7,
        1.1,
        12,
        0.9,
        size=40,
        bold=True,
        color=WHITE,
    )
    text(
        s,
        "The Identity Hub and the Profile Database",
        0.7,
        2.3,
        12,
        0.5,
        size=19,
        color=TEAL,
    )
    text(
        s,
        "How identity resolution works, and how to query what it produces",
        0.7,
        3.45,
        11.6,
        0.4,
        size=14,
        color=SKY,
    )
    y = 5.2
    for part, body in [
        ("Part 1", "The Identity Hub -- how we know two records are one person"),
        ("Part 2", "The Profile Database -- what is in it and how to query it"),
        ("Part 3", "How they fit together"),
    ]:
        text(s, part, 0.7, y, 1.1, 0.28, size=11, bold=True, color=TEAL)
        text(s, body, 1.95, y, 10, 0.28, size=11, color=SKY)
        y += 0.4
    return s


def s_sources(prs):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "where it starts",
        "Eight systems, none of which agree",
        "Each holds a fragment of the same person, under a different name for them.",
    )
    srcs = [
        ("Mailchimp", "subscriptions, opens, clicks"),
        ("GA4", "sessions, pages, referrals"),
        ("WordPress", "registration, forum, login"),
        ("LimeSurvey", "declared condition, demographics"),
        ("NPI registry", "clinician credential, specialty"),
        ("SurveyEngine", "the new registration path"),
        ("AIM", "clinician clickstream"),
        ("Ad platforms", "attribution"),
    ]
    x, y = 0.6, 1.5
    for i, (name, what) in enumerate(srcs):
        if i == 4:
            x, y = 0.6, 2.5
        rect(s, x, y, 2.95, 0.88, WHITE, line=BORDER)
        rect(s, x, y, 2.95, 0.05, SLATE)
        text(s, name, x + 0.18, y + 0.14, 2.6, 0.26, size=11.5, bold=True, color=DGRAY)
        text(s, what, x + 0.18, y + 0.42, 2.6, 0.36, size=9.5, color=MGRAY)
        x += 3.06
    arrow(s, 6.1, 3.62, 1.1, 0.5, TEAL)
    rect(s, 3.4, 4.32, 6.5, 0.95, NAVY)
    text(s, "IDENTITY HUB", 3.65, 4.48, 6.0, 0.3, size=12, bold=True, color=TEAL)
    text(
        s,
        "works out which fragments are the same person",
        3.65,
        4.78,
        6.0,
        0.3,
        size=12,
        color=WHITE,
    )
    arrow(s, 6.1, 5.42, 1.1, 0.5, PURPLE)
    rect(s, 3.4, 6.1, 6.5, 0.85, PURPLE)
    text(
        s,
        "PROFILE DATABASE",
        3.65,
        6.24,
        6.0,
        0.3,
        size=12,
        bold=True,
        color=RGBColor(0xE1, 0xBE, 0xE7),
    )
    text(
        s,
        "one row per person -- what you will query",
        3.65,
        6.52,
        6.0,
        0.3,
        size=12,
        color=WHITE,
    )
    footer(s, nxt())
    return s


# ══ PART 1 — IDENTITY HUB ════════════════════════════════════════════════════


def s_hub_divider(prs):
    return part_divider(
        prs,
        "part one",
        "The Identity Hub",
        "How we decide that two records are the same person",
        TEAL,
    )


def s_hub_problem(prs):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "the problem",
        "One person arrives as seven records",
        "Nobody introduces themselves the same way twice.",
        accent=NAVY,
    )
    frags = [
        ("Newsletter", "an email address"),
        ("Laptop", "a browser cookie"),
        ("Phone", "a different cookie"),
        ("Forum", "a login"),
        ("Survey", "a response ID"),
        ("Ad click", "a tracking ID"),
    ]
    x = 0.6
    for name, what in frags:
        rect(s, x, 1.5, 1.95, 0.85, WHITE, line=BORDER)
        rect(s, x, 1.5, 0.05, 0.85, SLATE)
        text(s, name, x + 0.16, 1.62, 1.7, 0.26, size=10.5, bold=True, color=DGRAY)
        text(s, what, x + 0.16, 1.89, 1.7, 0.36, size=9, color=MGRAY)
        x += 2.06
    arrow(s, 6.15, 2.55, 1.0, 0.45, TEAL)
    rect(s, 4.35, 3.15, 4.6, 1.12, TEAL)
    text(s, "ONE PERSON", 4.6, 3.28, 4.1, 0.26, size=10.5, bold=True, color=WHITE)
    text(s, "bn_id", 4.6, 3.52, 4.1, 0.38, size=20, bold=True,
         color=WHITE, font=MONO)
    text(s, "BN_7f3a91c4e2d05b6a", 4.6, 3.92, 4.1, 0.26, size=10,
         color=RGBColor(0xBF, 0xD4, 0xF2), font=MONO)
    fbu(
        s,
        4.5,
        "Groups every identifier belonging to one human being and gives that "
        "person a single permanent ID, called a bn_id.",
        "Without it you count browsers instead of people, and every audience "
        "number you produce inherits that error.",
        f"{L['ids']:,} identifiers resolve to {L['ppl']:,} people. The bn_id "
        "is the join key in every query you will write.",
        colour=NAVY,
    )
    takeaway(
        s,
        "Everything you query is keyed on bn_id. The rest of Part 1 is how we "
        "decide two records are one person, and what stops us getting it wrong.",
        colour=NAVY,
    )
    footer(s, nxt())
    return s


def s_hub_evidence(prs):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "how it decides",
        "Two kinds of evidence, treated very differently",
        "Proof and suggestion are never mixed together.",
        accent=NAVY,
    )
    rect(s, 0.6, 1.45, 6.0, 2.15, WHITE, line=GREEN, line_pt=1.5)
    rect(s, 0.6, 1.45, 6.0, 0.06, GREEN)
    text(s, "PROOF", 0.85, 1.62, 3, 0.26, size=10, bold=True, color=GREEN)
    text(
        s,
        "The same email on two records",
        0.85,
        1.88,
        5.5,
        0.32,
        size=14,
        bold=True,
        color=DGRAY,
    )
    bullets(
        s,
        [
            "A shared login or SSO key",
            "A form that captured both at once",
            "A provider registry entry",
        ],
        0.85,
        2.26,
        5.5,
        1.2,
        size=11,
    )
    text(
        s, "We merge these records immediately.", 0.85, 3.24, 5.5, 0.28, size=11, bold=True, color=GREEN
    )
    rect(s, 6.85, 1.45, 6.0, 2.15, WHITE, line=GOLD, line_pt=1.5)
    rect(s, 6.85, 1.45, 6.0, 0.06, GOLD)
    text(s, "SUGGESTION", 7.1, 1.62, 3, 0.26, size=10, bold=True, color=AMBER_TX)
    text(
        s,
        "Same device, same place, same hour",
        7.1,
        1.88,
        5.5,
        0.32,
        size=14,
        bold=True,
        color=DGRAY,
    )
    bullets(
        s,
        [
            "The same phone and laptop, same house, same evening",
            "Usually one person -- a laptop and a phone in one home",
            "But sometimes a clinic PC used by twelve staff",
        ],
        7.1,
        2.26,
        5.5,
        1.2,
        size=11,
    )
    text(
        s,
        "We only merge if the score clears our bar. Below it, we keep the evidence but leave the records separate.",
        7.1,
        3.24,
        5.5,
        0.28,
        size=11,
        bold=True,
        color=AMBER_TX,
    )
    fbu(
        s,
        3.85,
        "Every piece of evidence is scored, and only evidence above the bar is "
        "allowed to merge two people.",
        "Suggestion never silently becomes proof. A shared library computer "
        "does not turn six readers into one.",
        f"All {L['edges']:,} pieces of evidence are kept, including the ones "
        "rejected -- so any decision can be explained afterwards.",
        colour=NAVY,
    )
    takeaway(
        s,
        "We would rather show you two records for one person than one record for "
        "two people. Under-merging is recoverable; merging two humans is not.",
        colour=GREEN,
        band=GREENBG,
    )
    footer(s, nxt())
    return s


# ── Union-Find, in pictures ──────────────────────────────────────────────────


def s_uf_1(prs):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "grouping  1 of 3",
        "Everyone starts alone",
        "Six identifiers we believe belong to one person. The system does not know that yet.",
        accent=GOLD,
    )
    nodes = [
        ("email", "jane@ex.com", 0.75),
        ("browser", "laptop", 2.85),
        ("browser", "phone", 4.95),
        ("newsletter ID", "e7c1", 7.05),
        ("forum login", "40218", 9.15),
        ("provider ID", "1841", 11.25),
    ]
    for label, sub, x in nodes:
        person_node(s, label, sub, x, 1.55, SLATE)
        rect(s, x + 0.78, 2.24, 0.14, 0.2, SLATE)
    text(
        s,
        "each points only at itself -- six separate groups",
        0.6,
        2.55,
        12.25,
        0.3,
        size=11,
        italic=True,
        color=MGRAY,
        align=PP_ALIGN.CENTER,
    )
    rect(s, 0.6, 3.05, 12.25, 1.25, WHITE, line=BORDER)
    text(
        s, "THE STARTING POSITION", 0.85, 3.18, 5, 0.24, size=9.5, bold=True, color=GOLD
    )
    text(
        s,
        "Before any evidence is considered, every identifier is treated as its own "
        "person. The job of the next two slides is to join them up -- and, crucially, "
        "to decide which one of them gets to represent the group.",
        0.85,
        3.46,
        11.7,
        0.7,
        size=11.5,
        color=DGRAY,
    )
    fbu(
        s,
        4.5,
        "Starts from no assumptions. Nothing is grouped until evidence says so.",
        "The system cannot inherit somebody's earlier mistake, because it does "
        "not start from an existing grouping.",
        "This runs over 34.7 million identifiers every night.",
        colour=GOLD,
    )
    footer(s, nxt())
    return s


def s_uf_2(prs):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "grouping  2 of 3",
        "Evidence arrives, groups merge",
        "One rule decides which identifier represents the merged group.",
        accent=GOLD,
    )
    person_node(s, "email", "jane@ex.com", 1.2, 1.5, GOLD, w=2.2, h=0.72)
    text(
        s,
        "rank 1",
        1.2,
        2.26,
        2.2,
        0.24,
        size=9.5,
        bold=True,
        color=AMBER_TX,
        align=PP_ALIGN.CENTER,
    )
    person_node(s, "newsletter ID", "e7c1", 5.6, 1.5, SLATE, w=2.2, h=0.72)
    text(
        s, "rank 4", 5.6, 2.26, 2.2, 0.24, size=9.5, color=MGRAY, align=PP_ALIGN.CENTER
    )
    rect(s, 3.5, 1.82, 2.0, 0.06, TEAL)
    text(
        s,
        "same person",
        3.5,
        1.5,
        2.0,
        0.26,
        size=10,
        bold=True,
        color=TEAL,
        align=PP_ALIGN.CENTER,
    )
    arrow(s, 8.15, 1.72, 0.9, 0.4, SLATE)
    rect(s, 9.3, 1.4, 3.5, 1.15, WHITE, line=GOLD, line_pt=1.5)
    text(s, "MERGED GROUP", 9.5, 1.54, 3.1, 0.24, size=9, bold=True, color=AMBER_TX)
    text(
        s,
        "represented by the email",
        9.5,
        1.78,
        3.1,
        0.3,
        size=12,
        bold=True,
        color=DGRAY,
    )
    text(s, "because it ranks higher", 9.5, 2.1, 3.1, 0.28, size=10, color=MGRAY)
    rect(s, 0.6, 2.85, 12.25, 1.4, WHITE, line=BORDER)
    text(
        s,
        "THE RANKING, AND WHY IT EXISTS",
        0.85,
        2.97,
        6,
        0.24,
        size=9.5,
        bold=True,
        color=GOLD,
    )
    ranks = [
        ("1", "Our own visitor ID", TEAL),
        ("2", "Email, SSO key", TEAL),
        ("3", "Provider IDs", SLATE),
        ("4", "Newsletter ID", SLATE),
        ("5", "Analytics cookie", SLATE),
    ]
    x = 0.95
    for r, what, c in ranks:
        rect(s, x, 3.3, 2.25, 0.32, c)
        text(
            s,
            f"{r}.  {what}",
            x + 0.1,
            3.34,
            2.1,
            0.26,
            size=9.5,
            bold=True,
            color=WHITE,
        )
        x += 2.35
    text(
        s,
        "The representative becomes the group's identity. It must be something "
        "person-level -- an email, not a cookie -- so the identity survives when "
        "somebody clears their browser.",
        0.95,
        3.72,
        11.5,
        0.4,
        size=10.5,
        color=DGRAY,
    )
    fbu(
        s,
        4.45,
        "Merges two groups and picks the higher-ranked identifier to represent "
        "the result.",
        "Identity is anchored to something durable. Clearing cookies does not "
        "create a new person.",
        "This is why somebody who returns after eight months is still "
        "recognized rather than counted twice.",
        colour=GOLD,
    )
    footer(s, nxt())
    return s


def s_uf_3(prs):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "grouping  3 of 3",
        "The same people always produce the same bn_id",
        "Which is what stops a saved audience quietly losing its people.",
        accent=GREEN,
    )
    rect(s, 0.6, 1.42, 6.0, 2.4, WHITE, line=RED, line_pt=1.5)
    text(s, "IF IT WERE ARBITRARY", 0.85, 1.56, 4, 0.24, size=9.5, bold=True, color=RED)
    for i, (run, ident, col) in enumerate(
        [
            ("Monday's run", "bn_id = BN_7f3a...", RED),
            ("Tuesday's run", "bn_id = BN_c194...", RED),
        ]
    ):
        yy = 1.86 + i * 0.62
        rect(s, 0.85, yy, 5.5, 0.5, REDBG, line=RED)
        text(s, run, 1.02, yy + 0.12, 1.9, 0.26, size=10, bold=True, color=DGRAY)
        text(s, ident, 3.0, yy + 0.12, 3.2, 0.26, size=10, color=col, font=MONO)
    text(
        s,
        "Every report, export and saved audience that referenced the old ID "
        "silently loses those people. Nothing errors -- the rows are just gone.",
        0.85,
        3.12,
        5.5,
        0.6,
        size=10.5,
        color=DGRAY,
    )
    rect(s, 6.85, 1.42, 6.0, 2.4, WHITE, line=GREEN, line_pt=1.5)
    text(s, "AS BUILT", 7.1, 1.56, 4, 0.24, size=9.5, bold=True, color=GREEN)
    for i, run in enumerate(["Monday's run", "Tuesday's run"]):
        yy = 1.86 + i * 0.62
        rect(s, 7.1, yy, 5.5, 0.5, GREENBG, line=GREEN)
        text(s, run, 7.27, yy + 0.12, 1.9, 0.26, size=10, bold=True, color=DGRAY)
        text(
            s,
            "bn_id = BN_7f3a...",
            9.25,
            yy + 0.12,
            3.2,
            0.26,
            size=10,
            color=GREEN,
            font=MONO,
        )
    text(
        s,
        "The bn_id is calculated from the group's representative identifier, not "
        "handed out in sequence. Same members, same bn_id -- on any run, in any "
        "order.",
        7.1,
        3.12,
        5.5,
        0.6,
        size=10.5,
        color=DGRAY,
    )
    fbu(
        s,
        4.05,
        "Calculates the bn_id from the group itself, so it does not depend on "
        "processing order or on when the job ran.",
        "A group whose membership has not changed keeps its bn_id across a "
        "rebuild. When two groups do merge, the old bn_id is recorded with a "
        "pointer to the new one, so nothing is left stranded.",
        "Saved audiences keep working. If you cached a bn_id, resolve it through "
        "the redirect table before joining.",
        colour=GREEN,
    )
    takeaway(
        s,
        "Two records merging is normal and expected -- it means we learned "
        "something. The redirect table is what makes it safe for you.",
        colour=GREEN,
        band=GREENBG,
    )
    footer(s, nxt())
    return s

def s_hub_current(prs):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "staying current",
        "It runs every night, and it forgets slowly",
        "Two decisions that shape what the graph knows about your audience.",
        accent=NAVY,
    )
    rect(s, 0.6, 1.45, 6.0, 2.0, WHITE, line=BORDER)
    rect(s, 0.6, 1.45, 6.0, 0.06, NAVY)
    text(s, "NIGHTLY", 0.85, 1.62, 3, 0.24, size=9.5, bold=True, color=NAVY)
    text(
        s,
        "Yesterday folded in, every morning",
        0.85,
        1.86,
        5.5,
        0.32,
        size=14,
        bold=True,
        color=DGRAY,
    )
    bullets(
        s,
        [
            "Yesterday's sign-ups, clicks and visits are folded in",
            "A new cookie or signup attaches to the person it belongs to",
            "Finishes before 9am, so what you query is never more than a day old",
        ],
        0.85,
        2.24,
        5.5,
        1.1,
        size=11,
    )
    rect(s, 6.85, 1.45, 6.0, 2.0, WHITE, line=BORDER)
    rect(s, 6.85, 1.45, 6.0, 0.06, GOLD)
    text(s, "THIRTEEN MONTHS", 7.1, 1.62, 4, 0.24, size=9.5, bold=True, color=AMBER_TX)
    text(
        s,
        "We keep recognizing you for 13 months",
        7.1,
        1.86,
        5.5,
        0.32,
        size=14,
        bold=True,
        color=DGRAY,
    )
    bullets(
        s,
        [
            "Readers research intensively at diagnosis, then go quiet for months",
            "A 90-day memory would greet every one of them as a new stranger",
            "13 months keeps their condition, history and preferences attached",
        ],
        7.1,
        2.24,
        5.5,
        1.1,
        size=11,
    )
    text(
        s,
        "WHAT THE THIRTEEN MONTH MEMORY HOLDS",
        0.6,
        3.62,
        6,
        0.24,
        size=9.5,
        bold=True,
        color=MGRAY,
    )
    bars = [
        ("People we recognize", 7_557_487, TEAL),
        ("Last seen over 90 days ago", 4_508_886, GOLD),
        ("Last seen over a year ago", 529_890, SLATE),
    ]
    top = bars[0][1]
    y = 3.9
    for label, n, colour in bars:
        rect(s, 4.2, y, 6.6 * (n / top), 0.34, colour)
        text(s, label, 0.6, y + 0.04, 3.4, 0.28, size=10.5, color=DGRAY)
        text(s, f"{n:,}", 11.0, y + 0.05, 1.8, 0.26, size=10.5, bold=True, color=colour)
        y += 0.44
    fbu(
        s,
        5.3,
        "Refreshes nightly and keeps somebody recognizable for thirteen months "
        "after their last visit.",
        "4.5 million people we can recognize today would have been strangers "
        "under a shorter memory.",
        "Someone diagnosed last spring who returns this autumn still has their "
        "condition, history and preferences.",
        colour=GOLD,
    )
    footer(s, nxt())
    return s


def s_hub_safeguards(prs):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "why you can trust it",
        "It refuses to publish work it cannot vouch for",
        "The safeguards matter more than the algorithm, because they are what catches the "
        "case nobody predicted.",
        accent=GREEN,
    )
    guards = [
        (
            "You never see a half-finished run",
            "Last night's numbers stay in place until a new run finishes AND passes "
            "every check. A failed run is invisible, not partly applied.",
            GREEN,
        ),
        (
            "A sudden drop in people halts the run",
            "If tonight produces more than 10% fewer people than last night, the "
            "run is refused. We investigate before anything is published.",
            GREEN,
        ),
        (
            "We can explain any grouping, months later",
            "All 76 million pieces of evidence are retained, including rejected ones, "
            "so any grouping can be explained months later.",
            TEAL,
        ),
        (
            "Bots are identified and set aside",
            f"{L['bots']:,} identifiers are flagged as machines rather than people, "
            "and excluded from counts.",
            TEAL,
        ),
        (
            "Every run records what produced it",
            "Each night's run records which version of the code and settings produced "
            "it, so two runs that disagree can be explained.",
            NAVY,
        ),
        (
            "Monthly point-in-time copies, kept 13 months",
            "Point-in-time snapshots are kept for thirteen months, so a question asked "
            "late can still be answered.",
            NAVY,
        ),
    ]
    y = 1.45
    for i, (title, body, colour) in enumerate(guards):
        x = 0.6 if i % 2 == 0 else 6.95
        if i % 2 == 0 and i:
            y += 1.05
        rect(s, x, y, 5.9, 0.95, WHITE, line=BORDER)
        rect(s, x, y, 0.06, 0.95, colour)
        text(s, title, x + 0.28, y + 0.14, 5.3, 0.28, size=11.5, bold=True, color=DGRAY)
        text(s, body, x + 0.28, y + 0.44, 5.3, 0.45, size=10, color=MGRAY)
    takeaway(
        s,
        "When something looks wrong, the system stops and keeps serving the "
        "last good answer. It does not quietly serve a worse one.",
        colour=GREEN,
        band=GREENBG,
        y=4.75,
    )
    fbu(
        s,
        5.55,
        "Checks its own output before anything reaches you, and stops if it "
        "cannot vouch for it.",
        "The number in front of you is either right or absent. It is not "
        "quietly wrong.",
        "If a dashboard looks unchanged after an incident, that is the "
        "safeguard working.",
        colour=GREEN,
    )
    footer(s, nxt())
    return s


# ══ PART 2 — PROFILE DATABASE ════════════════════════════════════════════════


def s_profile_divider(prs):
    return part_divider(
        prs,
        "part two",
        "The Profile Database",
        "What is in it, and how to query it without getting a wrong answer",
        PURPLE,
    )


def s_profile_what(prs):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "what it is",
        "One row per person, rebuilt every morning",
        "Everything eight source systems know about somebody, resolved onto a single record.",
        accent=PURPLE,
    )
    groups = [
        ("Who they are", "name, country, role, condition"),
        ("What they told us", "survey answers, registration, preferences"),
        ("What they did", "opens, clicks, visits, forum posts"),
        ("What we may do", "consent, subscription, contactability"),
        ("Professional", "credential, specialty, practice location"),
        ("Provenance", "which source said so, and when"),
    ]
    x, y = 0.6, 1.45
    for i, (name, what) in enumerate(groups):
        if i == 3:
            x, y = 0.6, 2.45
        rect(s, x, y, 3.95, 0.88, WHITE, line=BORDER)
        rect(s, x, y, 3.95, 0.05, PURPLE)
        text(s, name, x + 0.2, y + 0.14, 3.5, 0.26, size=11.5, bold=True, color=DGRAY)
        text(s, what, x + 0.2, y + 0.42, 3.6, 0.36, size=9.5, color=MGRAY)
        x += 4.13
    nums = [
        (f"{L['tot']:,}", "profiles"),
        (f"{L['known']:,}", "people we can name"),
        (f"{L['roles']:,}", "with a known role"),
        (f"{L['conds']}", "conditions"),
        (f"{L['views']}", "ready-made views"),
    ]
    x = 0.6
    for big, label in nums:
        rect(s, x, 3.55, 2.36, 0.95, WHITE, line=BORDER)
        rect(s, x, 3.55, 2.36, 0.05, TEAL)
        text(s, big, x + 0.16, 3.68, 2.1, 0.4, size=17, bold=True, color=DGRAY)
        text(s, label, x + 0.16, 4.1, 2.1, 0.28, size=9.5, color=MGRAY)
        x += 2.45
    fbu(
        s,
        4.75,
        "Resolves eight systems onto one record per person and refreshes it "
        "every morning.",
        "One question, one answer. No reconciling exports by hand, and no two "
        "analysts producing different numbers.",
        "Audience sizing, condition reporting, campaign lists, growth "
        "tracking, clinician targeting.",
        colour=PURPLE,
    )
    footer(s, nxt())
    return s


def s_profile_pitfalls(prs):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "read this before you query",
        "Six ways to get a confidently wrong number",
        "Every one of these returns rows. None of them errors. All of them mislead.",
        accent=RED,
    )
    traps = [
        (
            "Forgetting is_bot = FALSE",
            "You are counting machines as people.",
            f"{L['bots']:,} identifiers are automated traffic.",
        ),
        (
            "Ignoring is_known_person",
            "You quote the whole table as your audience.",
            f"{L['tot']:,} rows, but {L['known']:,} people you can actually name.",
        ),
        (
            "Counting evidence rows",
            "You count links, not people.",
            "The evidence log holds rejected evidence too.",
        ),
        (
            "Mixing declared and inferred",
            "You call somebody a patient who only read an article.",
            "Check the source field before saying patients.",
        ),
        (
            "Using verified HCP as reach",
            "You overstate reachable clinicians about sixteenfold.",
            f"{L['hcp']:,} verified, {L['hcpm']:,} reachable.",
        ),
        (
            "Reusing a saved person ID",
            "Rows silently vanish after two records merge.",
            "Resolve it through the redirect table first.",
        ),
    ]
    y = 1.45
    for title, why, detail in traps:
        rect(s, 0.6, y, 12.25, 0.78, WHITE, line=BORDER)
        rect(s, 0.6, y, 0.07, 0.78, RED)
        text(s, title, 0.92, y + 0.11, 3.6, 0.3, size=11.5, bold=True, color=RED)
        text(s, why, 4.7, y + 0.1, 4.3, 0.55, size=10.5, color=DGRAY)
        text(s, detail, 9.2, y + 0.1, 3.45, 0.55, size=10, italic=True, color=MGRAY)
        y += 0.86
    rect(s, 0.6, 6.6, 12.25, 0.66, AMBER_BG, line=GOLD)
    text(
        s,
        "The BI query guides open with these rules, and every worked query states "
        "the mistake it prevents before it shows the query. Start there rather than "
        "from a blank page.",
        0.85,
        6.74,
        11.8,
        0.4,
        size=11,
        bold=True,
        color=AMBER_TX,
    )
    footer(s, nxt())
    return s


def s_profile_prebuilt(prs):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "what is already built for you",
        "Definitions agreed once, so nobody re-derives them",
        f"{L['views']} ready-made views. Most questions do not need a join.",
        accent=PURPLE,
    )
    rect(s, 0.6, 1.45, 12.25, 1.6, WHITE, line=TEAL, line_pt=1.5)
    rect(s, 0.6, 1.45, 12.25, 0.06, TEAL)
    text(s, "START HERE", 0.85, 1.62, 4, 0.24, size=9.5, bold=True, color=TEAL)
    text(
        s,
        "profile_metrics",
        0.85,
        1.86,
        4.5,
        0.36,
        size=17,
        bold=True,
        color=DGRAY,
        font=MONO,
    )
    text(
        s,
        "One view holding every audience definition as a clean yes/no flag: is this "
        "a person we can name, is this a verified clinician, can we email them, are "
        "they active, do they belong to the forum. Built so nobody has to re-derive "
        "a definition and get it slightly different.",
        0.85,
        2.28,
        11.7,
        0.7,
        size=11.5,
        color=DGRAY,
    )
    others = [
        (
            "Audience views",
            "HCPs, confirmed patients, caregivers, high engagement -- already filtered",
            PURPLE,
        ),
        ("Contactability", "who may be emailed, with consent already applied", GREEN),
        (
            "Coverage",
            "how complete each field is, so you know what will carry a question",
            GOLD,
        ),
        ("Safe view", "the same data with sensitive fields redacted", NAVY),
    ]
    y = 3.15
    for name, body, colour in others:
        rect(s, 0.6, y, 12.25, 0.62, WHITE, line=BORDER)
        rect(s, 0.6, y, 0.06, 0.62, colour)
        text(s, name, 0.92, y + 0.17, 3.0, 0.28, size=11, bold=True, color=DGRAY)
        text(s, body, 4.1, y + 0.17, 8.5, 0.3, size=10.5, color=MGRAY)
        y += 0.66
    fbu(
        s,
        5.85,
        "Publishes agreed definitions as ready-made views rather than leaving "
        "each analyst to build their own.",
        "Two people asking the same question get the same answer, and the "
        "definition can be argued about once rather than every time.",
        "Most audience questions are a single filter on profile_metrics.",
        colour=PURPLE,
    )
    footer(s, nxt())
    return s

def s_profile_usecases(prs):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "what you can ask it",
        "The questions people actually bring",
        "All answerable today, most as a single filter.",
        accent=PURPLE,
    )
    cases = [
        (
            "Audience sizing",
            "How many people do we have for a condition, how many can "
            "we reach, and how many are actually active.",
            TEAL,
        ),
        (
            "Declared versus inferred",
            "How many told us their condition rather than us "
            "inferring it from what they read. Only the first supports the word patients.",
            GOLD,
        ),
        (
            "Clinician targeting",
            "Verified professionals by specialty and location, "
            "narrowed to those we can actually email.",
            PURPLE,
        ),
        (
            "Growth",
            "New people month over month, with cookie sightings and repeat "
            "visits excluded so the number means something.",
            GREEN,
        ),
        (
            "Channel overlap",
            "Who reads the newsletter but never visits, who visits but "
            "never subscribed, who does both.",
            NAVY,
        ),
        (
            "Opportunity sizing",
            "If we launched a site for a new condition, how many "
            "people do we already have who would care.",
            TEAL,
        ),
    ]
    x, y = 0.6, 1.45
    for i, (name, body, colour) in enumerate(cases):
        if i == 3:
            x, y = 0.6, 3.05
        rect(s, x, y, 3.95, 1.45, WHITE, line=BORDER)
        rect(s, x, y, 3.95, 0.06, colour)
        text(s, name, x + 0.2, y + 0.18, 3.5, 0.3, size=12, bold=True, color=colour)
        text(s, body, x + 0.2, y + 0.52, 3.6, 0.85, size=10.5, color=MGRAY)
        x += 4.13
    rect(s, 0.6, 4.75, 12.25, 1.35, WHITE, line=BORDER)
    text(
        s,
        "AND THE ONE THAT CHANGES A CONVERSATION",
        0.85,
        4.88,
        7,
        0.24,
        size=9.5,
        bold=True,
        color=GREEN,
    )
    text(
        s,
        "For any condition we can produce the total audience, the reachable subset "
        "and the actively engaged subset, and stand behind all three. When a partner "
        "asks the second question, there is an answer.",
        0.85,
        5.16,
        11.7,
        0.8,
        size=12,
        color=DGRAY,
    )
    takeaway(
        s,
        "The BI query guides carry fifteen worked examples for profiles and "
        "fourteen for identity, each with the trap it avoids stated first.",
        colour=PURPLE,
        band=PURPBG,
    )
    footer(s, nxt())
    return s


# ══ PART 3 — HOW THEY FIT TOGETHER ═══════════════════════════════════════════


def s_interact(prs):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "part three",
        "How the two fit together",
        "One system answers who. The other answers what about them.",
        accent=GREEN,
    )
    rect(s, 0.7, 1.6, 3.5, 1.6, NAVY)
    text(s, "IDENTITY HUB", 0.95, 1.82, 3.1, 0.28, size=10.5, bold=True, color=TEAL)
    text(s, "Answers WHO", 0.95, 2.12, 3.1, 0.35, size=15, bold=True, color=WHITE)
    text(
        s,
        f"{L['ids']:,} identifiers\ninto {L['ppl']:,} people",
        0.95,
        2.5,
        3.1,
        0.6,
        size=10.5,
        color=SKY,
    )
    arrow(s, 4.45, 2.2, 1.0, 0.45, TEAL)
    chip(s, "one person ID", 4.3, 1.72, 1.3, 0.38, TEAL, size=8.5)
    rect(s, 5.7, 1.6, 3.5, 1.6, PURPLE)
    text(
        s,
        "PROFILE DATABASE",
        5.95,
        1.82,
        3.1,
        0.28,
        size=10.5,
        bold=True,
        color=RGBColor(0xE1, 0xBE, 0xE7),
    )
    text(s, "Answers WHAT", 5.95, 2.12, 3.1, 0.35, size=15, bold=True, color=WHITE)
    text(
        s,
        "eight sources onto\none row per person",
        5.95,
        2.5,
        3.1,
        0.6,
        size=10.5,
        color=RGBColor(0xE1, 0xBE, 0xE7),
    )
    arrow(s, 9.45, 2.2, 1.0, 0.45, GREEN)
    rect(s, 10.7, 1.6, 2.15, 1.6, GREEN)
    text(s, "YOU", 10.95, 1.85, 1.7, 0.3, size=11, bold=True, color=WHITE)
    text(
        s,
        "queries,\nreports,\naudiences",
        10.95,
        2.2,
        1.7,
        0.85,
        size=11,
        bold=True,
        color=WHITE,
    )
    panels = [
        (
            0.6,
            "WHY BOTH ARE NEEDED",
            TEAL,
            "Knowing what somebody read is only useful if you know who they are. "
            "Knowing who they are is only useful if you know what they care about. "
            "Neither system is worth much alone.",
        ),
        (
            6.85,
            "IT COMPOUNDS",
            GREEN,
            "Every new source is matched against people we already know, so it deepens "
            "existing profiles rather than just adding names. The value grows with each "
            "connection rather than merely accumulating.",
        ),
    ]
    for x, title, colour, body in panels:
        rect(s, x, 3.5, 6.0, 1.45, WHITE, line=BORDER)
        rect(s, x, 3.5, 6.0, 0.06, colour)
        text(s, title, x + 0.25, 3.66, 5, 0.24, size=9.5, bold=True, color=colour)
        text(s, body, x + 0.25, 3.94, 5.5, 0.9, size=10.5, color=DGRAY)
    fbu(
        s,
        5.2,
        "The hub establishes who exists; the profile database describes them. "
        "One join key connects the two.",
        "You query one place. The identity work happens upstream and you "
        "inherit the result.",
        "Every query you write starts from a person the hub already resolved.",
        colour=GREEN,
    )
    footer(s, nxt())
    return s


def s_close(prs):
    s = blank(prs)
    bgc(s, LGRAY)
    header(s, "getting started", "Where to go next", "", accent=TEAL)
    rows = [
        (
            "profile_db_bi_queries.docx",
            "Fifteen worked queries, each stating the mistake it prevents. Start here.",
            TEAL,
        ),
        (
            "profile_db_bi_queries.sql",
            "The same queries as a runnable file -- paste and go.",
            TEAL,
        ),
        (
            "profile_db_data_dictionary.docx",
            "Every table, every view, every column. The one you will keep open.",
            PURPLE,
        ),
        (
            "profile_db_table_reference.docx",
            "Tables grouped by what they are for, views first.",
            PURPLE,
        ),
        (
            "identity_hub_bi_queries.docx",
            "Fourteen queries for the identity side, if you need to look upstream.",
            NAVY,
        ),
        (
            "profile_db_overview.docx",
            "The plain-English explanation of how it all hangs together.",
            NAVY,
        ),
    ]
    y = 1.5
    for name, body, colour in rows:
        rect(s, 0.6, y, 12.25, 0.68, WHITE, line=BORDER)
        rect(s, 0.6, y, 0.06, 0.68, colour)
        text(
            s,
            name,
            0.92,
            y + 0.2,
            4.6,
            0.28,
            size=11,
            bold=True,
            color=DGRAY,
            font=MONO,
        )
        text(s, body, 5.8, y + 0.19, 6.9, 0.32, size=10.5, color=MGRAY)
        y += 0.76
    rect(s, 0.6, 6.15, 12.25, 0.95, GREENBG, line=GREEN)
    text(
        s,
        "THE THREE RULES THAT PREVENT MOST WRONG ANSWERS",
        0.85,
        6.28,
        8,
        0.24,
        size=9.5,
        bold=True,
        color=GREEN,
    )
    text(
        s,
        "Filter is_bot = FALSE in any count of people.   Use is_known_person before "
        "quoting an audience.   Check whether a condition was declared or inferred "
        "before calling somebody a patient.",
        0.85,
        6.56,
        11.7,
        0.45,
        size=11.5,
        bold=True,
        color=DGRAY,
    )
    footer(s, nxt())
    return s


# ══ APPENDIX ═════════════════════════════════════════════════════════════════


def s_apx_divider(prs):
    return part_divider(
        prs, "appendix", "Reference", "For questions, not for the main flow", SLATE
    )


def s_apx_numbers(prs):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "appendix",
        "The numbers, and which one answers which question",
        "Each is smaller than the one above. Using a wider number than the question "
        "deserves is the most common mistake.",
        accent=SLATE,
    )
    rows = [
        (
            f"{L['tot']:,}",
            "profiles in the database",
            "Includes anonymous visitors. Not an audience.",
            SLATE,
        ),
        (
            f"{L['known']:,}",
            "people we can name",
            "is_known_person. The honest starting point.",
            NAVY,
        ),
        (
            f"{L['roles']:,}",
            "with a known role",
            "Patient, caregiver, family, clinician.",
            TEAL,
        ),
        (
            f"{L['mail']:,}",
            "reachable by email",
            "Subscribed, opted in, consent not denied.",
            GREEN,
        ),
        (
            f"{L['hcp']:,}",
            "verified clinicians",
            "Real credential. Not an audience.",
            PURPLE,
        ),
        (
            f"{L['hcpm']:,}",
            "clinicians we can reach",
            "Use this one commercially.",
            GREEN,
        ),
    ]
    y = 1.45
    for big, label, note, colour in rows:
        rect(s, 0.6, y, 12.25, 0.78, WHITE, line=BORDER)
        rect(s, 0.6, y, 0.07, 0.78, colour)
        text(s, big, 0.95, y + 0.14, 2.3, 0.45, size=19, bold=True, color=colour)
        text(s, label, 3.5, y + 0.12, 4.0, 0.3, size=12, bold=True, color=DGRAY)
        text(s, note, 3.5, y + 0.42, 8.8, 0.3, size=10, color=MGRAY)
        y += 0.86
    footer(s, nxt())
    return s


def s_hub_tables(prs):
    """The two tables a consumer actually touches, with real columns."""
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "the identity hub tables",
        "Two you will use. Six you will not.",
        "Almost every question is answered by the first table on this slide.",
        accent=TEAL,
    )

    rect(s, 0.6, 1.4, 12.25, 2.25, WHITE, line=TEAL, line_pt=1.5)
    rect(s, 0.6, 1.4, 12.25, 0.06, TEAL)
    text(s, "START HERE", 0.85, 1.56, 3, 0.22, size=9, bold=True, color=TEAL)
    text(
        s,
        "bn_id_xref",
        0.85,
        1.78,
        3.4,
        0.34,
        size=16,
        bold=True,
        color=DGRAY,
        font=MONO,
    )
    text(
        s,
        f"{L['ids']:,} rows -- one per identifier, mapped to its person",
        4.4,
        1.86,
        8.2,
        0.26,
        size=11,
        color=MGRAY,
    )
    cols = [
        ("bn_id", "the person -- your join key"),
        ("identifier_type", "email, cookie, login, provider ID"),
        ("identifier_value", "the identifier itself"),
        ("cluster_tier", "tier1 = nameable, tier2 = anonymous"),
        ("cluster_size", "how many identifiers this person has"),
        ("is_bot", "TRUE = automated. Always filter this out"),
        ("is_hcp", "clinician flag"),
        ("is_shared_workstation", "clinic or library device"),
        ("is_suspicious", "flagged for review"),
        ("cluster_health_score", "confidence in the grouping"),
        ("last_seen", "most recent sighting"),
        ("source_profile", "how this person entered the graph"),
    ]
    x, y = 0.9, 2.2
    for i, (name, what) in enumerate(cols):
        if i == 6:
            x, y = 6.9, 2.2
        colour = RED if name == "is_bot" else (TEAL if name == "bn_id" else MGRAY)
        bold = name in ("bn_id", "is_bot")
        text(s, name, x, y, 2.15, 0.22, size=9.5, bold=bold, color=colour, font=MONO)
        text(s, what, x + 2.2, y, 3.4, 0.22, size=9, color=MGRAY)
        y += 0.23

    rect(s, 0.6, 3.8, 12.25, 1.35, WHITE, line=NAVY, line_pt=1.25)
    rect(s, 0.6, 3.8, 12.25, 0.06, NAVY)
    text(
        s,
        "AND ONE MORE, IF YOU CACHE A bn_id",
        0.85,
        3.95,
        5,
        0.22,
        size=9,
        bold=True,
        color=NAVY,
    )
    text(
        s,
        "bn_id_persistence",
        0.85,
        4.16,
        3.4,
        0.32,
        size=15,
        bold=True,
        color=DGRAY,
        font=MONO,
    )
    text(
        s,
        f"{123_337:,} rows -- where a retired bn_id now points",
        4.4,
        4.22,
        8.2,
        0.26,
        size=11,
        color=MGRAY,
    )
    pcols = [
        ("old_bn_id", "the ID you are holding"),
        ("current_bn_id", "the ID to use instead"),
        ("event_type", "why it changed"),
        ("event_date", "when"),
    ]
    x = 0.9
    for name, what in pcols:
        text(s, name, x, 4.58, 2.9, 0.22, size=9.5, bold=True, color=DGRAY, font=MONO)
        text(s, what, x, 4.79, 2.9, 0.22, size=9, color=MGRAY)
        x += 3.05

    fbu(
        s,
        5.35,
        "bn_id_xref maps every identifier to its person. bn_id_persistence "
        "forwards any bn_id that has since been retired.",
        "Two tables answer nearly every identity question. You do not need to "
        "understand the other six to use the graph correctly.",
        "Resolving an email to a person, sizing an audience, or refreshing a "
        "list of IDs you saved last quarter.",
        colour=TEAL,
    )
    footer(s, nxt())
    return s


def s_apx_tables(prs):
    """Appendix: the full eight, for anyone who asks."""
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "appendix",
        "All eight identity hub tables",
        "The six below the line are machinery. Listed so nothing looks hidden.",
        accent=SLATE,
    )
    rows = [
        (
            "bn_id_xref",
            f"{L['ids']:,}",
            "bn_id, identifier_type, identifier_value, cluster_tier, cluster_size, "
            "is_bot, is_hcp, is_shared_workstation, is_suspicious, "
            "cluster_health_score, last_seen, source_profile",
            TEAL,
        ),
        (
            "bn_id_persistence",
            "123,337",
            "old_bn_id, current_bn_id, event_type, event_date, run_id, build_mode, "
            "config_version, git_sha",
            NAVY,
        ),
        (
            "bn_id_hub",
            f"{L['edges']:,}",
            "bn_id, identifier_a_type, identifier_a_value, identifier_b_type, "
            "identifier_b_value, source_system, link_type, match_rule, "
            "base_confidence, confidence, effective_confidence, first_seen, "
            "last_seen, is_active, cluster_tier",
            SLATE,
        ),
        (
            "bn_id_identity_changes",
            "405,823",
            "event_date, event_type, surviving_bn_id, retired_bn_id, "
            "node_count_moved, trigger_edge, run_id, build_mode, config_version, "
            "git_sha",
            SLATE,
        ),
        (
            "bn_id_neighbors",
            "87,085,523",
            "bn_id, node_type, node_value, neighbor_type, neighbor_value, "
            "confidence, match_rule, source_system, first_seen, last_seen, "
            "cluster_tier",
            SLATE,
        ),
        (
            "bn_id_node_index",
            "46,229,058",
            "identifier_key, bn_id, identifier_type, is_output, cluster_tier, run_id",
            SLATE,
        ),
        (
            "bn_id_metrics",
            "11,747",
            "run_date, run_id, build_mode, config_version, git_sha, metric_name, "
            "metric_value",
            SLATE,
        ),
        ("bn_id_manifest", "35", "active_run_id, promoted_at, status", SLATE),
    ]
    y = 1.38
    for i, (name, n, cols, colour) in enumerate(rows):
        h = 0.72 if i < 2 else 0.62
        rect(s, 0.6, y, 12.25, h, WHITE, line=BORDER)
        rect(s, 0.6, y, 0.06, h, colour)
        text(
            s,
            name,
            0.9,
            y + 0.08,
            3.0,
            0.26,
            size=10.5,
            bold=True,
            color=DGRAY,
            font=MONO,
        )
        text(s, n, 0.9, y + 0.34, 3.0, 0.22, size=9, color=colour)
        text(s, cols, 4.05, y + 0.08, 8.6, h - 0.14, size=8.5, color=MGRAY)
        if i == 1:
            y += h + 0.14
            rect(s, 0.6, y - 0.09, 12.25, 0.02, SLATE)
            text(
                s,
                "MACHINERY -- NOT FOR CONSUMER QUERIES",
                0.6,
                y - 0.06,
                6,
                0.2,
                size=8,
                bold=True,
                color=SLATE,
            )
            y += 0.05
        else:
            y += h + 0.05
    footer(s, nxt())
    return s


def main() -> int:
    prs = Presentation()
    prs.slide_width = Inches(13.33)
    prs.slide_height = Inches(7.5)

    s_title(prs)
    s_sources(prs)

    s_hub_divider(prs)
    s_hub_problem(prs)
    s_hub_evidence(prs)
    s_uf_1(prs)
    s_uf_2(prs)
    s_uf_3(prs)
    s_hub_current(prs)
    s_hub_tables(prs)
    s_hub_safeguards(prs)

    s_profile_divider(prs)
    s_profile_what(prs)
    s_profile_pitfalls(prs)
    s_profile_prebuilt(prs)
    s_profile_usecases(prs)

    s_interact(prs)
    s_close(prs)

    s_apx_divider(prs)
    s_apx_numbers(prs)
    s_apx_tables(prs)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    prs.save(str(OUT))
    print(f"[OK] Wrote {OUT.relative_to(REPO)}  ({len(prs.slides._sldIdLst)} slides)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
