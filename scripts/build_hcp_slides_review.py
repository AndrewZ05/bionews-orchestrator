#!/usr/bin/env python3
"""
Build exports/hcp_slides_review.pptx -- the two HCP slides, standalone, for
review before pasting into the main deck.

Kept separate on purpose so the main deck is not disturbed while the wording is
still being settled. Reuses the palette and helpers from build_platform_deck.py
so anything pasted across matches without restyling.

Why two slides and not one: the single slide had four number cards, a
five-line correction and a four-route explanation stacked on top of each other.
The correction and the routes answer different questions -- "is this number
real" and "how does an NPI reach a person" -- and each needs room.

Every figure is production-verified 2026-08-20.

  python scripts/build_hcp_slides_review.py
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from pptx import Presentation
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / "exports" / "hcp_slides_review.pptx"

spec = importlib.util.spec_from_file_location(
    "_deck", REPO / "scripts" / "build_platform_deck.py"
)
D = importlib.util.module_from_spec(spec)
sys.modules["_deck"] = D
spec.loader.exec_module(D)

blank, bgc, rect, text, header, footer, takeaway = (
    D.blank,
    D.bg,
    D.rect,
    D.text,
    D.header,
    D.footer,
    D.takeaway,
)
NAVY, TEAL, GREEN, RED, REDBG = D.NAVY, D.TEAL, D.GREEN, D.RED, D.REDBG
SLATE, DGRAY, MGRAY, WHITE, LGRAY, BORDER = (
    D.SLATE,
    D.DGRAY,
    D.MGRAY,
    D.WHITE,
    D.LGRAY,
    D.BORDER,
)
GOLD, AMBER_BG, AMBER_TX, PURPLE = D.GOLD, D.AMBER_BG, D.AMBER_TX, D.PURPLE


def slide_numbers(prs):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "the number most often quoted wrongly",
        "How many HCPs do we have? Four answers, differing 26x",
        "The question decides which one is honest.",
        accent=RED,
    )

    cards = [
        ("383,213", "VERIFIED", "Real, current credential.\nNot an audience.", SLATE),
        ("102,367", "ENGAGED", "Has ever produced a\npageview, open or click.", TEAL),
        (
            "23,610",
            "REACHABLE",
            "Verified AND mailable.\nUse this commercially.",
            GREEN,
        ),
        ("14,467", "LIVE", "Active on email in the\nlast 90 days.", NAVY),
    ]
    x = 0.6
    for num, label, body, colour in cards:
        rect(s, x, 1.5, 2.95, 2.05, WHITE, line=BORDER)
        rect(s, x, 1.5, 2.95, 0.08, colour)
        text(s, label, x + 0.2, 1.72, 2.5, 0.26, size=10.5, bold=True, color=colour)
        text(s, num, x + 0.2, 1.98, 2.6, 0.6, size=27, bold=True, color=DGRAY)
        text(s, body, x + 0.2, 2.68, 2.6, 0.8, size=10.5, color=MGRAY)
        x += 3.06

    rect(s, 0.6, 3.78, 12.25, 2.42, REDBG, line=RED)
    text(
        s,
        "WHY THE FIRST NUMBER IS MISLEADING",
        0.85,
        3.9,
        6,
        0.26,
        size=10,
        bold=True,
        color=RED,
    )
    text(
        s,
        "About 73 percent of verified HCPs -- 280,846 people -- came from the "
        "federal NPI registry, not from our audience. 272,299 hold nothing but an "
        "email address and an NPI number: no browser, no session, no subscription.",
        0.85,
        4.18,
        11.7,
        0.62,
        size=12.5,
        color=DGRAY,
    )

    # The email-quality bar: the fact that explains the whole gap.
    text(
        s,
        "86 PERCENT ARE CLOSED-NETWORK CLINICAL ADDRESSES -- MAILCHIMP CANNOT SEND TO THEM",
        0.85,
        4.86,
        9,
        0.24,
        size=10,
        bold=True,
        color=RED,
    )
    seg = [
        (86.0, RED, "264,676 are HIPAA Direct endpoints"),
        (8.5, GOLD, "26,151 other clinical endpoints"),
        (5.5, GREEN, "16,915 real emails"),
    ]
    x0, total_w = 0.85, 11.7
    for pct, colour, _ in seg:
        w = total_w * pct / 100
        rect(s, x0, 5.14, w, 0.3, colour)
        if pct > 7:
            text(
                s,
                f"{pct:.0f}%",
                x0 + 0.06,
                5.17,
                1.2,
                0.24,
                size=10,
                bold=True,
                color=WHITE,
            )
        x0 += w
    text(
        s,
        "264,676 HIPAA Direct endpoints  |  26,151 other clinical endpoints  |  "
        "16,915 real emails",
        0.85,
        5.5,
        11.7,
        0.26,
        size=10,
        color=MGRAY,
    )
    text(
        s,
        "These are real addresses belonging to real, named clinicians -- 18 percent "
        "are literally firstname.lastname, and fewer than 3 percent are shared "
        "mailboxes, so sharing is not the issue. They live on the HIPAA Direct "
        "network, which only accepts mail from other certified medical senders. "
        "The barrier is the network, not the recipient. It is why 295,359 verified "
        "HCPs have never appeared in Mailchimp -- not unsubscribed, never present.",
        0.85,
        5.72,
        11.7,
        0.5,
        size=10.5,
        bold=True,
        color=DGRAY,
    )

    text(
        s,
        "LIVE is a rolling 90-day window and moves by a few every day. The other three are stable between builds.",
        0.85,
        3.52,
        11.7,
        0.24,
        size=9,
        italic=True,
        color=MGRAY,
    )
    takeaway(
        s,
        "They are a verification asset, not a dormant audience. Use 23,610 for "
        'anything commercial -- it is the only figure that survives "can you '
        'actually reach them?"',
        colour=GREEN,
        band=D.RGBColor(0xE8, 0xF5, 0xE9),
        y=6.35,
    )
    footer(s, 1)
    return s


def slide_matching(prs):
    s = blank(prs)
    bgc(s, LGRAY)
    header(
        s,
        "how an npi reaches a person",
        "We never match NPI to NPI",
        "A shared consumer identifier -- a real email or a device -- is what matches. "
        "The NPI rides along.",
        accent=NAVY,
    )

    routes = [
        (
            "1",
            "DMD_HCP_VERIFIED",
            "77,986",
            "clinicians",
            "A third-party file supplies email and NPI for clinicians it has already "
            "verified. The email is a real one, so it matches a reader we already have.",
            "THE BIGGEST GENUINE ROUTE",
            GREEN,
        ),
        (
            "2",
            "AIM_CLICKSTREAM",
            "30,090",
            "clinicians",
            "The AIM ad platform passes an NPI alongside a device id when a clinician "
            "clicks. That device is already in our graph from their browsing.",
            "",
            TEAL,
        ),
        (
            "3",
            "Shared email",
            "7,791",
            "of 297,629",
            "The registry endpoint email happens to equal one we already hold, so the "
            "clusters merge. Only 2.6 percent of registry-created people ever pick up a "
            "browser this way.",
            "",
            SLATE,
        ),
        (
            "4",
            "Self-declared",
            "170",
            "clinicians",
            "MAILCHIMP_HCP_MERGE (133) and WORDPRESS_NPI (37). Someone typed their NPI "
            "into a form. Smallest, but the strongest evidence we hold.",
            "",
            PURPLE,
        ),
    ]
    y = 1.62
    for num, name, big, unit, body, badge, colour in routes:
        rect(s, 0.6, y, 12.25, 1.16, WHITE, line=BORDER)
        rect(s, 0.6, y, 0.08, 1.16, colour)
        rect(s, 0.95, y + 0.37, 0.42, 0.42, colour)
        text(
            s,
            num,
            0.95,
            y + 0.42,
            0.42,
            0.32,
            size=14,
            bold=True,
            color=WHITE,
            align=PP_ALIGN.CENTER,
        )
        text(
            s,
            name,
            1.55,
            y + 0.14,
            3.5,
            0.3,
            size=13,
            bold=True,
            color=DGRAY,
            font="Consolas" if "_" in name else None,
        )
        text(s, body, 1.55, y + 0.46, 7.3, 0.62, size=10.5, color=MGRAY)
        text(
            s,
            big,
            9.3,
            y + 0.2,
            2.0,
            0.5,
            size=22,
            bold=True,
            color=colour,
            align=PP_ALIGN.RIGHT,
        )
        text(
            s,
            unit,
            9.3,
            y + 0.68,
            2.0,
            0.26,
            size=10,
            color=MGRAY,
            align=PP_ALIGN.RIGHT,
        )
        if badge:
            text(
                s,
                badge,
                11.45,
                y + 0.3,
                1.3,
                0.5,
                size=8.5,
                bold=True,
                color=colour,
                align=PP_ALIGN.CENTER,
            )
        y += 1.24

    rect(s, 0.6, 6.5, 12.25, 0.62, AMBER_BG, line=GOLD)
    text(
        s,
        "78,000 plus 30,000 is roughly is_engaged_hcp at 102,367. The genuine "
        "routes and the engaged population are the same size -- which is the point: "
        "the registry bulk reaches nobody because it arrives with no consumer "
        "identifier to match on.",
        0.85,
        6.62,
        11.8,
        0.42,
        size=11.5,
        bold=True,
        color=AMBER_TX,
    )
    footer(s, 2)
    return s


def main() -> int:
    prs = Presentation()
    prs.slide_width = Inches(13.33)
    prs.slide_height = Inches(7.5)
    slide_numbers(prs)
    slide_matching(prs)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    prs.save(str(OUT))
    print(f"[OK] Wrote {OUT.relative_to(REPO)}  ({len(prs.slides._sldIdLst)} slides)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
