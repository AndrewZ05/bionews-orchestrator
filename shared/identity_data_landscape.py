#!/usr/bin/env python3
"""
Generate Bionews Identity Graph Data Landscape
Shows all node-neighbor relationships in the identity graph
"""

def main():
    print()
    print("=" * 145)
    print("BIONEWS IDENTITY GRAPH - DATA LANDSCAPE")
    print("=" * 145)
    print()

    # Define all identity mappings
    mappings = [
        # Priority 1 sources (highest value)
        {"row": 1, "mapping": "visitor_id <> aim_dgid", "type1": "visitor", "domain1": "bionews", "id1": "visitor_id", "source": "pageload_sessions", "link_attr": "same_row", "type2": "identifier", "domain2": "healthcare", "id2": "aim_dgid", "provider": "AIM/IQVIA"},
        {"row": 2, "mapping": "visitor_id <> client_id", "type1": "visitor", "domain1": "bionews", "id1": "visitor_id", "source": "pageload_sessions", "link_attr": "same_row", "type2": "identifier", "domain2": "analytics", "id2": "client_id", "provider": "GA"},
        {"row": 3, "mapping": "visitor_id <> clarity_user", "type1": "visitor", "domain1": "bionews", "id1": "visitor_id", "source": "pageload_sessions", "link_attr": "same_row", "type2": "identifier", "domain2": "analytics", "id2": "clarity_user", "provider": "MS Clarity"},

        # Facebook identifiers
        {"row": 4, "mapping": "visitor_id <> fbp", "type1": "visitor", "domain1": "bionews", "id1": "visitor_id", "source": "pageload_sessions", "link_attr": "same_row", "type2": "identifier", "domain2": "social", "id2": "fbp", "provider": "FB Pixel"},
        {"row": 5, "mapping": "visitor_id <> fbc", "type1": "visitor", "domain1": "bionews", "id1": "visitor_id", "source": "pageload_sessions", "link_attr": "cookie_extract", "type2": "identifier", "domain2": "social", "id2": "_fbc", "provider": "FB Click"},
        {"row": 6, "mapping": "visitor_id <> fbclid", "type1": "visitor", "domain1": "bionews", "id1": "visitor_id", "source": "pageload_sessions", "link_attr": "same_row", "type2": "identifier", "domain2": "social", "id2": "fbclid", "provider": "FB Click ID"},

        # Marketing/Ad identifiers
        {"row": 7, "mapping": "visitor_id <> dmd_tag", "type1": "visitor", "domain1": "bionews", "id1": "visitor_id", "source": "pageload_sessions", "link_attr": "cookie_extract", "type2": "identifier", "domain2": "marketing", "id2": "dmd-tag", "provider": "DMD Marketing"},
        {"row": 8, "mapping": "visitor_id <> gcl_au", "type1": "visitor", "domain1": "bionews", "id1": "visitor_id", "source": "pageload_sessions", "link_attr": "cookie_extract", "type2": "identifier", "domain2": "ads", "id2": "_gcl_au", "provider": "Google Ads"},
        {"row": 9, "mapping": "visitor_id <> gcl_aw", "type1": "visitor", "domain1": "bionews", "id1": "visitor_id", "source": "pageload_sessions", "link_attr": "cookie_extract", "type2": "identifier", "domain2": "ads", "id2": "_gcl_aw", "provider": "Google Ads"},
        {"row": 10, "mapping": "visitor_id <> kla_id", "type1": "visitor", "domain1": "bionews", "id1": "visitor_id", "source": "pageload_sessions", "link_attr": "cookie_extract", "type2": "identifier", "domain2": "email", "id2": "__kla_id", "provider": "Klaviyo"},

        # Analytics identifiers
        {"row": 11, "mapping": "visitor_id <> gpi", "type1": "visitor", "domain1": "bionews", "id1": "visitor_id", "source": "pageload_sessions", "link_attr": "cookie_extract", "type2": "identifier", "domain2": "ads", "id2": "__gpi", "provider": "Google Pub"},
        {"row": 12, "mapping": "visitor_id <> ga", "type1": "visitor", "domain1": "bionews", "id1": "visitor_id", "source": "pageload_sessions", "link_attr": "cookie_extract", "type2": "identifier", "domain2": "analytics", "id2": "_ga", "provider": "GA Universal"},
        {"row": 13, "mapping": "visitor_id <> chartbeat", "type1": "visitor", "domain1": "bionews", "id1": "visitor_id", "source": "pageload_sessions", "link_attr": "cookie_extract", "type2": "identifier", "domain2": "analytics", "id2": "_cb", "provider": "Chartbeat"},
        {"row": 14, "mapping": "visitor_id <> omappvp", "type1": "visitor", "domain1": "bionews", "id1": "visitor_id", "source": "pageload_sessions", "link_attr": "cookie_extract", "type2": "identifier", "domain2": "marketing", "id2": "_omappvp", "provider": "OptinMonster"},

        # Survey/Low-priority
        {"row": 15, "mapping": "visitor_id <> limesurvey", "type1": "visitor", "domain1": "bionews", "id1": "visitor_id", "source": "pageload_sessions", "link_attr": "cookie_extract", "type2": "identifier", "domain2": "survey", "id2": "LS-*_STATUS", "provider": "LimeSurvey"},
        {"row": 16, "mapping": "visitor_id <> bing_user", "type1": "visitor", "domain1": "bionews", "id1": "visitor_id", "source": "pageload_sessions", "link_attr": "same_row", "type2": "identifier", "domain2": "ads", "id2": "bing_user", "provider": "MS Bing"},
    ]

    # Stitching relationships (calculated)
    stitching = [
        {"row": 17, "mapping": "visitor_id <> bionews_id", "type1": "visitor", "domain1": "bionews", "id1": "visitor_id", "source": "identity_lookup", "link_attr": "stitched", "type2": "bionews_id", "domain2": "bionews", "id2": "bionews_id", "provider": "calculated"},
        {"row": 18, "mapping": "identifier <> identifier", "type1": "identifier", "domain1": "*", "id1": "*_value", "source": "identifier_pairs", "link_attr": "co_occurrence", "type2": "identifier", "domain2": "*", "id2": "*_value", "provider": "calculated"},
        {"row": 19, "mapping": "identifier <> bionews_id", "type1": "identifier", "domain1": "*", "id1": "*_value", "source": "identity_edges", "link_attr": "member_of", "type2": "bionews_id", "domain2": "bionews", "id2": "bionews_id", "provider": "calculated"},
        {"row": 20, "mapping": "bionews_id <> identifier", "type1": "bionews_id", "domain1": "bionews", "id1": "bionews_id", "source": "identity_edges", "link_attr": "contains", "type2": "identifier", "domain2": "*", "id2": "*_value", "provider": "calculated"},
    ]

    def print_header():
        print(f"{'#':<3} {'Client Mapping':<28} {'Type1':<10} {'Domain1':<10} {'ID1/Col':<15} {'Source':<18} {'Link Attr':<14} {'Type2':<12} {'Domain2':<10} {'ID2/Col':<12} {'Provider':<14}")
        print("-" * 145)

    def print_row(m):
        print(f"{m['row']:<3} {m['mapping']:<28} {m['type1']:<10} {m['domain1']:<10} {m['id1']:<15} {m['source']:<18} {m['link_attr']:<14} {m['type2']:<12} {m['domain2']:<10} {m['id2']:<12} {m['provider']:<14}")

    def print_section(title, data):
        print("=" * 145)
        print(title)
        print("=" * 145)
        print()
        print_header()
        for m in data:
            print_row(m)
        print()

    print_section("PRIORITY 1 SOURCES - Healthcare & High-Value Analytics", mappings[:3])
    print_section("SOCIAL/FACEBOOK IDENTIFIERS", mappings[3:6])
    print_section("MARKETING/ADVERTISING IDENTIFIERS", mappings[6:11])
    print_section("ANALYTICS/TRACKING IDENTIFIERS", mappings[11:14])
    print_section("LOWER PRIORITY IDENTIFIERS", mappings[14:16])
    print_section("STITCHING RELATIONSHIPS (Probabilistic Matching)", stitching)

    print("=" * 145)
    print("NOTES")
    print("=" * 145)
    print("""
1. SAME_ROW link: Data captured directly by Bionews Acceptor JavaScript tag on pageload.
   If fill rate is within 5% or less, we deem that acceptable.

2. COOKIE_EXTRACT link: Extracted from all_cookies field using regex patterns.
   These cookies are set by third-party services (Facebook, Google, Klaviyo, etc.).

3. STITCHED link: Probabilistic matching via 4-step iterative algorithm:
   Step 1: Same identifier_value -> assign MIN(stitched_id)
   Step 2: Same visitor_id -> propagate MIN(stitched_id)
   Step 3: Same identifier_value -> propagate again (transitive closure)
   Step 4: Co-occurring identifiers -> propagate MIN(stitched_id) [NEW in V3]

4. CO_OCCURRENCE link: Two identifiers appearing in the same pageload row are directly
   linked as neighbors, indicating they belong to the same visitor session.

5. MEMBER_OF link: Each identifier belongs to exactly one canonical bionews_id.

6. CONTAINS link: Each bionews_id contains all identifiers that were stitched to it.

7. HIGH-VALUE identifiers for healthcare audience:
   - aim_dgid: Healthcare professional identity from IQVIA/AIM (Priority 1)
   - client_id: GA Client ID with 100% fill rate, 3.7M unique (Priority 2)
   - clarity_user: Microsoft Clarity user ID (Priority 3)

8. EXCLUDED from graph:
   - Identifiers linked to >125 unique visitors (likely bots or shared devices)
   - Visitors with >125 unique identifiers (suspicious activity)
   - Identifiers with <8 characters (noise)
   - Invalid values: null, undefined, false, empty, NA, REDACTED

9. EMAIL IS NOT AVAILABLE in pageload tracking data (GDPR/privacy compliance).
   To enable email-based identity resolution, WordPress user tracking would need
   to capture user_id and link it to wp_users.user_email.
""")

    print()
    print("=" * 145)
    print("OUTPUT TABLES")
    print("=" * 145)
    print("""
+------------------------------------------+----------------------------------------------------+
| Table                                    | Description                                        |
+------------------------------------------+----------------------------------------------------+
| bio_acceptor_data.page_load_sessions     | SOURCE: Sessionized pageload events with identity  |
| bio_acceptor_data.identity_lookup        | Working table for stitching algorithm              |
| bio_acceptor_data.identifier_pairs       | Co-occurrence pairs (identifier <> identifier)     |
| bio_acceptor_data.bionews_id_xref        | Final visitor_id to bionews_id mapping             |
| identity_graph.identifier_edges          | Flat edge table with all relationships             |
| identity_graph.graph_snapshot_v3         | Nested graph with direct identifier links          |
| identity_graph.graph_diff                | Daily diff for incremental updates                 |
+------------------------------------------+----------------------------------------------------+
""")


if __name__ == "__main__":
    main()
