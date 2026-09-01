import argparse
import os

from onetrust_probes.common import OneTrustSession, fetch_paginated, get_env, print_summary, write_output


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe OneTrust receipts and preferences.")
    parser.add_argument("--max-pages", type=int, default=1, help="Max pages per endpoint")
    parser.add_argument("--page-size", type=int, default=100, help="Page size")
    parser.add_argument("--identifier", type=str, default=None, help="Optional data subject identifier")
    parser.add_argument("--output", type=str, default=None, help="Write combined JSON output")
    args = parser.parse_args()

    hostname = get_env("ONETRUST_HOSTNAME")
    client_id = get_env("ONETRUST_CLIENT_ID")
    client_secret = get_env("ONETRUST_CLIENT_SECRET")
    scopes = os.getenv("ONETRUST_SCOPES")

    session = OneTrustSession(hostname, client_id, client_secret, scopes=scopes)
    results = {}

    preferences_params = {"identifier": args.identifier} if args.identifier else None
    preferences, pref_meta = fetch_paginated(
        session,
        "/v2/preferences",
        params=preferences_params,
        page_size=args.page_size,
        max_pages=args.max_pages,
    )
    print_summary("preferences", preferences, pref_meta)
    results["preferences"] = {"records": preferences, "meta": pref_meta}

    receipts_params = {"identifier": args.identifier} if args.identifier else None
    receipts, receipts_meta = fetch_paginated(
        session,
        "/api/consentmanager/v1/receipts",
        params=receipts_params,
        page_size=args.page_size,
        max_pages=args.max_pages,
    )
    print_summary("receipts", receipts, receipts_meta)
    results["receipts"] = {"records": receipts, "meta": receipts_meta}

    link_tokens, lt_meta = fetch_paginated(
        session,
        "/api/consentmanager/v1/linktokens",
        page_size=args.page_size,
        max_pages=args.max_pages,
    )
    print_summary("link_tokens", link_tokens, lt_meta)
    results["link_tokens"] = {"records": link_tokens, "meta": lt_meta}

    write_output(args.output, results)


if __name__ == "__main__":
    main()
