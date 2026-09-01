import argparse
import os

from onetrust_probes.common import OneTrustSession, fetch_paginated, get_env, print_summary, write_output


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe arbitrary OneTrust endpoints.")
    parser.add_argument("--endpoint", action="append", required=True, help="Endpoint path (repeatable)")
    parser.add_argument("--max-pages", type=int, default=1, help="Max pages per endpoint")
    parser.add_argument("--page-size", type=int, default=100, help="Page size")
    parser.add_argument("--output", type=str, default=None, help="Write combined JSON output")
    args = parser.parse_args()

    hostname = get_env("ONETRUST_HOSTNAME")
    client_id = get_env("ONETRUST_CLIENT_ID")
    client_secret = get_env("ONETRUST_CLIENT_SECRET")
    scopes = os.getenv("ONETRUST_SCOPES")

    session = OneTrustSession(hostname, client_id, client_secret, scopes=scopes)
    results = {}

    for endpoint in args.endpoint:
        records, meta = fetch_paginated(
            session,
            endpoint,
            page_size=args.page_size,
            max_pages=args.max_pages,
        )
        name = endpoint.strip("/").replace("/", "_") or "root"
        print_summary(name, records, meta)
        results[name] = {"endpoint": endpoint, "records": records, "meta": meta}

    write_output(args.output, results)


if __name__ == "__main__":
    main()
