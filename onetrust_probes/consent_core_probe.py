import argparse
import os

from onetrust_probes.common import OneTrustSession, fetch_paginated, get_env, print_summary, write_output


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe OneTrust consent core endpoints.")
    parser.add_argument("--max-pages", type=int, default=1, help="Max pages per endpoint")
    parser.add_argument("--page-size", type=int, default=100, help="Page size")
    parser.add_argument("--output", type=str, default=None, help="Write combined JSON output")
    parser.add_argument("--preference-center-state", type=str, default=None, help="Optional state param")
    args = parser.parse_args()

    hostname = get_env("ONETRUST_HOSTNAME")
    client_id = get_env("ONETRUST_CLIENT_ID")
    client_secret = get_env("ONETRUST_CLIENT_SECRET")
    scopes = os.getenv("ONETRUST_SCOPES")

    session = OneTrustSession(hostname, client_id, client_secret, scopes=scopes)
    results = {}

    data_subjects, ds_meta = fetch_paginated(
        session,
        "/api/consentmanager/v1/datasubjects/profiles",
        page_size=args.page_size,
        max_pages=args.max_pages,
    )
    print_summary("data_subjects", data_subjects, ds_meta)
    results["data_subjects"] = {"records": data_subjects, "meta": ds_meta}

    purposes, purposes_meta = fetch_paginated(
        session,
        "/api/consentmanager/v1/purposes",
        page_size=args.page_size,
        max_pages=args.max_pages,
    )
    print_summary("purposes", purposes, purposes_meta)
    results["purposes"] = {"records": purposes, "meta": purposes_meta}

    collection_points, cp_meta = fetch_paginated(
        session,
        "/api/consentmanager/v1/collectionpoints",
        page_size=args.page_size,
        max_pages=args.max_pages,
    )
    print_summary("collection_points", collection_points, cp_meta)
    results["collection_points"] = {"records": collection_points, "meta": cp_meta}

    preference_centers, pc_meta = fetch_paginated(
        session,
        "/api/consentmanager/v1/preferencecenters",
        page_size=args.page_size,
        max_pages=args.max_pages,
    )
    print_summary("preference_centers", preference_centers, pc_meta)
    results["preference_centers"] = {"records": preference_centers, "meta": pc_meta}

    if preference_centers:
        first_id = preference_centers[0].get("id")
        if first_id:
            endpoint = f"/api/consentmanager/v1/preferencecenters/{first_id}/preferences"
            params = {"state": args.preference_center_state} if args.preference_center_state else None
            response = session.request("GET", endpoint, params=params)
            response.raise_for_status()
            detail = response.json() if response.content else {}
            results["preference_center_detail"] = {
                "id": first_id,
                "payload": detail,
            }

    write_output(args.output, results)


if __name__ == "__main__":
    main()
