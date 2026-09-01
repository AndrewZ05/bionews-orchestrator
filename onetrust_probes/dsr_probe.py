import argparse
import os

from onetrust_probes.common import OneTrustSession, fetch_paginated, get_env, print_summary, write_output


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe OneTrust DSR endpoints.")
    parser.add_argument("--language", type=str, default="en-us", help="Language code for request queue")
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

    queue_endpoint = f"/api/datasubject/v2/requestqueues/{args.language}"
    requests_list, rq_meta = fetch_paginated(
        session,
        queue_endpoint,
        page_size=args.page_size,
        max_pages=args.max_pages,
    )
    print_summary("dsr_requests", requests_list, rq_meta)
    results["dsr_requests"] = {"records": requests_list, "meta": rq_meta}

    if requests_list:
        first = requests_list[0]
        ref_id = first.get("requestQueueRefId") or first.get("requestQueueId")
        if ref_id:
            subtasks, st_meta = fetch_paginated(
                session,
                f"/api/datasubject/v2/requestqueues/{ref_id}/subtasks",
                page_size=args.page_size,
                max_pages=args.max_pages,
            )
            print_summary("dsr_subtasks", subtasks, st_meta)
            results["dsr_subtasks"] = {"records": subtasks, "meta": st_meta}

            audit, au_meta = fetch_paginated(
                session,
                f"/api/datasubject/v2/requestqueues/{ref_id}/requesthistory",
                page_size=args.page_size,
                max_pages=args.max_pages,
            )
            print_summary("dsr_request_audit", audit, au_meta)
            results["dsr_request_audit"] = {"records": audit, "meta": au_meta}

    write_output(args.output, results)


if __name__ == "__main__":
    main()
