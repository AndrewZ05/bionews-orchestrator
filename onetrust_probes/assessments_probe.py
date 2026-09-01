import argparse
import os

from onetrust_probes.common import OneTrustSession, fetch_paginated, get_env, print_summary, write_output


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe OneTrust assessments endpoints.")
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

    assessments, a_meta = fetch_paginated(
        session,
        "/api/assessment/v2/assessments",
        page_size=args.page_size,
        max_pages=args.max_pages,
    )
    print_summary("assessments", assessments, a_meta)
    results["assessments"] = {"records": assessments, "meta": a_meta}

    if assessments:
        assessment_id = assessments[0].get("assessmentId")
        if assessment_id:
            endpoint = f"/api/assessment/v2/assessments/{assessment_id}/export"
            response = session.request("GET", endpoint)
            response.raise_for_status()
            detail = response.json() if response.content else {}
            results["assessment_export"] = {"assessmentId": assessment_id, "payload": detail}

    write_output(args.output, results)


if __name__ == "__main__":
    main()
