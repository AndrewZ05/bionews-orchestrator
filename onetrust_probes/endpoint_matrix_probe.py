import argparse
import json
import os
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests


def load_env_file() -> None:
    """Load environment variables from .env if present."""
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def get_token(base_url: str, client_id: str, client_secret: str) -> str:
    token_url = urljoin(base_url + "/", "api/access/v1/oauth/token")
    response = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    return payload["access_token"]


def normalize_base_url(hostname: str) -> str:
    if hostname.startswith("http://") or hostname.startswith("https://"):
        return hostname.rstrip("/")
    return f"https://{hostname}".rstrip("/")


def summarize_payload(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        for key in ("content", "data", "results", "items"):
            if isinstance(payload.get(key), list):
                items = payload.get(key, [])
                break
        else:
            items = []
    else:
        items = []

    sample_keys: List[str] = []
    if items and isinstance(items[0], dict):
        sample_keys = sorted(items[0].keys())

    return {
        "item_count": len(items),
        "sample_keys": sample_keys,
    }


def is_html_response(response: requests.Response) -> bool:
    content_type = (response.headers.get("Content-Type") or "").lower()
    if "text/html" in content_type:
        return True
    text_start = response.text[:200].lstrip().lower()
    return text_start.startswith("<!doctype html") or text_start.startswith("<html")


def probe_endpoint(
    base_url: str,
    token: str,
    method: str,
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    url = urljoin(base_url + "/", endpoint.lstrip("/"))
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    response = requests.request(
        method=method,
        url=url,
        params=params,
        json=json_body,
        headers=headers,
        timeout=60,
    )

    result: Dict[str, Any] = {
        "status_code": response.status_code,
        "method": method,
        "endpoint": endpoint,
    }

    if is_html_response(response):
        result["response_type"] = "html"
        result["summary"] = "HTML response (likely UI or wrong host/path)"
        return result

    if not response.content:
        result["response_type"] = "empty"
        return result

    try:
        payload = response.json()
    except ValueError:
        result["response_type"] = "text"
        result["summary"] = response.text[:200]
        return result

    result["response_type"] = "json"
    result["summary"] = summarize_payload(payload)
    if isinstance(payload, dict):
        result["json_keys"] = sorted(payload.keys())
    return result


def build_endpoints(page_size: int) -> List[Dict[str, Any]]:
    base_params = {"page": 0, "size": page_size}
    return [
        # Access & Audit
        {"name": "users_v2", "method": "GET", "endpoint": "/api/access/v2/users", "params": base_params},
        {"name": "login_history", "method": "GET", "endpoint": "/api/access/v1/login-history", "params": base_params},
        # DSR
        {"name": "dsr_request_queue", "method": "GET", "endpoint": "/api/datasubject/v2/requestqueues/en-us", "params": base_params},
        {"name": "dsr_subtasks_list", "method": "GET", "endpoint": "/api/datasubject/v2/subtasks", "params": base_params},
        # Consent (v1/v2/v4 variants)
        {"name": "data_subjects_v1", "method": "GET", "endpoint": "/api/consentmanager/v1/datasubjects/profiles", "params": base_params},
        {"name": "data_subjects_v4_basic", "method": "GET", "endpoint": "/rest/api/consent/v4/datasubjects/basic-details", "params": base_params},
        {"name": "data_subjects_v4_list", "method": "GET", "endpoint": "/api/consentmanager/v4/datasubjects", "params": base_params},
        {"name": "purposes_v1", "method": "GET", "endpoint": "/api/consentmanager/v1/purposes", "params": base_params},
        {"name": "purposes_v2", "method": "GET", "endpoint": "/v2/purposes", "params": base_params},
        {"name": "collection_points_v1", "method": "GET", "endpoint": "/api/consentmanager/v1/collectionpoints", "params": base_params},
        {"name": "collection_points_v2", "method": "GET", "endpoint": "/v2/collection-points", "params": base_params},
        {"name": "custom_preferences_v1", "method": "GET", "endpoint": "/api/consentmanager/v1/custompreferences", "params": base_params},
        {"name": "custom_preferences_v2", "method": "GET", "endpoint": "/v2/custom-preferences", "params": base_params},
        {"name": "preference_centers_v1", "method": "GET", "endpoint": "/api/consentmanager/v1/preferencecenters", "params": base_params},
        {"name": "preference_centers_v2", "method": "GET", "endpoint": "/v2/preference-centers", "params": base_params},
        {"name": "receipts_v1", "method": "GET", "endpoint": "/api/consentmanager/v1/receipts", "params": base_params},
        {"name": "receipts_v2", "method": "POST", "endpoint": "/receipts/v2", "json_body": base_params},
        {"name": "preferences_v2", "method": "GET", "endpoint": "/v2/preferences", "params": base_params},
        {"name": "link_tokens_v1", "method": "GET", "endpoint": "/api/consentmanager/v1/linktokens", "params": base_params},
        {"name": "link_tokens_v2", "method": "GET", "endpoint": "/v2/link-tokens", "params": base_params},
        # Assessments
        {"name": "assessments_v2", "method": "GET", "endpoint": "/api/assessment/v2/assessments", "params": base_params},
        # Cookie Consent (two path variants)
        {"name": "cookieconsent_cookies", "method": "GET", "endpoint": "/api/cookieconsent/v1/cookies", "params": base_params},
        {"name": "cookieconsent_domains", "method": "GET", "endpoint": "/api/cookieconsent/v1/domains", "params": base_params},
        {"name": "cookieconsent_scans", "method": "GET", "endpoint": "/api/cookieconsent/v1/scans", "params": base_params},
        {"name": "cookiemanager_cookies", "method": "GET", "endpoint": "/api/cookiemanager/v1/cookies", "params": base_params},
        {"name": "cookiemanager_domains", "method": "GET", "endpoint": "/api/cookiemanager/v1/domains", "params": base_params},
        {"name": "cookiemanager_scans", "method": "GET", "endpoint": "/api/cookiemanager/v1/scans", "params": base_params},
        # Inventory
        {"name": "inventory_v1", "method": "GET", "endpoint": "/api/inventory/v1/inventories", "params": base_params},
        {"name": "inventory_v2", "method": "GET", "endpoint": "/api/inventory/v2/inventories", "params": base_params},
        {"name": "personal_data_elements_v1", "method": "GET", "endpoint": "/api/inventory/v1/personaldataelements", "params": base_params},
        {"name": "personal_data_elements_v2", "method": "GET", "endpoint": "/api/inventory/v2/personaldataelements", "params": base_params},
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe OneTrust endpoints for access and sample data.")
    parser.add_argument("--host", type=str, default=None, help="Override ONETRUST_HOSTNAME")
    parser.add_argument("--page-size", type=int, default=1, help="Page size for list endpoints")
    parser.add_argument("--output", type=str, default=None, help="Write JSON summary to file")
    args = parser.parse_args()

    load_env_file()

    hostname = args.host or get_env("ONETRUST_HOSTNAME")
    client_id = get_env("ONETRUST_CLIENT_ID")
    client_secret = get_env("ONETRUST_CLIENT_SECRET")

    base_url = normalize_base_url(hostname)
    token = get_token(base_url, client_id, client_secret)

    results: Dict[str, Any] = {}
    for entry in build_endpoints(args.page_size):
        name = entry["name"]
        result = probe_endpoint(
            base_url=base_url,
            token=token,
            method=entry["method"],
            endpoint=entry["endpoint"],
            params=entry.get("params"),
            json_body=entry.get("json_body"),
        )
        results[name] = result
        print(f"{name}: {result['status_code']} ({result.get('response_type')})")

    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(results, handle, indent=2)


if __name__ == "__main__":
    main()
