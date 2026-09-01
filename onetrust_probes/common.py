import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests


def get_env(name: str, default: Optional[str] = None, required: bool = True) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def normalize_base_url(hostname: str) -> str:
    if hostname.startswith("http://") or hostname.startswith("https://"):
        return hostname.rstrip("/")
    return f"https://{hostname}".rstrip("/")


@dataclass
class TokenInfo:
    access_token: str
    expires_at: float
    scope: str
    token_type: str


class OneTrustSession:
    def __init__(self, hostname: str, client_id: str, client_secret: str, scopes: Optional[str] = None):
        self.base_url = normalize_base_url(hostname)
        self.client_id = client_id
        self.client_secret = client_secret
        self.scopes = scopes
        self._token_info: Optional[TokenInfo] = None
        self._session = requests.Session()

    def get_token(self) -> TokenInfo:
        if self._token_info and time.time() < (self._token_info.expires_at - 60):
            return self._token_info

        token_url = urljoin(self.base_url + "/", "api/access/v1/oauth/token")
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        if self.scopes:
            data["scope"] = self.scopes

        response = self._session.post(
            token_url,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        expires_in = int(payload.get("expires_in", 3600))
        self._token_info = TokenInfo(
            access_token=payload["access_token"],
            expires_at=time.time() + expires_in,
            scope=payload.get("scope", ""),
            token_type=payload.get("token_type", "Bearer"),
        )
        return self._token_info

    def request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        token = self.get_token()
        url = urljoin(self.base_url + "/", endpoint.lstrip("/"))
        request_headers = {
            "Authorization": f"{token.token_type} {token.access_token}",
            "Accept": "application/json",
        }
        if headers:
            request_headers.update(headers)
        return self._session.request(
            method=method,
            url=url,
            params=params,
            json=json_body,
            headers=request_headers,
            timeout=60,
        )


def parse_collection(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("content", "data", "results", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []


def fetch_paginated(
    session: OneTrustSession,
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
    page_size: int = 100,
    max_pages: int = 1,
    page_param: str = "page",
    size_param: str = "size",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    params = dict(params or {})
    params[size_param] = page_size
    all_records: List[Dict[str, Any]] = []
    meta: Dict[str, Any] = {"pages_fetched": 0}

    for page in range(max_pages):
        params[page_param] = page
        response = session.request("GET", endpoint, params=params)
        response.raise_for_status()
        payload = response.json() if response.content else {}
        records = parse_collection(payload)
        all_records.extend(records)
        meta["pages_fetched"] += 1
        meta["last_response_keys"] = list(payload.keys()) if isinstance(payload, dict) else []
        meta["last_response_size"] = len(records)
        if not records:
            break
    return all_records, meta


def print_summary(name: str, records: List[Dict[str, Any]], meta: Dict[str, Any]) -> None:
    count = len(records)
    sample_keys = sorted(records[0].keys()) if records else []
    print(f"[{name}] records={count} pages={meta.get('pages_fetched', 0)}")
    if sample_keys:
        print(f"[{name}] sample_keys={sample_keys}")


def write_output(path: Optional[str], payload: Any) -> None:
    if not path:
        return
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)
