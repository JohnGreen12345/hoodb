"""Small EODHD REST client with a local, token-free response cache.

Set EODHD_API_TOKEN in the process environment. Response bodies and request
metadata (with the token omitted) are cached under .eodhd_cache/ by default.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv

BASE_URL = "https://eodhd.com/api"
REPO_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=REPO_DIR / ".env", override=False)
DEFAULT_CACHE_DIR = REPO_DIR / ".eodhd_cache"


class EODHDError(RuntimeError):
    """Sanitized EODHD request error; never includes the request URL or token."""


class EODHDPlanError(EODHDError):
    """401/403 response: invalid credentials or endpoint unavailable on plan."""


class EODHDClient:
    def __init__(self, token: str | None = None, cache_dir: str | Path = DEFAULT_CACHE_DIR,
                 session: requests.Session | None = None, max_429_retries: int = 3):
        self._token = token if token is not None else os.environ.get("EODHD_API_TOKEN", "")
        if not self._token:
            raise EODHDError("EODHD_API_TOKEN is not set in this process environment.")
        self.cache_dir = Path(cache_dir)
        self.session = session or requests.Session()
        self.max_429_retries = max(0, int(max_429_retries))

    @staticmethod
    def _cache_key(endpoint: str, params: Mapping[str, Any]) -> str:
        safe = {k: v for k, v in params.items() if k.lower() not in {"api_token", "token", "apikey"}}
        payload = json.dumps({"endpoint": endpoint, "params": safe}, sort_keys=True, default=str)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def request_json(self, endpoint: str, params: Mapping[str, Any] | None = None) -> Any:
        endpoint = endpoint.lstrip("/")
        safe_params = dict(params or {})
        safe_params.pop("api_token", None)
        cache_file = self.cache_dir / f"{self._cache_key(endpoint, safe_params)}.json"
        if cache_file.exists():
            cached = json.loads(cache_file.read_text(encoding="utf-8"))
            if isinstance(cached, dict) and "_http_status" in cached:
                status = int(cached["_http_status"])
                if status in (401, 403):
                    raise EODHDPlanError(
                        f"Cached EODHD HTTP {status} for /{endpoint}: invalid token or endpoint not in plan."
                    )
                if status >= 400:
                    raise EODHDError(f"Cached EODHD HTTP {status} for /{endpoint}.")
                return cached.get("_body")
            return cached

        request_params = dict(safe_params)
        request_params["api_token"] = self._token
        url = f"{BASE_URL}/{endpoint}"
        for attempt in range(self.max_429_retries + 1):
            try:
                response = self.session.get(url, params=request_params, timeout=30)
            except requests.RequestException:
                raise EODHDError(f"Network/timeout error requesting /{endpoint}; URL details suppressed.") from None
            if response.status_code == 429 and attempt < self.max_429_retries:
                time.sleep(min(2 ** attempt, 8))
                continue
            try:
                body = response.json()
            except ValueError:
                body = {"_non_json_response": True}
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(json.dumps({"_http_status": response.status_code, "_body": body},
                                             ensure_ascii=False, indent=2), encoding="utf-8")
            if response.status_code in (401, 403):
                raise EODHDPlanError(
                    f"EODHD HTTP {response.status_code} for /{endpoint}: invalid token or endpoint not in plan."
                )
            if response.status_code >= 400:
                raise EODHDError(f"EODHD HTTP {response.status_code} for /{endpoint}.")
            if isinstance(body, dict) and body.get("_non_json_response"):
                raise EODHDError(f"EODHD returned non-JSON content for /{endpoint}.")
            return body
        raise EODHDError(f"EODHD HTTP 429 after retries for /{endpoint}.")

    def get_user_limits(self) -> Any:
        return self.request_json("user")

    def get_end_of_day(self, symbol: str, date_from: str, date_to: str) -> Any:
        return self.request_json(f"eod/{symbol}", {"from": date_from, "to": date_to, "fmt": "json"})

    @staticmethod
    def _us_symbol(symbol: str) -> str:
        return symbol if "." in symbol else f"{symbol}.US"

    def get_earnings_calendar(self, symbol: str, date_from: str, date_to: str) -> Any:
        return self.request_json("calendar/earnings", {
            "symbols": self._us_symbol(symbol), "from": date_from, "to": date_to, "fmt": "json"
        })

    def get_news(self, symbol: str, date_from: str, date_to: str, limit: int = 100) -> Any:
        return self.request_json("news", {
            "s": self._us_symbol(symbol), "from": date_from, "to": date_to, "limit": limit, "fmt": "json"
        })

    def get_exchange_symbols(self, exchange: str = "US", delisted: bool = True) -> Any:
        return self.request_json(f"exchange-symbol-list/{exchange}", {
            "delisted": 1 if delisted else 0, "fmt": "json"
        })

    def get_fundamentals(self, symbol: str, filter: str | None = None) -> Any:
        params: dict[str, Any] = {"fmt": "json"}
        if filter:
            params["filter"] = filter
        return self.request_json(f"v1.1/fundamentals/{symbol}", params)


def masked_url(endpoint: str, params: Mapping[str, Any] | None = None) -> str:
    """Return a printable URL with every credential value redacted."""
    values = dict(params or {})
    for key in list(values):
        if key.lower() in {"api_token", "token", "apikey"}:
            values[key] = "***"
    query = urlencode(values, doseq=True)
    return f"{BASE_URL}/{endpoint.lstrip('/')}" + (f"?{query}" if query else "")
