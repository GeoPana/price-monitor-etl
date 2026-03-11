from __future__ import annotations

"""HTTP fetcher implementation backed by requests."""

import requests

from pricemonitor.fetchers.base import BaseFetcher, FetchResponse


class HttpFetcher(BaseFetcher):
    """Thin requests-based fetcher with a stable user agent and timeout."""

    def __init__(self, timeout_seconds: int = 10, user_agent: str | None = None) -> None:
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent or "PriceMonitorETL/0.1"

    def fetch(self, url: str) -> FetchResponse:
        response = requests.get(
            url,
            timeout=self.timeout_seconds,
            headers={"User-Agent": self.user_agent},
        )
        response.raise_for_status()
        return FetchResponse(
            url=str(response.url),
            status_code=response.status_code,
            text=response.text,
            content_type=response.headers.get("Content-Type"),
        )
