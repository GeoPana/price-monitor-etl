from __future__ import annotations

"""HTTP fetcher implementation backed by requests."""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pricemonitor.fetchers.base import BaseFetcher, FetchResponse


class HttpFetcher(BaseFetcher):
    """Requests-based fetcher with a stable user agent, timeout, and retry policy."""

    def __init__(
        self,
        timeout_seconds: int = 10,
        user_agent: str | None = None,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.user_agent = user_agent or "PriceMonitorETL/0.1"
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})

        # Retry transient network errors so one refused connection does not force
        # Airflow to retry the entire task.
        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            status=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "HEAD"}),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

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
