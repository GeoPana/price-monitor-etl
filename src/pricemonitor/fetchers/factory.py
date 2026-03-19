from __future__ import annotations

"""Factory helpers for selecting the correct fetcher per source."""

from pricemonitor.config import SourceSettings
from pricemonitor.fetchers.base import BaseFetcher
from pricemonitor.fetchers.browser_fetcher import BrowserFetcher
from pricemonitor.fetchers.http_fetcher import HttpFetcher


def create_fetcher(source_settings: SourceSettings) -> BaseFetcher:
    """Build the fetcher configured for a source."""

    if source_settings.fetcher == "http":
        return HttpFetcher(
            timeout_seconds=source_settings.timeout_seconds,
            user_agent=source_settings.user_agent,
        )

    if source_settings.fetcher == "browser":
        return BrowserFetcher(
            timeout_seconds=source_settings.timeout_seconds,
            headless=source_settings.browser_headless,
            wait_for_selector=source_settings.browser_wait_for_selector,
            wait_for_timeout_ms=source_settings.browser_wait_for_timeout_ms,
            user_agent=source_settings.user_agent,
        )

    raise ValueError(
        f"Unsupported fetcher '{source_settings.fetcher}' for source '{source_settings.name}'."
    )
