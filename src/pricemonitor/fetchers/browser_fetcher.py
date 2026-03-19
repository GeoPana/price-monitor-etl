from __future__ import annotations

"""Browser-based fetcher backed by Playwright for JavaScript-rendered pages."""

from pricemonitor.fetchers.base import BaseFetcher, FetchResponse


class BrowserFetcher(BaseFetcher):
    """Render pages in Chromium so scrapers can parse the final DOM, not just raw HTML."""

    def __init__(
        self,
        timeout_seconds: int = 10,
        *,
        headless: bool = True,
        wait_for_selector: str | None = None,
        wait_for_timeout_ms: int | None = None,
        user_agent: str | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.headless = headless
        self.wait_for_selector = wait_for_selector
        self.wait_for_timeout_ms = wait_for_timeout_ms
        self.user_agent = user_agent or "PriceMonitorETL/0.1"

    def fetch(self, url: str) -> FetchResponse:
        """Open a page in Chromium and return the rendered HTML snapshot."""

        try:
            from playwright.sync_api import Error as PlaywrightError
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise RuntimeError(
                "Playwright is not installed. Run `pip install -e .[dev]` "
                "and `playwright install chromium`."
            ) from exc

        timeout_ms = self.timeout_seconds * 1000

        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=self.headless)
                context = browser.new_context(user_agent=self.user_agent)
                page = context.new_page()

                response = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

                # If a source knows the element that appears after rendering, wait for it explicitly.
                if self.wait_for_selector:
                    page.wait_for_selector(self.wait_for_selector, timeout=timeout_ms)
                elif self.wait_for_timeout_ms is not None:
                    page.wait_for_timeout(self.wait_for_timeout_ms)
                else:
                    # Fallback for simple JS pages with no source-specific selector configured.
                    page.wait_for_load_state("networkidle", timeout=timeout_ms)

                rendered_html = page.content()
                final_url = page.url
                status_code = response.status if response is not None else 200
                content_type = response.headers.get("content-type") if response is not None else None

                context.close()
                browser.close()

                return FetchResponse(
                    url=final_url,
                    status_code=status_code,
                    text=rendered_html,
                    content_type=content_type,
                )
        except PlaywrightError as exc:
            raise RuntimeError(f"Browser fetch failed for {url}: {exc}") from exc
