from __future__ import annotations

"""Helpers for turning messy extracted HTML values into normalized inputs."""

import re
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

# These are common mojibake sequences we already see in scraped price text.
_MOJIBAKE_FIXES = {
    "Â£": "£",
    "â‚¬": "€",
}

_CURRENCY_SYMBOLS = {
    "£": "GBP",
    "€": "EUR",
    "$": "USD",
}


def clean_text(value: Any) -> str | None:
    """Trim whitespace, collapse internal spacing, and repair common encoding artifacts."""

    if value is None:
        return None

    text = str(value)
    for bad, good in _MOJIBAKE_FIXES.items():
        text = text.replace(bad, good)

    text = " ".join(text.strip().split())
    return text or None


def normalize_price(value: Any) -> Decimal | None:
    """Parse a messy price string like '€1,299.99' into a Decimal."""

    if value is None:
        return None

    if isinstance(value, Decimal):
        return value

    if isinstance(value, (int, float)):
        return Decimal(str(value))

    text = clean_text(value)
    if text is None:
        return None

    compact = text.replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", compact)
    if match is None:
        return None

    try:
        return Decimal(match.group(0))
    except InvalidOperation:
        return None


def normalize_currency(value: Any, price_text: Any = None, default: str = "USD") -> str:
    """Prefer explicit currency codes and fall back to symbols found in price text."""

    text = clean_text(value)
    if text is not None:
        upper = text.upper()
        if upper in {"USD", "EUR", "GBP"}:
            return upper

    raw_price = clean_text(price_text) or ""
    for symbol, currency_code in _CURRENCY_SYMBOLS.items():
        if symbol in raw_price:
            return currency_code

    return default


def normalize_url(value: Any, base_url: str | None = None) -> str | None:
    """Resolve relative URLs, strip fragments, and normalize scheme/host casing."""

    text = clean_text(value)
    if text is None:
        return None

    resolved = urljoin(base_url or "", text)
    parts = urlsplit(resolved)
    normalized_path = re.sub(r"/{2,}", "/", parts.path)

    return urlunsplit(
        (
            parts.scheme.lower(),
            parts.netloc.lower(),
            normalized_path,
            parts.query,
            "",
        )
    )


def normalize_availability(value: Any) -> str:
    """Map source-specific availability text to a small stable vocabulary."""

    text = clean_text(value)
    if text is None:
        return "unknown"

    normalized = text.lower()
    if "out of stock" in normalized or "unavailable" in normalized:
        return "out_of_stock"
    if "in stock" in normalized or "available" in normalized:
        return "in_stock"

    return normalized.replace(" ", "_")
