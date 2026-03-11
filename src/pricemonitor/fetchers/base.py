from __future__ import annotations

"""Fetcher abstractions for retrieving remote source content."""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(slots=True)
class FetchResponse:
    """Normalized HTTP-like response payload returned by fetchers."""

    url: str
    status_code: int
    text: str
    content_type: str | None = None


class BaseFetcher(ABC):
    """Interface for source fetchers."""

    @abstractmethod
    def fetch(self, url: str) -> FetchResponse:
        raise NotImplementedError
