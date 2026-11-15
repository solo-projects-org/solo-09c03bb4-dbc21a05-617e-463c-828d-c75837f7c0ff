"""HTTP client for the county station mock API."""
from __future__ import annotations

from typing import Optional

import httpx


class StationError(Exception):
    """Base error for station interaction problems."""


class StaleCursorError(StationError):
    """Station reports the cursor belongs to a superseded revision."""

    def __init__(self, cursor_revision: int, current_revision: int) -> None:
        super().__init__(
            f"stale cursor: got revision {cursor_revision}, "
            f"station is now at revision {current_revision}"
        )
        self.cursor_revision = cursor_revision
        self.current_revision = current_revision


class StationReadError(StationError):
    """Network/timeout/5xx failure -- retryable for the current page."""


class StationClient:
    def __init__(self, base_url: str, timeout: float = 3.0) -> None:
        self._base = base_url.rstrip("/")
        self._timeout = timeout

    def _client(self) -> httpx.Client:
        return httpx.Client(base_url=self._base, timeout=self._timeout)

    def list_sections(self) -> list[str]:
        try:
            with self._client() as c:
                resp = c.get("/sections")
                resp.raise_for_status()
                return list(resp.json()["sections"])
        except (httpx.HTTPError, KeyError, ValueError) as exc:
            raise StationReadError(f"cannot list sections: {exc}") from exc

    def metadata(self, section: str, date: str) -> dict:
        try:
            with self._client() as c:
                resp = c.get("/metadata", params={"section": section, "date": date})
                resp.raise_for_status()
                return resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise StationReadError(f"metadata request failed: {exc}") from exc

    def fetch_page(
        self, section: str, date: str, cursor: Optional[str]
    ) -> tuple[dict, Optional[str]]:
        """Fetch one page. Returns (page_body, next_cursor).

        409 (old cursor after a correction) is surfaced as
        ``StaleCursorError`` and must not be retried.
        """
        params = {"section": section, "date": date}
        if cursor is not None:
            params["cursor"] = cursor
        try:
            with self._client() as c:
                resp = c.get("/observations", params=params)
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
            raise StationReadError(f"page read failed: {exc}") from exc
        except httpx.HTTPError as exc:
            raise StationReadError(f"page request failed: {exc}") from exc

        if resp.status_code == 409:
            try:
                detail = resp.json().get("detail", {})
                raise StaleCursorError(
                    detail.get("cursor_revision", -1),
                    detail.get("current_revision", -1),
                )
            except ValueError:
                raise StaleCursorError(-1, -1)
        if resp.status_code >= 500:
            raise StationReadError(f"station returned {resp.status_code}")
        if resp.status_code >= 400:
            raise StationError(f"station rejected request: {resp.status_code} {resp.text}")
        try:
            body = resp.json()
        except ValueError as exc:
            raise StationReadError(f"invalid JSON in page: {exc}") from exc
        return body, body.get("next_cursor")
