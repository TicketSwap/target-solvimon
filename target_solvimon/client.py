"""HTTP client for the Solvimon event ingest API.

Docs: https://docs.solvimon.com/api-docs/event-api/post-v-2-events-ingest-batch
"""

from __future__ import annotations

import typing as t

import requests
from requests.adapters import HTTPAdapter
from singer_sdk.exceptions import FatalAPIError
from singer_sdk.singerlib.json import serialize_json
from urllib3.util import Retry

if t.TYPE_CHECKING:
    import logging

# Default to the TEST environment: a half-configured pipeline should never write
# billable events to production.
DEFAULT_API_URL = "https://test.api.solvimon.com"
# The v2 endpoint the API docs describe answers 404 on both the test and the live
# host, so default to the v1 batch endpoint that is actually served. Both take the
# same events, under a different key.
API_VERSIONS = ("v1", "v2")
DEFAULT_API_VERSION = "v1"
INGEST_BATCH_PATHS = {
    "v1": "/v1/events/ingest-batch",
    "v2": "/v2/events/ingest-batch",
}
INGEST_BATCH_KEYS = {"v1": "meter_datas", "v2": "events"}

# Events the endpoint accepts per call. The v1 batch endpoint tops out far below the
# 1000 the v2 docs describe, and neither limit is discoverable, so `max_events_per_request`
# overrides these when Solvimon changes them.
MAX_EVENTS_PER_REQUEST = {"v1": 50, "v2": 1000}

DEFAULT_TIMEOUT = 60
DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_FACTOR = 2

# The ingest service is rate limited and horizontally scaled, so these statuses mean
# "try again later" rather than "this payload is wrong".
RETRYABLE_STATUS_CODES = (408, 425, 429, 500, 502, 503, 504)

# API error bodies are small, but a proxy may return a full HTML page.
ERROR_BODY_MAX_CHARS = 1000


class SolvimonClient:
    """Thin ``requests`` wrapper around the Solvimon event ingest API."""

    def __init__(  # ruff: ignore[too-many-arguments]
        self,
        *,
        logger: logging.Logger,
        api_key: str,
        api_url: str = DEFAULT_API_URL,
        api_version: str = DEFAULT_API_VERSION,
        auth_token: str | None = None,
        platform_id: str | None = None,
        max_events_per_request: int | None = None,
        timeout: int = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_factor: int = DEFAULT_BACKOFF_FACTOR,
    ) -> None:
        """Initialize the client.

        Args:
            logger: Logger to report retries and API failures on.
            api_key: Value of the ``X-API-KEY`` header.
            api_url: Base URL of the Solvimon API (test or live environment).
            api_version: Version of the batch ingest endpoint to call.
            max_events_per_request: Events to send per call, defaulting to the
                endpoint's own limit.
            auth_token: Optional bearer token for the ``Authorization`` header.
            platform_id: Optional value of the ``x-platform-id`` header.
            timeout: Per-request timeout in seconds.
            max_retries: Attempts to retry a request that failed transiently.
            backoff_factor: Exponential backoff factor between retries, in seconds.
        """
        self.logger = logger
        self.api_url = api_url.rstrip("/")
        self.api_version = api_version
        self.events_key = INGEST_BATCH_KEYS[api_version]
        self.max_events_per_request = max_events_per_request or MAX_EVENTS_PER_REQUEST[api_version]
        self.timeout = timeout

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-API-KEY": api_key,
        }
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        if platform_id:
            headers["x-platform-id"] = platform_id

        self.session = requests.Session()
        self.session.headers.update(headers)

        # Retry inside the connection pool so a rate limit or a rolling deploy on the
        # ingest service does not fail a whole sync.
        retry = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=RETRYABLE_STATUS_CODES,
            allowed_methods=frozenset({"POST"}),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    @property
    def ingest_batch_url(self) -> str:
        """URL of the batch ingest endpoint.

        Returns:
            The fully qualified endpoint URL.
        """
        return f"{self.api_url}{INGEST_BATCH_PATHS[self.api_version]}"

    def ingest_events(self, events: list[dict]) -> dict:
        """Send one batch of events to the ingest endpoint.

        Args:
            events: At most :attr:`max_events_per_request` event objects.

        Returns:
            The decoded API response, or an empty dict if the body was not JSON.

        Raises:
            ValueError: If more events than the API accepts are passed in.
            FatalAPIError: If the API rejected the batch, or kept failing after all
                retries were exhausted.
        """
        if len(events) > self.max_events_per_request:
            msg = (
                f"Cannot ingest {len(events)} events in a single request; this endpoint "
                f"accepts at most {self.max_events_per_request}"
            )
            raise ValueError(msg)

        body = serialize_json({self.events_key: events}).encode()
        response = self.session.post(
            self.ingest_batch_url,
            data=body,
            timeout=self.timeout,
        )

        if not response.ok:
            # Retries are exhausted by the time a retryable status reaches this point.
            detail = response.text[:ERROR_BODY_MAX_CHARS].strip()
            msg = (
                f"Solvimon rejected a batch of {len(events)} events: "
                f"{response.status_code} {response.reason}: {detail}"
            )
            raise FatalAPIError(msg)

        try:
            return t.cast("dict", response.json())
        except ValueError:
            self.logger.warning(
                "Solvimon returned a non-JSON body with status %s",
                response.status_code,
            )
            return {}

    def close(self) -> None:
        """Release the underlying HTTP connection pool."""
        self.session.close()
