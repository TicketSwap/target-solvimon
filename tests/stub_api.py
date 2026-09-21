"""An in-process stub of the Solvimon ingest API.

Tests run the target against this instead of the real API, so they never need
credentials and never write events to Solvimon.
"""

from __future__ import annotations

import json
import sys
import threading
import typing as t
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

INGEST_PATH = "/v1/events/ingest-batch"
INGEST_PATH_V2 = "/v2/events/ingest-batch"
EVENTS_KEYS = ("meter_datas", "events")

ERROR_BODY = {
    "type": "INVALID_REQUEST",
    "code": "BAD_REQUEST",
    "message": "stubbed failure",
}


def events_key(body: dict) -> str:
    """Return the key a request body carries its events under.

    Args:
        body: The decoded request body.

    Returns:
        The wrapper key used by the API version the target called.
    """
    return next((key for key in EVENTS_KEYS if key in body), EVENTS_KEYS[0])


class StubRequest(t.NamedTuple):
    """One request captured by the stub."""

    path: str
    headers: dict[str, str]
    body: dict


class StubSolvimonAPI:
    """A local HTTP server that records the ingest calls a target makes."""

    def __init__(self) -> None:
        """Start the server on a random port of the loopback interface."""
        self.requests: list[StubRequest] = []
        self._statuses: list[int] = []
        self._lock = threading.Lock()
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._build_handler())
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        self._thread.start()

    @property
    def url(self) -> str:
        """Base URL to configure the target with.

        Returns:
            The base URL of the running stub.
        """
        host, port = self._server.server_address[:2]
        return f"http://{host!s}:{port}"

    @property
    def events(self) -> list[dict]:
        """Every event the target sent, across all requests.

        Returns:
            The flattened list of ingested events.
        """
        return [
            event for request in self.requests for event in request.body[events_key(request.body)]
        ]

    def reset(self) -> None:
        """Forget captured requests and queued statuses, for reuse across tests."""
        with self._lock:
            self.requests.clear()
            self._statuses.clear()

    def queue_statuses(self, *statuses: int) -> None:
        """Respond with these status codes before falling back to 200.

        Args:
            statuses: Status codes to return, in order.
        """
        with self._lock:
            self._statuses.extend(statuses)

    def close(self) -> None:
        """Shut the server down."""
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def _next_status(self) -> int:
        with self._lock:
            return self._statuses.pop(0) if self._statuses else 200

    def _record(self, request: StubRequest) -> None:
        with self._lock:
            self.requests.append(request)

    def _build_handler(self) -> type[BaseHTTPRequestHandler]:
        stub = self

        class Handler(BaseHTTPRequestHandler):
            """Handler bound to the enclosing stub instance."""

            protocol_version = "HTTP/1.1"

            def do_POST(self) -> None:
                """Capture the request and answer with the queued status."""
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}")
                stub._record(
                    StubRequest(
                        path=self.path,
                        headers=dict(self.headers),
                        body=body,
                    )
                )

                status = stub._next_status()
                if status >= 400:  # ruff: ignore[magic-value-comparison]
                    payload = json.dumps(ERROR_BODY).encode()
                else:
                    key = events_key(body)
                    payload = json.dumps({
                        key: [{"reference": event.get("reference")} for event in body.get(key, [])]
                    }).encode()

                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            @override
            def log_message(self, format: str, *args: t.Any) -> None:
                """Keep the test output clean.

                Args:
                    format: Unused format string.
                    args: Unused format arguments.
                """

        return Handler
