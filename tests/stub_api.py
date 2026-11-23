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

INGEST_PATH = "/v1/ingest/meter-datas"
PREDICTION_PATH = f"{INGEST_PATH}?cost_prediction=true"
EVENT_PATH = "/v1/ingest/meter-data/"
DELETE_PATH = "/v1/ingest/meter-data/delete"
EVENTS_KEY = "meter_datas"

ERROR_BODY = {
    "type": "INVALID_REQUEST",
    "code": "BAD_REQUEST",
    "message": "stubbed failure",
}
MISSING_EVENT_BODY = {
    "type": "INVALID_REQUEST",
    "code": "RESOURCE_NOT_FOUND",
    "message": "The requested resource was not found",
}
MISSING_CUSTOMER_BODY = {
    "type": "RESOURCES_NOT_FOUND",
    "code": "NOT_FOUND",
    "message": "The requested CUSTOMER could not be found",
}


class StubRequest(t.NamedTuple):
    """One request captured by the stub."""

    path: str
    headers: dict[str, str]
    body: dict
    status: int


class StubSolvimonAPI:
    """A local HTTP server that records the ingest calls a target makes."""

    def __init__(self) -> None:
        """Start the server on a random port of the loopback interface."""
        self.requests: list[StubRequest] = []
        self._statuses: list[int] = []
        self._rejected_ids: set[str] = set()
        self._missing_references: set[str] = set()
        self._pending_ids: set[str] = set()
        self._stored: dict[str, dict] = {}
        self.looked_up_ids: list[str] = []
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
            event
            for request in self.requests
            if request.path != DELETE_PATH
            for event in request.body[EVENTS_KEY]
        ]

    @property
    def ingested_events(self) -> list[dict]:
        """Every event of the requests the stub accepted.

        Returns:
            The flattened list of accepted events.
        """
        return [
            event
            for request in self.requests
            if request.path != DELETE_PATH and request.status < 400
            for event in request.body[EVENTS_KEY]
        ]

    def reset(self) -> None:
        """Forget captured requests, queued statuses and rejected ids."""
        with self._lock:
            self.requests.clear()
            self._statuses.clear()
            self._rejected_ids.clear()
            self._missing_references.clear()
            self._pending_ids.clear()
            self._stored.clear()
            self.looked_up_ids.clear()

    def pend_predictions(self, *event_ids: str) -> None:
        """Answer these events as `PENDING` on ingest, and without a prediction on lookup.

        Args:
            event_ids: Ids of the events whose prediction is not final.
        """
        with self._lock:
            self._pending_ids.update(event_ids)

    def settle_predictions(self, *event_ids: str) -> None:
        """Answer the cost prediction of these events as `READY` from now on.

        Args:
            event_ids: Ids of the events whose prediction is final.
        """
        with self._lock:
            self._pending_ids.difference_update(event_ids)

    def _meter_data(self, event: dict) -> dict:
        event_id = event.get("id") or f"mdat-{event['reference']}"
        status = "PENDING" if event_id in self._pending_ids else "READY"
        return {**event, "id": event_id, "cost_prediction": {"response": "OK", "status": status}}

    @property
    def deleted_references(self) -> list[str]:
        """References of every delete request, in order.

        Returns:
            The references the target asked to delete.
        """
        return [
            request.body["reference"] for request in self.requests if request.path == DELETE_PATH
        ]

    def miss_references(self, *references: str) -> None:
        """Answer deletes of these references as if they have no live event.

        Args:
            references: References to report as not found.
        """
        with self._lock:
            self._missing_references.update(references)

    def reject_events(self, *event_ids: str) -> None:
        """Answer 404 for a missing customer to every request carrying these ids.

        Args:
            event_ids: Ids of the events to refuse.
        """
        with self._lock:
            self._rejected_ids.update(event_ids)

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

    def _response_for(self, path: str, body: dict) -> tuple[int, dict]:
        with self._lock:
            if path == DELETE_PATH and body.get("reference") in self._missing_references:
                return 404, MISSING_EVENT_BODY
            if path == DELETE_PATH:
                status = self._statuses.pop(0) if self._statuses else 200
                return status, ERROR_BODY if status >= 400 else {}
            events = body.get(EVENTS_KEY, [])
            if any(event.get("id") in self._rejected_ids for event in events):
                return 404, MISSING_CUSTOMER_BODY
            return (self._statuses.pop(0) if self._statuses else 200), ERROR_BODY

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
                status, error_body = stub._response_for(self.path, body)
                stub._record(
                    StubRequest(
                        path=self.path,
                        headers=dict(self.headers),
                        body=body,
                        status=status,
                    )
                )

                if status >= 400 or self.path == DELETE_PATH:
                    payload = json.dumps(error_body).encode()
                elif self.path == PREDICTION_PATH:
                    with stub._lock:
                        acknowledged = [stub._meter_data(event) for event in body[EVENTS_KEY]]
                        stub._stored.update({event["id"]: event for event in acknowledged})
                    payload = json.dumps({EVENTS_KEY: acknowledged}).encode()
                else:
                    acknowledged = [
                        {"reference": event.get("reference")} for event in body[EVENTS_KEY]
                    ]
                    payload = json.dumps({EVENTS_KEY: acknowledged}).encode()

                self._respond(status, payload)

            def do_GET(self) -> None:
                """Answer an event lookup with the event and its cost prediction."""
                event_id = self.path.removeprefix(EVENT_PATH)
                with stub._lock:
                    stub.looked_up_ids.append(event_id)
                    event = stub._stored.get(event_id)
                    body = stub._meter_data(event) if event else MISSING_EVENT_BODY
                    if event and event_id in stub._pending_ids:
                        del body["cost_prediction"]
                self._respond(200 if event else 404, json.dumps(body).encode())

            def _respond(self, status: int, payload: bytes) -> None:
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
