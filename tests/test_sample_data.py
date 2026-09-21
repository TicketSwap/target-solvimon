"""Runs the sample ticket file through the target, as a real pipeline would."""

from __future__ import annotations

import io
import json
import typing as t
from pathlib import Path

import pytest
from singer_sdk.testing.runners import TargetTestRunner

from target_solvimon.target import TargetSolvimon
from tests.stub_api import StubSolvimonAPI

if t.TYPE_CHECKING:
    from collections.abc import Iterator

SAMPLE_DIR = Path(__file__).parents[1] / "sample_data"
SAMPLE_DATA = SAMPLE_DIR / "tickets.singer"
SAMPLE_CONFIG = SAMPLE_DIR / "config.sample.json"


@pytest.fixture
def stub_api() -> Iterator[StubSolvimonAPI]:
    """Run a stub Solvimon API for the duration of a test.

    Yields:
        The running stub.
    """
    api = StubSolvimonAPI()
    try:
        yield api
    finally:
        api.close()


# The event Solvimon's docs show for the `tickets` meter, which the first record of
# the sample file must reproduce. The API additionally requires a customer, so the
# sample carries one.
EXAMPLE_EVENT = {
    "meter_reference": "tickets",
    "timestamp": "2026-09-21T13:26:00+02:00",
    "reference": "9d80a2b7_9bea_4599_96c1_8dea3bbb911a",
    "meter_properties": [
        {"reference": "country", "value": "NL"},
        {"reference": "event_name", "value": "Test event"},
    ],
    "meter_values": [
        {
            "reference": "collected_payment",
            "amount": {"currency": "EUR", "quantity": "12"},
        },
        {"reference": "collected_payment_counter", "number": "1"},
        {"reference": "service_fee", "amount": {"currency": "EUR", "quantity": "12"}},
    ],
}


def test_sample_tickets_become_events(stub_api: StubSolvimonAPI) -> None:
    """The sample file loads with the sample config, as documented in the README."""
    config = json.loads(SAMPLE_CONFIG.read_text(encoding="utf-8"))
    config |= {"api_key": "test-api-key", "api_url": stub_api.url}

    runner = TargetTestRunner(
        target_class=TargetSolvimon,
        config=config,
        # Read the file here rather than passing `input_filepath`, which the SDK
        # runner leaves open.
        input_io=io.StringIO(SAMPLE_DATA.read_text(encoding="utf-8")),
        parse_env_config=False,
    )
    runner.sync_all()

    # One batch, since the file holds fewer records than the API limit.
    assert len(stub_api.requests) == 1
    assert EXAMPLE_EVENT | {"customer_reference": "partner_a"} in (
        stub_api.events
    )
    assert len(stub_api.events) == 5  # ruff: ignore[magic-value-comparison]
    assert all("id" not in event for event in stub_api.events)
    assert {event["meter_reference"] for event in stub_api.events} == {"tickets"}
    # The target forwards the tap's state once every record before it is loaded.
    assert runner.state_messages == [
        {"bookmarks": {"tickets": {"replication_key_value": "2026-09-21T13:26:00+02:00"}}}
    ]
