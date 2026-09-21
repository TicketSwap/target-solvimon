"""Tests for the record-to-event mapping and the ingest client."""

from __future__ import annotations

import datetime
import logging
import typing as t

import pytest
from singer_sdk.exceptions import ConfigValidationError, FatalAPIError

from target_solvimon.client import MAX_EVENTS_PER_REQUEST
from target_solvimon.sinks import SolvimonSink
from target_solvimon.target import TargetSolvimon
from tests.stub_api import INGEST_PATH, INGEST_PATH_V2, StubSolvimonAPI

if t.TYPE_CHECKING:
    from collections.abc import Iterator

SCHEMA: dict[str, t.Any] = {
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "reference": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"},
        "customer_reference": {"type": "string"},
        "quantity": {"type": "integer"},
    },
}


@pytest.fixture(scope="module")
def stub_server() -> Iterator[StubSolvimonAPI]:
    """Run a stub Solvimon API for the duration of this module.

    Yields:
        The running stub.
    """
    api = StubSolvimonAPI()
    try:
        yield api
    finally:
        api.close()


@pytest.fixture
def stub_api(stub_server: StubSolvimonAPI) -> StubSolvimonAPI:
    """Hand each test a stub with no leftover state.

    Args:
        stub_server: The stub shared by this module.

    Returns:
        The stub, reset.
    """
    stub_server.reset()
    return stub_server


def build_sink(
    stub_api: StubSolvimonAPI,
    *,
    key_properties: list[str] | None = None,
    **config: t.Any,
) -> SolvimonSink:
    """Build a sink wired to the stub API.

    Args:
        stub_api: The stub to send events to.
        key_properties: Key properties of the test stream.
        config: Extra target configuration.

    Returns:
        A sink for the `tickets_sold` stream.
    """
    target = TargetSolvimon(
        config={
            "api_key": "test-api-key",
            "api_url": stub_api.url,
            "backoff_factor": 0,
            **config,
        },
        parse_env_config=False,
        validate_config=True,
    )
    return SolvimonSink(
        target,
        stream_name="tickets_sold",
        schema=SCHEMA,
        key_properties=key_properties or ["id"],
    )


def test_record_maps_onto_the_event_shape(stub_api: StubSolvimonAPI) -> None:
    """A record with the canonical fields is sent through as an event."""
    sink = build_sink(stub_api)
    record = {
        "id": "evt-1",
        "reference": "ticket_sold",
        "timestamp": datetime.datetime(2026, 9, 21, 12, 30, tzinfo=datetime.timezone.utc),
        "customer_reference": "cus-1",
        "quantity": 2,
    }

    sink.process_batch({"records": [sink.build_event(record)]})

    assert stub_api.requests[0].path == INGEST_PATH
    assert stub_api.events == [
        {
            "id": "evt-1",
            "reference": "ticket_sold",
            "timestamp": "2026-09-21T12:30:00+00:00",
            "customer_reference": "cus-1",
            "quantity": 2,
        }
    ]


def test_v2_posts_events_to_the_v2_endpoint(stub_api: StubSolvimonAPI) -> None:
    """`api_version: v2` switches both the path and the wrapper key."""
    sink = build_sink(stub_api, api_version="v2")

    sink.process_batch({"records": [sink.build_event({"id": "evt-1"})]})

    request = stub_api.requests[0]
    assert request.path == INGEST_PATH_V2
    assert list(request.body) == ["events"]


def test_credentials_are_sent_as_headers(stub_api: StubSolvimonAPI) -> None:
    """The API key, bearer token and platform id all reach the API."""
    sink = build_sink(stub_api, auth_token="tok-1", platform_id="plat-1")  # ruff: ignore[hardcoded-password-func-arg]

    sink.process_batch({"records": [sink.build_event({"id": "evt-1"})]})

    headers = stub_api.requests[0].headers
    assert headers["X-API-KEY"] == "test-api-key"
    assert headers["Authorization"] == "Bearer tok-1"
    assert headers["x-platform-id"] == "plat-1"
    assert headers["Content-Type"] == "application/json"


def test_missing_fields_fall_back(stub_api: StubSolvimonAPI) -> None:
    """Records without the canonical fields still produce valid events."""
    sink = build_sink(stub_api)

    event = sink.build_event({"quantity": 2, "_sdc_extracted_at": "2026-09-21T10:00:00Z"})

    # No id in the record, so it is derived from the key properties and is stable.
    assert event["id"] == sink.build_event({"quantity": 3})["id"]
    assert event["reference"] == "tickets_sold"
    assert event["timestamp"] == "2026-09-21T10:00:00Z"
    assert "_sdc_extracted_at" not in event


def test_event_id_can_be_left_out(stub_api: StubSolvimonAPI) -> None:
    """With `derive_event_id` off, records without an id field send no id at all."""
    sink = build_sink(stub_api, derive_event_id=False)

    assert "id" not in sink.build_event({"reference": "evt-1"})
    # A record that does carry an id still sends it.
    assert sink.build_event({"id": "evt-1"})["id"] == "evt-1"


def test_derived_id_follows_the_key_properties(stub_api: StubSolvimonAPI) -> None:
    """Two records with the same key get the same derived event id."""
    sink = build_sink(stub_api, key_properties=["customer_reference"])

    first = sink.build_event({"customer_reference": "cus-1", "quantity": 1})
    second = sink.build_event({"customer_reference": "cus-1", "quantity": 99})
    other = sink.build_event({"customer_reference": "cus-2", "quantity": 1})

    assert first["id"] == second["id"]
    assert first["id"] != other["id"]


def test_field_names_are_configurable(stub_api: StubSolvimonAPI) -> None:
    """Records that name the event fields differently can be remapped."""
    sink = build_sink(
        stub_api,
        event_id_field="sale_id",
        event_timestamp_field="sold_at",
        event_reference="ticket_sold",
    )

    event = sink.build_event({
        "sale_id": "sale-1",
        "sold_at": 1758456600,
        "reference": "ignored-by-the-static-reference",
    })

    assert event["id"] == "sale-1"
    assert event["reference"] == "ticket_sold"
    assert event["timestamp"] == "2025-09-21T12:10:00+00:00"


def test_extra_fields_as_meter_properties(stub_api: StubSolvimonAPI) -> None:
    """Unmapped fields can be turned into meter properties."""
    sink = build_sink(stub_api, extra_fields="meter_properties")

    event = sink.build_event({
        "id": "evt-1",
        "customer_reference": "cus-1",
        "seat": "A12",
        "quantity": 2,
        "empty": None,
    })

    # The customer is an event field of its own, not a meter property.
    assert event["customer_reference"] == "cus-1"
    assert event["meter_properties"] == [
        {"reference": "seat", "value": "A12"},
        {"reference": "quantity", "value": "2"},
    ]


def test_meter_properties_and_values_are_built_from_flat_fields(
    stub_api: StubSolvimonAPI,
) -> None:
    """Flat columns map onto the meter reference, properties and values."""
    sink = build_sink(
        stub_api,
        meter_reference="tickets",
        meter_properties=["country"],
        meter_values={
            "collected_payment": {
                "amount_field": "gross_amount",
                "currency_field": "currency",
            },
            "collected_payment_counter": {"number": "1"},
        },
        extra_fields="ignore",
    )

    event = sink.build_event({
        "id": "evt-1",
        "country": "NL",
        "currency": "EUR",
        "gross_amount": "12.50",
    })

    assert event["meter_reference"] == "tickets"
    assert event["meter_properties"] == [{"reference": "country", "value": "NL"}]
    assert event["meter_values"] == [
        {
            "reference": "collected_payment",
            "amount": {"currency": "EUR", "quantity": "12.50"},
        },
        {"reference": "collected_payment_counter", "number": "1"},
    ]


def test_meter_properties_can_rename_fields(stub_api: StubSolvimonAPI) -> None:
    """A mapping form sends a property under a name the record does not use."""
    sink = build_sink(
        stub_api,
        meter_properties={"country": "country_code"},
        extra_fields="ignore",
    )

    event = sink.build_event({"id": "evt-1", "country_code": "NL"})

    assert event["meter_properties"] == [{"reference": "country", "value": "NL"}]


def test_mapped_fields_are_not_also_sent_as_extras(stub_api: StubSolvimonAPI) -> None:
    """Fields claimed by the meter settings drop out of `extra_fields`."""
    sink = build_sink(
        stub_api,
        meter_properties=["country"],
        meter_values={"fee": {"amount_field": "fee", "currency": "EUR"}},
    )

    event = sink.build_event({
        "id": "evt-1",
        "country": "NL",
        "fee": "1.50",
        "seat": "A12",
    })

    # `country` and `fee` were mapped; only `seat` is left to pass through.
    assert event["seat"] == "A12"
    assert "country" not in event
    assert "fee" not in event
    assert event["meter_values"] == [
        {"reference": "fee", "amount": {"currency": "EUR", "quantity": "1.50"}}
    ]


def test_meter_values_without_data_are_left_out(stub_api: StubSolvimonAPI) -> None:
    """An amount the record has no value for is omitted, not sent as an empty one."""
    sink = build_sink(
        stub_api,
        meter_values={
            "refund": {"amount_field": "refund_amount", "currency_field": "currency"},
            "tickets": {"count_field": "ticket_count"},
        },
    )

    event = sink.build_event({"id": "evt-1", "currency": "EUR", "ticket_count": 2})

    assert event["meter_values"] == [{"reference": "tickets", "count": "2"}]


def test_incomplete_meter_values_fail_on_startup(stub_api: StubSolvimonAPI) -> None:
    """A half-specified value is reported instead of silently dropping it."""
    with pytest.raises(ConfigValidationError) as caught:
        build_sink(
            stub_api,
            meter_values={
                "collected_payment": {"amount_field": "gross_amount"},
                "counter": {},
            },
        )

    assert caught.value.errors == [
        (
            "`meter_values.collected_payment`: `amount_field` also needs `currency` "
            "or `currency_field`"
        ),
        (
            "`meter_values.counter`: expected exactly one of `amount_field`, "
            "`number`, `number_field`, `count` or `count_field`, got none"
        ),
    ]


def test_misspelled_meter_value_keys_fail_on_startup(
    stub_api: StubSolvimonAPI,
) -> None:
    """The config schema rejects a key that is not part of a meter value spec."""
    with pytest.raises(ConfigValidationError) as caught:
        build_sink(stub_api, meter_values={"counter": {"numbr": "1"}})

    assert "'numbr' was unexpected" in caught.value.errors[0]


def test_customer_can_be_set_for_every_record(stub_api: StubSolvimonAPI) -> None:
    """A static customer reference overrides the record field."""
    from_record = build_sink(stub_api).build_event({
        "id": "evt-1",
        "customer_reference": "cus-1",
    })
    static = build_sink(stub_api, customer_reference="cus-2").build_event({
        "id": "evt-1",
        "customer_reference": "cus-1",
    })

    assert from_record["customer_reference"] == "cus-1"
    assert static["customer_reference"] == "cus-2"
    # Records with no customer at all send none, and Solvimon rejects those.
    assert "customer_reference" not in build_sink(stub_api).build_event({"id": "e"})


def test_extra_fields_can_be_dropped(stub_api: StubSolvimonAPI) -> None:
    """Unmapped fields can be left out of the event entirely."""
    sink = build_sink(stub_api, extra_fields="ignore")

    event = sink.build_event({"id": "evt-1", "quantity": 2})

    assert set(event) == {"id", "reference", "timestamp"}


def test_batches_are_split_at_the_api_limit(stub_api: StubSolvimonAPI) -> None:
    """More events than the endpoint accepts are sent in several requests."""
    limit = MAX_EVENTS_PER_REQUEST["v1"]
    sink = build_sink(stub_api)
    events = [sink.build_event({"id": f"evt-{index}"}) for index in range(limit + 1)]

    sink.process_batch({"records": events})

    assert [len(request.body["meter_datas"]) for request in stub_api.requests] == [
        limit,
        1,
    ]
    assert len(stub_api.events) == limit + 1


def test_events_per_request_is_configurable(stub_api: StubSolvimonAPI) -> None:
    """The endpoint's limit can be overridden when Solvimon changes it."""
    sink = build_sink(stub_api, max_events_per_request=2)
    events = [sink.build_event({"id": f"evt-{index}"}) for index in range(5)]

    sink.process_batch({"records": events})

    assert [len(request.body["meter_datas"]) for request in stub_api.requests] == [
        2,
        2,
        1,
    ]


def test_events_per_request_follows_the_api_version(stub_api: StubSolvimonAPI) -> None:
    """Each endpoint version defaults to its own limit."""
    assert build_sink(stub_api).client.max_events_per_request == MAX_EVENTS_PER_REQUEST["v1"]
    assert (
        build_sink(stub_api, api_version="v2").client.max_events_per_request
        == MAX_EVENTS_PER_REQUEST["v2"]
    )


def test_max_size_defaults_to_the_api_limit(stub_api: StubSolvimonAPI) -> None:
    """A batch fills up at the endpoint's limit unless `batch_size_rows` says otherwise."""
    batch_size_rows = 10
    big_batch_size_rows = 5000

    assert build_sink(stub_api).max_size == MAX_EVENTS_PER_REQUEST["v1"]
    assert build_sink(stub_api, batch_size_rows=batch_size_rows).max_size == batch_size_rows
    # A bigger batch is allowed; `process_batch` splits it over several calls.
    assert build_sink(stub_api, batch_size_rows=big_batch_size_rows).max_size == big_batch_size_rows


def test_transient_failures_are_retried(stub_api: StubSolvimonAPI) -> None:
    """A rate limit or a 5xx is retried instead of failing the sync."""
    sink = build_sink(stub_api)
    transient_statuses = (429, 503)
    stub_api.queue_statuses(*transient_statuses)

    sink.process_batch({"records": [sink.build_event({"id": "evt-1"})]})

    assert len(stub_api.requests) == len(transient_statuses) + 1


def test_retries_are_bounded(stub_api: StubSolvimonAPI) -> None:
    """A destination that keeps failing eventually fails the sync."""
    sink = build_sink(stub_api, max_retries=1)
    stub_api.queue_statuses(503, 503)

    with pytest.raises(FatalAPIError, match="503"):
        sink.process_batch({"records": [sink.build_event({"id": "evt-1"})]})


def test_rejected_batches_fail_the_sync(stub_api: StubSolvimonAPI) -> None:
    """A 4xx is not retried and surfaces the API error body."""
    sink = build_sink(stub_api)
    stub_api.queue_statuses(400)

    with pytest.raises(FatalAPIError, match="stubbed failure"):
        sink.process_batch({"records": [sink.build_event({"id": "evt-1"})]})

    assert len(stub_api.requests) == 1


def test_timestamp_fallback_is_reported_once(
    stub_api: StubSolvimonAPI,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Stamping events with the ingestion time is warned about, but only once."""
    sink = build_sink(stub_api)

    with caplog.at_level(logging.WARNING):
        sink.build_event({"id": "evt-1"})
        sink.build_event({"id": "evt-2"})

    warnings = [record for record in caplog.records if "ingestion" in record.message]
    assert len(warnings) == 1
