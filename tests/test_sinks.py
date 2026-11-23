"""Tests for the record-to-event mapping and the ingest client."""

from __future__ import annotations

import datetime as dt
import gzip
import json
import logging
import typing as t

import pytest
from s3fs import S3FileSystem
from singer_sdk.exceptions import ConfigValidationError, FatalAPIError

from target_solvimon.client import MAX_EVENTS_PER_REQUEST
from target_solvimon.sinks import SolvimonSink
from target_solvimon.target import TargetSolvimon
from tests.stub_api import (
    DELETE_PATH,
    INGEST_PATH,
    PREDICTION_PATH,
    StubSolvimonAPI,
)

if t.TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

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
    stream_name: str = "tickets_sold",
    **config: t.Any,
) -> SolvimonSink:
    """Build a sink wired to the stub API.

    Args:
        stub_api: The stub to send events to.
        key_properties: Key properties of the test stream.
        stream_name: Name of the test stream.
        config: Extra target configuration.

    Returns:
        A sink for the test stream.
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
        stream_name=stream_name,
        schema=SCHEMA,
        key_properties=key_properties or ["id"],
    )


def send(sink: SolvimonSink, records: list[dict]) -> None:
    """Stage records the way the SDK does, then send them as one batch.

    Args:
        sink: The sink to send through.
        records: Records of the batch.
    """
    context: dict = {}
    for record in records:
        sink.process_record(record, context)
    sink.process_batch(context)


def test_record_maps_onto_the_event_shape(stub_api: StubSolvimonAPI) -> None:
    """A record with the canonical fields is sent through as an event."""
    sink = build_sink(stub_api)
    record = {
        "id": "evt-1",
        "reference": "ticket_sold",
        "timestamp": dt.datetime(2026, 9, 21, 12, 30, tzinfo=dt.UTC),
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


def test_credentials_are_sent_as_headers(stub_api: StubSolvimonAPI) -> None:
    """The API key, bearer token and platform id all reach the API."""
    sink = build_sink(stub_api, auth_token="tok-1", platform_id="plat-1")

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

    event = sink.build_event(
        {
            "sale_id": "sale-1",
            "sold_at": 1758456600,
            "reference": "ignored-by-the-static-reference",
        }
    )

    assert event["id"] == "sale-1"
    assert event["reference"] == "ticket_sold"
    assert event["timestamp"] == "2025-09-21T12:10:00+00:00"


def test_extra_fields_as_meter_properties(stub_api: StubSolvimonAPI) -> None:
    """Unmapped fields can be turned into meter properties."""
    sink = build_sink(stub_api, extra_fields="meter_properties")

    event = sink.build_event(
        {
            "id": "evt-1",
            "customer_reference": "cus-1",
            "seat": "A12",
            "quantity": 2,
            "empty": None,
        }
    )

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

    event = sink.build_event(
        {
            "id": "evt-1",
            "country": "NL",
            "currency": "EUR",
            "gross_amount": "12.50",
        }
    )

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

    event = sink.build_event(
        {
            "id": "evt-1",
            "country": "NL",
            "fee": "1.50",
            "seat": "A12",
        }
    )

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
    from_record = build_sink(stub_api).build_event(
        {
            "id": "evt-1",
            "customer_reference": "cus-1",
        }
    )
    static = build_sink(stub_api, customer_reference="cus-2").build_event(
        {
            "id": "evt-1",
            "customer_reference": "cus-1",
        }
    )

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
    """More events than the API accepts are sent in several requests."""
    sink = build_sink(stub_api)
    events = [
        sink.build_event({"id": f"evt-{index}"}) for index in range(MAX_EVENTS_PER_REQUEST + 1)
    ]

    sink.process_batch({"records": events})

    assert [len(request.body["meter_datas"]) for request in stub_api.requests] == [
        MAX_EVENTS_PER_REQUEST,
        1,
    ]
    assert len(stub_api.events) == MAX_EVENTS_PER_REQUEST + 1


def test_max_size_defaults_to_the_api_limit(stub_api: StubSolvimonAPI) -> None:
    """A batch fills up at 1000 records unless `batch_size_rows` says otherwise."""
    batch_size_rows = 10

    assert build_sink(stub_api).max_size == MAX_EVENTS_PER_REQUEST
    assert build_sink(stub_api, batch_size_rows=batch_size_rows).max_size == batch_size_rows


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


def test_rejected_events_are_skipped_with_a_warning(
    stub_api: StubSolvimonAPI,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A refused batch is split until only the refused events are left out."""
    sink = build_sink(stub_api)
    ids = [f"evt-{index}" for index in range(8)]
    stub_api.reject_events("evt-2", "evt-5")

    with caplog.at_level(logging.WARNING):
        sink.process_batch({"records": [sink.build_event({"id": id_}) for id_ in ids]})

    assert sorted(event["id"] for event in stub_api.ingested_events) == [
        id_ for id_ in ids if id_ not in {"evt-2", "evt-5"}
    ]
    warnings = [record.message for record in caplog.records if "Skipping event" in record.message]
    assert len(warnings) == 2
    assert all("RESOURCES_NOT_FOUND" in message for message in warnings)
    assert '"id":"evt-2"' in warnings[0]
    assert '"id":"evt-5"' in warnings[1]


@pytest.mark.parametrize("status", [401, 404])
def test_request_failures_fail_the_sync(stub_api: StubSolvimonAPI, status: int) -> None:
    """An auth error or a 404 without a missing resource is not split and fails the sync."""
    sink = build_sink(stub_api)
    stub_api.queue_statuses(status)

    with pytest.raises(FatalAPIError, match="stubbed failure"):
        sink.process_batch({"records": [sink.build_event({"id": f"evt-{i}"}) for i in range(4)]})

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


def test_creates_send_no_deletes_and_drop_the_action(stub_api: StubSolvimonAPI) -> None:
    """`create` records are only ingested, and the action field never reaches Solvimon."""
    sink = build_sink(stub_api, action_field="action")

    send(sink, [{"id": "evt-1", "action": "create"}, {"id": "evt-2"}])

    assert stub_api.deleted_references == []
    assert [event["id"] for event in stub_api.events] == ["evt-1", "evt-2"]
    assert all("action" not in event for event in stub_api.events)


def test_replace_deletes_the_reference_before_ingesting(stub_api: StubSolvimonAPI) -> None:
    """A `replace` record deletes the live event with its reference, then ingests it."""
    sink = build_sink(stub_api, action_field="action")

    send(sink, [{"id": "evt-1", "reference": "ticket_1", "action": "replace"}])

    assert [request.path for request in stub_api.requests] == [DELETE_PATH, INGEST_PATH]
    assert stub_api.deleted_references == ["ticket_1"]
    assert stub_api.ingested_events[0]["reference"] == "ticket_1"
    assert "action" not in stub_api.ingested_events[0]


def test_replace_without_a_live_event_is_still_ingested(stub_api: StubSolvimonAPI) -> None:
    """A delete answered with `RESOURCE_NOT_FOUND` counts as done."""
    sink = build_sink(stub_api, action_field="action")
    stub_api.miss_references("ticket_1")

    send(sink, [{"id": "evt-1", "reference": "ticket_1", "action": "replace"}])

    assert stub_api.requests[0].status == 404
    assert [event["reference"] for event in stub_api.ingested_events] == ["ticket_1"]


def test_deletes_of_a_chunk_run_before_its_ingest(stub_api: StubSolvimonAPI) -> None:
    """A mixed chunk deletes every replaced reference first, then ingests all its events."""
    sink = build_sink(stub_api, action_field="action")
    actions = ["create", "replace", "create", "replace"]

    send(
        sink,
        [
            {"id": f"evt-{index}", "reference": f"ticket_{index}", "action": action}
            for index, action in enumerate(actions)
        ],
    )

    assert [request.path for request in stub_api.requests] == [
        DELETE_PATH,
        DELETE_PATH,
        INGEST_PATH,
    ]
    assert stub_api.deleted_references == ["ticket_1", "ticket_3"]
    assert [event["reference"] for event in stub_api.ingested_events] == [
        f"ticket_{index}" for index in range(len(actions))
    ]


def test_unknown_actions_fail_the_sync(stub_api: StubSolvimonAPI) -> None:
    """An action other than `create` or `replace` is refused rather than guessed."""
    sink = build_sink(stub_api, action_field="action")

    with pytest.raises(ValueError, match="'update'"):
        send(sink, [{"id": "evt-1", "action": "update"}])


def test_delete_failures_are_retried(stub_api: StubSolvimonAPI) -> None:
    """A delete that hits a rate limit or a 5xx is retried before the ingest."""
    sink = build_sink(stub_api, action_field="action")
    stub_api.queue_statuses(503)

    send(sink, [{"id": "evt-1", "reference": "ticket_1", "action": "replace"}])

    assert [(request.path, request.status) for request in stub_api.requests] == [
        (DELETE_PATH, 503),
        (DELETE_PATH, 200),
        (INGEST_PATH, 200),
    ]


def test_refused_deletes_fail_the_sync(stub_api: StubSolvimonAPI) -> None:
    """A delete refused for another reason than a missing event fails before the ingest."""
    sink = build_sink(stub_api, action_field="action")
    stub_api.queue_statuses(401)

    with pytest.raises(FatalAPIError, match="refused to delete event 'ticket_1'"):
        send(sink, [{"id": "evt-1", "reference": "ticket_1", "action": "replace"}])

    assert [request.path for request in stub_api.requests] == [DELETE_PATH]


def test_rejected_replacements_fail_the_sync(stub_api: StubSolvimonAPI) -> None:
    """A replacement refused after its live event was deleted fails instead of being skipped."""
    sink = build_sink(stub_api, action_field="action")
    stub_api.reject_events("evt-2")

    with pytest.raises(FatalAPIError, match="replacement of event 'ticket_2'"):
        send(
            sink,
            [
                {"id": "evt-1", "reference": "ticket_1", "action": "create"},
                {"id": "evt-2", "reference": "ticket_2", "action": "replace"},
            ],
        )


def test_rejected_replacements_without_a_live_event_are_skipped(
    stub_api: StubSolvimonAPI,
) -> None:
    """A refused replacement whose delete found nothing lost nothing, so it is skipped."""
    sink = build_sink(stub_api, action_field="action")
    stub_api.miss_references("ticket_2")
    stub_api.reject_events("evt-2")

    send(
        sink,
        [
            {"id": "evt-1", "reference": "ticket_1", "action": "create"},
            {"id": "evt-2", "reference": "ticket_2", "action": "replace"},
        ],
    )

    assert [event["reference"] for event in stub_api.ingested_events] == ["ticket_1"]


@pytest.mark.parametrize(
    "actions",
    [("create", "replace"), ("replace", "create"), ("replace", "replace")],
)
def test_repeated_references_with_a_replace_fail(
    stub_api: StubSolvimonAPI,
    actions: tuple[str, str],
) -> None:
    """A batch can't order a replace with another event of its reference, so it refuses them."""
    sink = build_sink(stub_api, action_field="action")

    with pytest.raises(ValueError, match="repeats reference 'ticket_1'"):
        send(
            sink,
            [
                {"id": f"evt-{index}", "reference": "ticket_1", "action": action}
                for index, action in enumerate(actions)
            ],
        )

    assert stub_api.requests == []


def test_replaces_across_batches_apply_in_order(stub_api: StubSolvimonAPI) -> None:
    """A reference created in one batch and replaced in the next is deleted, then re-ingested."""
    sink = build_sink(stub_api, action_field="action")

    send(sink, [{"id": "evt-1", "reference": "ticket_1", "action": "create"}])
    send(sink, [{"id": "evt-2", "reference": "ticket_1", "action": "replace"}])

    assert [request.path for request in stub_api.requests] == [
        INGEST_PATH,
        DELETE_PATH,
        INGEST_PATH,
    ]


def read_predictions(folder: Path) -> list[list[dict]]:
    """Read the cost prediction files under a `cost_prediction_path`, oldest first.

    Args:
        folder: The configured `cost_prediction_path`.

    Returns:
        The events of each file.
    """
    return [
        [json.loads(line) for line in gzip.decompress(path.read_bytes()).decode().splitlines()]
        for path in sorted((folder / "predictions").glob("*.jsonl.gz"))
    ]


def pending_ids(folder: Path) -> list[str]:
    """Read the event ids listed as pending for the test stream.

    Args:
        folder: The configured `cost_prediction_path`.

    Returns:
        Every listed id.
    """
    return [
        event_id
        for path in sorted((folder / "pending" / "tickets_sold").glob("*"))
        for event_id in path.read_text().split()
    ]


def test_cost_predictions_are_written_per_request(
    stub_api: StubSolvimonAPI,
    tmp_path: Path,
) -> None:
    """Events are ingested with predictions on, and each request is written as one file."""
    sink = build_sink(stub_api, cost_prediction_path=str(tmp_path), derive_event_id=False)
    stub_api.pend_predictions("mdat-ticket-0002")

    send(sink, [{"reference": "ticket-0001"}, {"reference": "ticket-0002"}])

    assert [request.path for request in stub_api.requests] == [PREDICTION_PATH]
    assert all("id" not in event for event in stub_api.events)
    [written] = read_predictions(tmp_path)
    assert [(event["id"], event["cost_prediction"]["status"]) for event in written] == [
        ("mdat-ticket-0001", "READY"),
        ("mdat-ticket-0002", "PENDING"),
    ]
    assert all("_sdc_batched_at" in event for event in written)
    assert pending_ids(tmp_path) == ["mdat-ticket-0002"]


def test_predictions_are_written_before_a_later_request_fails(
    stub_api: StubSolvimonAPI,
    tmp_path: Path,
) -> None:
    """A refused replacement can't lose the predictions of a request accepted before it."""
    sink = build_sink(stub_api, cost_prediction_path=str(tmp_path), action_field="action")
    stub_api.reject_events("evt-2")

    with pytest.raises(FatalAPIError, match="replacement of event 'ticket_2'"):
        send(
            sink,
            [
                {"id": "evt-1", "reference": "ticket_1", "action": "create"},
                {"id": "evt-2", "reference": "ticket_2", "action": "replace"},
            ],
        )

    assert [[event["id"] for event in written] for written in read_predictions(tmp_path)] == [
        ["evt-1"]
    ]


def test_s3_paths_get_a_working_s3_session(stub_api: StubSolvimonAPI) -> None:
    """Options meant for local folders never reach the AWS session of an S3 path."""
    sink = build_sink(stub_api, cost_prediction_path="s3://bucket/solvimon/cost_prediction")

    assert isinstance(sink.prediction_fs, S3FileSystem)
    sink.prediction_fs.connect()


def test_pending_predictions_are_looked_up_at_the_end_of_the_run(
    stub_api: StubSolvimonAPI,
    tmp_path: Path,
) -> None:
    """Pending events are looked up by Solvimon's id; only final predictions are stored."""
    sink = build_sink(stub_api, cost_prediction_path=str(tmp_path), derive_event_id=False)
    stub_api.pend_predictions("mdat-ticket-0001", "mdat-ticket-0002")
    send(sink, [{"reference": "ticket-0001"}, {"reference": "ticket-0002"}])
    (tmp_path / "pending" / "tickets_sold" / "0-earlier-run.txt").write_text("mdat-gone")
    stub_api.settle_predictions("mdat-ticket-0001")

    sink.clean_up()

    assert sorted(stub_api.looked_up_ids) == ["mdat-gone", "mdat-ticket-0001", "mdat-ticket-0002"]
    _, looked_up = read_predictions(tmp_path)
    assert [(event["id"], event["cost_prediction"]["status"]) for event in looked_up] == [
        ("mdat-ticket-0001", "READY")
    ]
    assert pending_ids(tmp_path) == ["mdat-ticket-0002"]


def test_stream_names_stay_inside_the_pending_folder(
    stub_api: StubSolvimonAPI,
    tmp_path: Path,
) -> None:
    """A stream name can't point the pending lists, which get deleted, at another folder."""
    sink = build_sink(stub_api, cost_prediction_path=str(tmp_path), stream_name="../../etc")

    assert sink.pending_dir == f"{tmp_path}/pending/______etc"


def test_without_a_cost_prediction_path_nothing_is_looked_up(
    stub_api: StubSolvimonAPI,
) -> None:
    """Requests ask for no predictions and the run ends without lookups."""
    sink = build_sink(stub_api)

    send(sink, [{"id": "evt-1"}])
    sink.clean_up()

    assert [request.path for request in stub_api.requests] == [INGEST_PATH]
    assert stub_api.looked_up_ids == []
