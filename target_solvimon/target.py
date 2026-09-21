"""Solvimon target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_solvimon.client import (
    API_VERSIONS,
    DEFAULT_API_URL,
    DEFAULT_API_VERSION,
    DEFAULT_BACKOFF_FACTOR,
    DEFAULT_MAX_RETRIES,
    DEFAULT_TIMEOUT,
    MAX_EVENTS_PER_REQUEST,
)
from target_solvimon.sinks import (
    AMOUNT_FIELD,
    CURRENCY,
    CURRENCY_FIELD,
    DEFAULT_CUSTOMER_REFERENCE_FIELD,
    DEFAULT_EXTRA_FIELDS,
    DEFAULT_ID_FIELD,
    DEFAULT_REFERENCE_FIELD,
    DEFAULT_TIMESTAMP_FIELD,
    EXTRA_FIELDS_IGNORE,
    EXTRA_FIELDS_METER_PROPERTIES,
    EXTRA_FIELDS_PASSTHROUGH,
    SolvimonSink,
)


class TargetSolvimon(Target):
    """Target for Solvimon."""

    name = "target-solvimon"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType(nullable=False),
            secret=True,  # Flag config as protected.
            required=True,
            title="API Key",
            description="Solvimon API key, sent as the `X-API-KEY` header. Needs the "
            "`METER_DATA.INGEST` permission.",
        ),
        th.Property(
            "api_url",
            th.StringType(nullable=False),
            default=DEFAULT_API_URL,
            title="API URL",
            description="Base URL of the Solvimon API. Defaults to the test "
            "environment; use `https://api.solvimon.com` to write billable events.",
        ),
        th.Property(
            "api_version",
            th.StringType(nullable=False),
            default=DEFAULT_API_VERSION,
            allowed_values=list(API_VERSIONS),
            title="Ingest API Version",
            description="Version of the batch ingest endpoint to call: `v1` posts "
            "`meter_datas` to `/v1/events/ingest-batch`, `v2` posts `events` to "
            "`/v2/events/ingest-batch`. The v2 endpoint is documented but currently "
            "answers 404 on both the test and the live host.",
        ),
        th.Property(
            "auth_token",
            th.StringType(nullable=True),
            secret=True,  # Flag config as protected.
            title="Auth Token",
            description="Bearer token sent as the `Authorization` header, for "
            "deployments that require it on top of the API key.",
        ),
        th.Property(
            "platform_id",
            th.StringType(nullable=True),
            title="Platform ID",
            description="Solvimon platform ID, sent as the `x-platform-id` header.",
        ),
        th.Property(
            "max_events_per_request",
            th.IntegerType(nullable=True),
            title="Max Events Per Request",
            description="Events to send in one ingest call. Defaults to the limit of "
            f"the endpoint in use: {MAX_EVENTS_PER_REQUEST['v1']} for `v1`, "
            f"{MAX_EVENTS_PER_REQUEST['v2']} for `v2`. Larger batches are split over "
            "several calls.",
        ),
        th.Property(
            "timeout",
            th.IntegerType(nullable=False),
            default=DEFAULT_TIMEOUT,
            title="Request Timeout",
            description="Timeout of a single ingest request, in seconds.",
        ),
        th.Property(
            "max_retries",
            th.IntegerType(nullable=False),
            default=DEFAULT_MAX_RETRIES,
            title="Max Retries",
            description="How often to retry an ingest request that failed with a "
            "rate limit, a timeout or a 5xx response.",
        ),
        th.Property(
            "backoff_factor",
            th.IntegerType(nullable=False),
            default=DEFAULT_BACKOFF_FACTOR,
            title="Backoff Factor",
            description="Exponential backoff factor between retries, in seconds.",
        ),
        th.Property(
            "event_id_field",
            th.StringType(nullable=False),
            default=DEFAULT_ID_FIELD,
            title="Event ID Field",
            description="Record field holding the event `id`. Records without it get "
            "an id derived from their key properties, so replays stay idempotent.",
        ),
        th.Property(
            "derive_event_id",
            th.BooleanType(nullable=False),
            default=True,
            title="Derive Event ID",
            description="Whether to derive an event `id` for records that have no "
            "`event_id_field`. Turn this off to leave `id` out entirely and let "
            "Solvimon identify events by their `reference`.",
        ),
        th.Property(
            "event_reference_field",
            th.StringType(nullable=False),
            default=DEFAULT_REFERENCE_FIELD,
            title="Event Reference Field",
            description="Record field holding the event `reference`. Records without "
            "it fall back to the stream name.",
        ),
        th.Property(
            "event_reference",
            th.StringType(nullable=True),
            title="Event Reference",
            description="Static event `reference` applied to every record, overriding "
            "`event_reference_field`.",
        ),
        th.Property(
            "event_timestamp_field",
            th.StringType(nullable=False),
            default=DEFAULT_TIMESTAMP_FIELD,
            title="Event Timestamp Field",
            description="Record field holding the event `timestamp`. Records without "
            "it fall back to `_sdc_extracted_at`, then to the time of ingestion.",
        ),
        th.Property(
            "customer_reference_field",
            th.StringType(nullable=False),
            default=DEFAULT_CUSTOMER_REFERENCE_FIELD,
            title="Customer Reference Field",
            description="Record field holding the reference of the customer the "
            "event is metered against. Solvimon rejects events it cannot attribute "
            "to a customer.",
        ),
        th.Property(
            "customer_reference",
            th.StringType(nullable=True),
            title="Customer Reference",
            description="Static customer reference applied to every record, "
            "overriding `customer_reference_field`.",
        ),
        th.Property(
            "meter_reference",
            th.StringType(nullable=True),
            title="Meter Reference",
            description="Reference of the Solvimon meter every event belongs to, "
            "added to each event. Leave unset for records that carry their own "
            "`meter_reference`.",
        ),
        th.Property(
            "meter_properties",
            th.CustomType({
                "oneOf": [
                    {"type": "array", "items": {"type": "string"}},
                    {"type": "object", "additionalProperties": {"type": "string"}},
                ],
            }),
            title="Meter Properties",
            description="Record fields to send as meter properties, either as a list "
            "of field names (`[country, event_name]`) or as a mapping of property "
            "reference to record field (`{country: country_code}`).",
        ),
        th.Property(
            "meter_values",
            th.ObjectType(
                additional_properties=th.ObjectType(
                    th.Property(
                        AMOUNT_FIELD,
                        th.StringType,
                        description="Record field holding the amount quantity.",
                    ),
                    th.Property(
                        CURRENCY_FIELD,
                        th.StringType,
                        description="Record field holding the amount currency.",
                    ),
                    th.Property(
                        CURRENCY,
                        th.StringType,
                        description="Fixed currency, instead of `currency_field`.",
                    ),
                    th.Property(
                        "number_field",
                        th.StringType,
                        description="Record field holding a number value.",
                    ),
                    th.Property(
                        "number",
                        th.StringType,
                        description='Fixed number value, e.g. `"1"` to count events.',
                    ),
                    th.Property(
                        "count_field",
                        th.StringType,
                        description="Record field holding a count value.",
                    ),
                    th.Property(
                        "count",
                        th.StringType,
                        description="Fixed count value.",
                    ),
                    additional_properties=False,
                ),
                nullable=True,
            ),
            title="Meter Values",
            description="Meter values to build per record, keyed by the value "
            "reference in Solvimon. Each entry takes exactly one of `amount_field` "
            "(with `currency_field` or `currency`), `number`, `number_field`, `count` "
            "or `count_field`. Values whose record field is empty are left out.",
        ),
        th.Property(
            "extra_fields",
            th.StringType(nullable=False),
            default=DEFAULT_EXTRA_FIELDS,
            allowed_values=[
                EXTRA_FIELDS_PASSTHROUGH,
                EXTRA_FIELDS_METER_PROPERTIES,
                EXTRA_FIELDS_IGNORE,
            ],
            title="Extra Fields Handling",
            description="What to do with record fields that are not mapped to `id`, "
            "`reference` or `timestamp`: send them as-is (`passthrough`, for records "
            "already shaped like Solvimon events), turn them into `meter_properties` "
            "entries, or drop them.",
        ),
    ).to_dict()

    default_sink_class = SolvimonSink


if __name__ == "__main__":
    TargetSolvimon.cli()
