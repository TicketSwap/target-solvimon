"""Solvimon target sink class, which handles writing streams."""

from __future__ import annotations

import datetime
import decimal
import sys
import typing as t
import uuid

from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.singerlib.json import serialize_json
from singer_sdk.sinks import BatchSink

from target_solvimon.client import (
    DEFAULT_API_URL,
    DEFAULT_API_VERSION,
    DEFAULT_BACKOFF_FACTOR,
    DEFAULT_MAX_RETRIES,
    DEFAULT_TIMEOUT,
    SolvimonClient,
)

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.target_base import Target

SDC_PREFIX = "_sdc_"
EXTRACTED_AT_FIELD = "_sdc_extracted_at"

# Fixed namespace for derived event ids: replaying a sync then produces the same ids,
# so Solvimon deduplicates instead of double-billing.
EVENT_ID_NAMESPACE = uuid.UUID("12e457de-ce18-475c-bae7-a7c692c52315")

DEFAULT_ID_FIELD = "id"
DEFAULT_REFERENCE_FIELD = "reference"
DEFAULT_TIMESTAMP_FIELD = "timestamp"
DEFAULT_CUSTOMER_REFERENCE_FIELD = "customer_reference"

# Keys of a `meter_values` entry: each entry carries exactly one of the value kinds,
# either read from a record field (`*_field`) or given as a fixed value.
AMOUNT_FIELD = "amount_field"
CURRENCY_FIELD = "currency_field"
CURRENCY = "currency"
VALUE_KINDS = ("number", "count")
METER_VALUE_KEYS = frozenset({
    AMOUNT_FIELD,
    CURRENCY_FIELD,
    CURRENCY,
    *VALUE_KINDS,
    *(f"{kind}_field" for kind in VALUE_KINDS),
})

EXTRA_FIELDS_PASSTHROUGH = "passthrough"
EXTRA_FIELDS_METER_PROPERTIES = "meter_properties"
EXTRA_FIELDS_IGNORE = "ignore"
DEFAULT_EXTRA_FIELDS = EXTRA_FIELDS_PASSTHROUGH


def to_iso8601(value: t.Any) -> str:  # ruff: ignore[any-type]
    """Render a record value as an ISO 8601 timestamp string.

    Args:
        value: A datetime (the SDK parses ``date-time`` fields for us), a date, an
            epoch-seconds number, or an already formatted string.

    Returns:
        The value as an ISO 8601 string. Naive datetimes are assumed to be UTC.
    """
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        return value.isoformat()
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, (int, float, decimal.Decimal)) and not isinstance(value, bool):
        epoch = datetime.datetime.fromtimestamp(float(value), tz=datetime.timezone.utc)
        return epoch.isoformat()
    return str(value)


def to_property_value(value: t.Any) -> str:  # ruff: ignore[any-type]
    """Render a record value as a Solvimon meter property value.

    Args:
        value: Any JSON-compatible record value.

    Returns:
        The value as a string, since the API only accepts string property values.
    """
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return serialize_json(value)
    return str(value)


def build_meter_value(reference: str, spec: dict, record: dict) -> dict | None:
    """Build one meter value from its configured spec.

    Args:
        reference: Reference of the meter value in Solvimon.
        spec: The `meter_values` entry for it.
        record: Individual record in the stream.

    Returns:
        The meter value, or None when the record has no data for it — a missing
        amount is left out rather than sent as an empty one.
    """
    if AMOUNT_FIELD in spec:
        quantity = record.get(spec[AMOUNT_FIELD])
        currency = spec[CURRENCY] if CURRENCY in spec else record.get(spec[CURRENCY_FIELD])
        if quantity is None or currency is None:
            return None
        return {
            "reference": reference,
            "amount": {
                "currency": to_property_value(currency),
                "quantity": to_property_value(quantity),
            },
        }

    for kind in VALUE_KINDS:
        if kind in spec:
            return {"reference": reference, kind: to_property_value(spec[kind])}
        if f"{kind}_field" in spec:
            value = record.get(spec[f"{kind}_field"])
            if value is None:
                return None
            return {"reference": reference, kind: to_property_value(value)}

    return None  # pragma: no cover - the spec is validated on startup


def meter_value_spec_problems(spec: t.Any) -> list[str]:  # ruff: ignore[any-type]
    """Describe what is wrong with one `meter_values` entry.

    Args:
        spec: The configured entry.

    Returns:
        Human-readable problems, empty when the entry is usable.
    """
    if not isinstance(spec, dict):
        return [f"expected a mapping, got {type(spec).__name__}"]

    problems = []
    if unknown := sorted(set(spec) - METER_VALUE_KEYS):
        problems.append(f"unknown keys {unknown}; expected any of {sorted(METER_VALUE_KEYS)}")

    kinds = [
        key
        for key in (AMOUNT_FIELD, *VALUE_KINDS, *(f"{k}_field" for k in VALUE_KINDS))
        if key in spec
    ]
    if len(kinds) != 1:
        problems.append(
            f"expected exactly one of `{AMOUNT_FIELD}`, `number`, `number_field`, "
            f"`count` or `count_field`, got {kinds or 'none'}"
        )
    elif kinds == [AMOUNT_FIELD] and not spec.keys() & {CURRENCY, CURRENCY_FIELD}:
        problems.append(f"`{AMOUNT_FIELD}` also needs `{CURRENCY}` or `{CURRENCY_FIELD}`")

    return problems


class SolvimonSink(BatchSink):
    """Solvimon target sink class."""

    def __init__(
        self,
        target: Target,
        stream_name: str,
        schema: dict,
        key_properties: t.Sequence[str] | None,
    ) -> None:
        """Initialize the sink and its API client.

        Args:
            target: Target instance.
            stream_name: Name of the stream to sink.
            schema: Schema of the stream to sink.
            key_properties: Primary key of the stream to sink.
        """
        super().__init__(target, stream_name, schema, key_properties)

        self.client = SolvimonClient(
            logger=self.logger,
            api_key=self.config["api_key"],
            api_url=self.config.get("api_url", DEFAULT_API_URL),
            api_version=self.config.get("api_version", DEFAULT_API_VERSION),
            max_events_per_request=self.config.get("max_events_per_request"),
            auth_token=self.config.get("auth_token"),
            platform_id=self.config.get("platform_id"),
            timeout=self.config.get("timeout", DEFAULT_TIMEOUT),
            max_retries=self.config.get("max_retries", DEFAULT_MAX_RETRIES),
            backoff_factor=self.config.get("backoff_factor", DEFAULT_BACKOFF_FACTOR),
        )

        self.id_field: str = self.config.get("event_id_field", DEFAULT_ID_FIELD)
        self.reference_field: str = self.config.get(
            "event_reference_field",
            DEFAULT_REFERENCE_FIELD,
        )
        self.timestamp_field: str = self.config.get(
            "event_timestamp_field",
            DEFAULT_TIMESTAMP_FIELD,
        )
        self.derive_id: bool = self.config.get("derive_event_id", True)
        self.customer_reference_field: str = self.config.get(
            "customer_reference_field",
            DEFAULT_CUSTOMER_REFERENCE_FIELD,
        )
        self.static_customer_reference: str | None = self.config.get("customer_reference")
        self.static_reference: str | None = self.config.get("event_reference")
        self.extra_fields: str = self.config.get("extra_fields", DEFAULT_EXTRA_FIELDS)

        self.meter_reference: str | None = self.config.get("meter_reference")
        self.property_fields: dict[str, str] = self.parse_meter_properties()
        self.value_specs: dict[str, dict] = self.parse_meter_values()
        self._consumed_fields = self.collect_consumed_fields()

        self._clobbered_fields: set[str] = set()
        self._warned_timestamp_fallback = False

    @property
    @override
    def max_size(self) -> int:
        """Records to buffer before draining.

        Returns:
            `batch_size_rows` if set, else what the endpoint takes in one call, so a
            full batch is one request. A larger value is still split on send.
        """
        return self.batch_size_rows or self.client.max_events_per_request

    @override
    def process_record(self, record: dict, context: dict) -> None:
        """Convert a record into a Solvimon event and stage it for the batch.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
        context.setdefault("records", []).append(self.build_event(record))

    @override
    def process_batch(self, context: dict) -> None:
        """Send the staged events to the Solvimon ingest endpoint.

        Args:
            context: Stream partition or context dictionary.
        """
        events: list[dict] = context.get("records", [])
        per_request = self.client.max_events_per_request

        for start in range(0, len(events), per_request):
            chunk = events[start : start + per_request]
            response = self.client.ingest_events(chunk)
            acknowledged = response.get(self.client.events_key)
            if acknowledged is not None and len(acknowledged) != len(chunk):
                self.logger.warning(
                    "Solvimon acknowledged %d of the %d events sent for stream '%s'",
                    len(acknowledged),
                    len(chunk),
                    self.stream_name,
                )
            self.logger.debug(
                "Ingested %d events for stream '%s'",
                len(chunk),
                self.stream_name,
            )

    @override
    def clean_up(self) -> None:
        """Close the HTTP session once the stream is fully drained."""
        super().clean_up()
        self.client.close()

    def build_event(self, record: dict) -> dict:
        """Map a record onto the Solvimon event shape.

        Args:
            record: Individual record in the stream.

        Returns:
            An event object accepted by ``POST /v2/events/ingest-batch``.
        """
        event: dict[str, t.Any] = {}
        event_id = self.event_id(record)
        if event_id is not None:
            event["id"] = event_id
        event["reference"] = self.event_reference(record)
        event["timestamp"] = self.event_timestamp(record)
        if self.meter_reference:
            event["meter_reference"] = self.meter_reference
        customer_reference = self.customer_reference(record)
        if customer_reference is not None:
            event["customer_reference"] = customer_reference

        properties = self.build_meter_properties(record)
        values = self.build_meter_values(record)
        extra = self.extra_record_fields(record)

        if self.extra_fields == EXTRA_FIELDS_METER_PROPERTIES:
            properties.extend(
                {"reference": key, "value": to_property_value(value)}
                for key, value in extra.items()
                if value is not None
            )
            extra = {}
        elif self.extra_fields == EXTRA_FIELDS_IGNORE:
            extra = {}

        if properties:
            event["meter_properties"] = properties
        if values:
            event["meter_values"] = values

        for key, value in extra.items():
            if key in event:
                # A field built from the config owns the event key, so the like-named
                # record field cannot also have it.
                self.warn_clobbered_field(key)
                continue
            event[key] = value

        return event

    def extra_record_fields(self, record: dict) -> dict:
        """Return the record fields that no setting has claimed.

        Args:
            record: Individual record in the stream.

        Returns:
            The record without its `_sdc_*` metadata and without the fields mapped
            onto the event by configuration.
        """
        return {
            key: value
            for key, value in record.items()
            if key not in self._consumed_fields and not key.startswith(SDC_PREFIX)
        }

    def build_meter_properties(self, record: dict) -> list[dict]:
        """Build the meter properties configured by `meter_properties`.

        Args:
            record: Individual record in the stream.

        Returns:
            One entry per configured property whose record field holds a value.
        """
        return [
            {"reference": reference, "value": to_property_value(record[field])}
            for reference, field in self.property_fields.items()
            if record.get(field) is not None
        ]

    def build_meter_values(self, record: dict) -> list[dict]:
        """Build the meter values configured by `meter_values`.

        Args:
            record: Individual record in the stream.

        Returns:
            One entry per configured value that the record has data for.
        """
        values = []
        for reference, spec in self.value_specs.items():
            value = build_meter_value(reference, spec, record)
            if value is not None:
                values.append(value)
        return values

    def event_id(self, record: dict) -> str | None:
        """Return the event id for a record.

        Args:
            record: Individual record in the stream.

        Returns:
            The configured id field, a derived id when the record has none, or None
            when `derive_event_id` is off and the record has none — Solvimon then
            identifies the event by its reference alone.
        """
        value = record.get(self.id_field)
        if value is not None:
            return str(value)
        return self.derived_event_id(record) if self.derive_id else None

    def event_reference(self, record: dict) -> str:
        """Return the event reference for a record.

        Args:
            record: Individual record in the stream.

        Returns:
            The static `event_reference` setting if configured, else the configured
            reference field, else the stream name.
        """
        if self.static_reference:
            return self.static_reference
        value = record.get(self.reference_field)
        return str(value) if value is not None else self.stream_name

    def customer_reference(self, record: dict) -> str | None:
        """Return the reference of the customer an event is metered against.

        Args:
            record: Individual record in the stream.

        Returns:
            The static `customer_reference` setting if configured, else the configured
            customer field, else None — Solvimon rejects events it cannot attribute to
            a customer, so this is normally set one way or the other.
        """
        if self.static_customer_reference:
            return self.static_customer_reference
        value = record.get(self.customer_reference_field)
        return str(value) if value is not None else None

    def event_timestamp(self, record: dict) -> str:
        """Return the event timestamp for a record.

        Args:
            record: Individual record in the stream.

        Returns:
            The configured timestamp field, falling back to the extraction time and
            finally to the current time.
        """
        value = record.get(self.timestamp_field)
        if value is None:
            value = record.get(EXTRACTED_AT_FIELD)
        if value is None:
            self.warn_timestamp_fallback()
            return datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        return to_iso8601(value)

    def derived_event_id(self, record: dict) -> str:
        """Derive a stable event id for a record without one.

        Args:
            record: Individual record in the stream.

        Returns:
            A UUIDv5 of the stream name plus the record's key properties, or the whole
            record when the stream has no key properties.
        """
        seed = (
            {key: record.get(key) for key in self.key_properties}
            if self.key_properties
            else {key: value for key, value in record.items() if not key.startswith(SDC_PREFIX)}
        )
        name = f"{self.stream_name}:{serialize_json(seed, sort_keys=True)}"
        return str(uuid.uuid5(EVENT_ID_NAMESPACE, name))

    def parse_meter_properties(self) -> dict[str, str]:
        """Read the `meter_properties` setting.

        Returns:
            A mapping of property reference to the record field holding its value. A
            list of field names is read as properties named after those fields.

        Raises:
            ConfigValidationError: If the setting is neither a list nor a mapping.
        """
        configured = self.config.get("meter_properties") or {}
        if isinstance(configured, dict):
            return {str(key): str(value) for key, value in configured.items()}
        if isinstance(configured, list):
            return {str(field): str(field) for field in configured}

        msg = (
            "`meter_properties` must be a list of record fields or a mapping of "
            f"property reference to record field, not {type(configured).__name__}"
        )
        raise ConfigValidationError(msg)

    def parse_meter_values(self) -> dict[str, dict]:
        """Read and check the `meter_values` setting.

        Returns:
            A mapping of meter value reference to its spec.

        Raises:
            ConfigValidationError: If a spec is malformed. Checking on startup keeps a
                typo from silently dropping a billable value from every event.
        """
        configured: dict[str, dict] = self.config.get("meter_values") or {}
        errors = [
            f"`meter_values.{reference}`: {problem}"
            for reference, spec in configured.items()
            for problem in meter_value_spec_problems(spec)
        ]
        if errors:
            msg = "Invalid `meter_values` configuration"
            raise ConfigValidationError(msg, errors=errors)
        return configured

    def collect_consumed_fields(self) -> set[str]:
        """Return every record field that a setting maps onto the event.

        Returns:
            The field names that `extra_fields` should no longer see.
        """
        consumed = {
            self.id_field,
            self.reference_field,
            self.timestamp_field,
            self.customer_reference_field,
        }
        consumed.update(self.property_fields.values())
        for spec in self.value_specs.values():
            consumed.update(value for key, value in spec.items() if key.endswith("_field"))
        return consumed

    def warn_timestamp_fallback(self) -> None:
        """Warn once per stream that events are stamped with the ingestion time.

        Billing periods depend on the event timestamp, so a stream falling back to
        "now" is almost always a mapping mistake rather than an intent.
        """
        if not self._warned_timestamp_fallback:
            self._warned_timestamp_fallback = True
            self.logger.warning(
                "Records of stream '%s' have no '%s' field, so their events are "
                "stamped with the time of ingestion. Set `event_timestamp_field` to "
                "the record field holding the event time.",
                self.stream_name,
                self.timestamp_field,
            )

    def warn_clobbered_field(self, key: str) -> None:
        """Warn once per stream about a record field dropped by a field remapping.

        Args:
            key: Name of the dropped record field.
        """
        if key not in self._clobbered_fields:
            self._clobbered_fields.add(key)
            self.logger.warning(
                "Dropping record field '%s' of stream '%s': it collides with the "
                "event field built from another record field",
                key,
                self.stream_name,
            )
