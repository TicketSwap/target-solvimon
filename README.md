# target-solvimon

`target-solvimon` is a Singer target for [Solvimon](https://solvimon.com).

It loads records into Solvimon as metering events through the batch ingest endpoint,
in batches of up to 50 events per call.

The target posts to `POST /v1/events/ingest-batch` with the events under a
`meter_datas` key, and Solvimon answers `201` echoing the created meter data. The
[`POST /v2/events/ingest-batch`](https://docs.solvimon.com/api-docs/event-api/post-v-2-events-ingest-batch)
endpoint the docs describe — same events, under an `events` key — currently answers
`404 Page does not exist` on both `test.api.solvimon.com` and `api.solvimon.com`; set
`api_version: v2` to use it once it is served.

What the API enforces, beyond the event shape:

- **A customer per event.** Without a resolvable `customer_reference` the batch fails
  with `RESOURCES_NOT_FOUND: The requested CUSTOMER could not be found`.
- **At most 50 events per call** on `v1`: a bigger batch is rejected whole with
  `value holds too many items (51, maximum allowed: 50)`.
- **A reference of at least 10 characters.**
- **Timestamps at most 30 minutes in the future.**
- **Meter properties and values must match the meter**, including the allowed values
  of an `ENUM` property. Everything the meter marks required has to be present.

Build with the [Meltano Target SDK](https://sdk.meltano.com).

## Installation

Install from GitHub:

```bash
uv tool install git+https://github.com/ticketswap/target-solvimon.git@main
```

## Configuration

### Accepted Config Options

| Setting | Required | Default | Description |
|:--------|:--------:|:-------:|:------------|
| api_key | True | None | Solvimon API key, sent as the `X-API-KEY` header. Needs the `METER_DATA.INGEST` permission. |
| api_version | False | `v1` | Batch ingest endpoint to call, `v1` or `v2`. |
| max_events_per_request | False | endpoint limit | Events per ingest call: 50 on `v1`, 1000 on `v2`. |
| api_url | False | `https://test.api.solvimon.com` | Base URL of the Solvimon API. Defaults to the test environment; use `https://api.solvimon.com` to write billable events. |
| auth_token | False | None | Bearer token sent as the `Authorization` header, for deployments that require it on top of the API key. |
| platform_id | False | None | Solvimon platform ID, sent as the `x-platform-id` header. |
| timeout | False | 60 | Timeout of a single ingest request, in seconds. |
| max_retries | False | 5 | How often to retry an ingest request that failed with a rate limit, a timeout or a 5xx response. |
| backoff_factor | False | 2 | Exponential backoff factor between retries, in seconds. |
| event_id_field | False | `id` | Record field holding the event `id`. |
| derive_event_id | False | true | Whether to derive an event `id` for records that have none. Turn off to leave `id` out and let Solvimon identify events by `reference`. |
| event_reference_field | False | `reference` | Record field holding the event `reference`. |
| event_reference | False | None | Static event `reference` applied to every record, overriding `event_reference_field`. |
| event_timestamp_field | False | `timestamp` | Record field holding the event `timestamp`. |
| customer_reference_field | False | `customer_reference` | Record field holding the customer the event is metered against. |
| customer_reference | False | None | Static customer reference for every record, overriding the field. |
| meter_reference | False | None | Reference of the meter every event belongs to, added to each event. |
| meter_properties | False | None | Record fields to send as meter properties: a list of field names, or a mapping of property reference to record field. |
| meter_values | False | None | Meter values to build per record, keyed by value reference. See below. |
| extra_fields | False | `passthrough` | What to do with record fields that are not mapped to `id`, `reference` or `timestamp`: `passthrough`, `meter_properties` or `ignore`. |

The SDK also provides `batch_size_rows`, `add_record_metadata`, `validate_records`,
`stream_maps` and schema flattening. A full list of supported settings and
capabilities is available by running:

```bash
target-solvimon --about
```

### How records become events

Every record is mapped onto one Solvimon event:

| Event field | Comes from |
|:------------|:-----------|
| `id` | `event_id_field`. When the record has no such field, a UUIDv5 is derived from the stream name and the record's key properties (or the whole record, if the stream has none). Derived ids are stable, so replaying a sync sends the same ids instead of double-billing. With `derive_event_id: false`, no `id` is sent at all. |
| `reference` | `event_reference` if set, else `event_reference_field`, else the stream name. |
| `timestamp` | `event_timestamp_field`, else `_sdc_extracted_at`, else the time of ingestion (logged as a warning, since billing periods depend on it). Datetimes, dates and epoch seconds are normalized to ISO 8601; naive datetimes are assumed to be UTC. |
| `customer_reference` | `customer_reference` if set, else `customer_reference_field`. Required by the API. |

`meter_reference`, `meter_properties` and `meter_values` build the rest of the event
from flat record fields:

```yaml
meter_reference: tickets
meter_properties: [country, event_name]      # or {country: country_code} to rename
meter_values:
  collected_payment: {amount_field: gross_amount, currency_field: currency}
  collected_payment_counter: {number: "1"}
  service_fee: {amount_field: fee_amount, currency: EUR}
```

Each `meter_values` entry is keyed by its reference in Solvimon and takes exactly one
of `amount_field` (paired with `currency_field` or a fixed `currency`), `number`,
`number_field`, `count` or `count_field`. A `*_field` reads the record; the bare form
sends a fixed value. Entries whose record field is empty are left out of the event
rather than sent empty, and a half-specified entry fails on startup instead of quietly
dropping a billable value.

Fields consumed this way no longer count as extras. The remaining record fields are
handled according to `extra_fields`:

- `passthrough` (default): sent as-is alongside the three fields above. Use this when
  the records are already shaped like Solvimon events, so that `customer_reference`,
  `meter_reference`, `meter_values`, `meter_properties` and friends reach the API
  unchanged. Use [stream maps](https://sdk.meltano.com/en/latest/stream_maps.html) to
  reshape records that are not.
- `meter_properties`: each remaining field becomes a
  `{"reference": <field>, "value": <value>}` meter property. Null fields are skipped
  and values are stringified, since the API only accepts string property values.
- `ignore`: dropped, sending only `id`, `reference` and `timestamp`.

`_sdc_*` metadata fields are never sent to the API.

### Batching, retries and failures

Records are buffered per stream and sent in batches of at most `max_events_per_request`
events, which defaults to what the endpoint in use accepts: 50 on `v1`, 1000 on `v2`.
Raise it if Solvimon raises the limit. A larger `batch_size_rows` buffers more records
but still splits them across requests.

Requests that fail with `408`, `425`, `429` or a `5xx` are retried `max_retries` times
with exponential backoff, honouring `Retry-After`. Anything else — and anything still
failing once the retries are used up — fails the sync with the status code and the API
error body, rather than silently dropping events.

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file. See
[`.env.example`](.env.example).

### Authentication and Authorization

The API key is sent as the `X-API-KEY` header and needs the `METER_DATA.INGEST`
permission. Deployments that additionally require a bearer token can set `auth_token`,
which is sent as `Authorization: Bearer <token>`, and `platform_id`, which is sent as
`x-platform-id`.

Note that `api_url` defaults to the **test** environment
(`https://test.api.solvimon.com`), so a half-configured pipeline cannot write billable
events. Set it to `https://api.solvimon.com` explicitly for production.

## Usage

You can easily run `target-solvimon` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-solvimon --version
target-solvimon --help
# Test using the "Smoke Test" tap:
tap-smoke-test | target-solvimon --config /path/to/target-solvimon-config.json
```

### Sample data

[`sample_data/tickets.singer`](sample_data/tickets.singer) holds five sold tickets as
Singer messages (newline-delimited JSON), shaped like the flat columns a warehouse tap
emits: `ticket_reference`, `sold_at`, `customer_reference`, `country`, `event_name`,
`currency`, `collected_payment` and `service_fee`.

Its `customer_reference` values are placeholders: Solvimon rejects a batch whose
customer it cannot resolve, so swap in references that exist in your own environment
before running it. The same goes for `country`, which the meter defines as an enum.

[`sample_data/config.sample.json`](sample_data/config.sample.json) maps those columns
onto events of the `tickets` meter:

```json
{
  "api_key": "your_api_key_here",
  "api_url": "https://test.api.solvimon.com",
  "derive_event_id": false,
  "event_reference_field": "ticket_reference",
  "event_timestamp_field": "sold_at",
  "customer_reference_field": "customer_reference",
  "meter_reference": "tickets",
  "meter_properties": ["country", "event_name"],
  "meter_values": {
    "collected_payment": {"amount_field": "collected_payment", "currency_field": "currency"},
    "collected_payment_counter": {"number": "1"},
    "service_fee": {"amount_field": "service_fee", "currency_field": "currency"}
  },
  "extra_fields": "ignore"
}
```

Copy it, fill in your API key, and send the file to the test environment:

```bash
cp sample_data/config.sample.json config.json  # config.json is gitignored
target-solvimon --config config.json < sample_data/tickets.singer
```

Each record is sent as one event:

```json
{
  "meter_reference": "tickets",
  "customer_reference": "partner_a",
  "reference": "9d80a2b7_9bea_4599_96c1_8dea3bbb911a",
  "timestamp": "2026-09-21T13:26:00+02:00",
  "meter_properties": [
    {"reference": "country", "value": "NL"},
    {"reference": "event_name", "value": "Test event"}
  ],
  "meter_values": [
    {"reference": "collected_payment", "amount": {"currency": "EUR", "quantity": "12"}},
    {"reference": "collected_payment_counter", "number": "1"},
    {"reference": "service_fee", "amount": {"currency": "EUR", "quantity": "12"}}
  ]
}
```

That pair is the template for a real pipeline: point the settings at your own tap's
column names and the target assembles the events. Records that already carry the
Solvimon event shape need none of it — with the default `extra_fields: passthrough`
their `meter_reference`, `meter_properties` and `meter_values` are sent as they are.

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
then run:

```bash
uv run pytest
```

The tests run the target against an in-process stub of the ingest API
([`tests/stub_api.py`](tests/stub_api.py)), so they need no credentials and never send
events to Solvimon. That includes the sample ticket file, which is loaded end to end in
[`tests/test_sample_data.py`](tests/test_sample_data.py).

You can also test the `target-solvimon` CLI interface directly using `uv run`:

```bash
uv run target-solvimon --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Use Meltano to run an EL pipeline:

```bash
# Install meltano
uv tool install meltano

# Test invocation
meltano invoke target-solvimon --version

# Replay the sample ticket file into Solvimon
meltano run tap-singer-jsonl target-solvimon
```

[`tap-singer-jsonl`](https://hub.meltano.com/extractors/tap-singer-jsonl) replays
`sample_data/*.singer` as if a real tap had produced it, and the loader config in
`meltano.yml` maps the columns onto events of the `tickets` meter — the same mapping
as the config file above. Swap the extractor for your warehouse tap and point those
settings at its column names to load real data.

The mapping lives in `meltano.yml`, not in `.env`: environment variables outrank it,
so anything left in `.env` silently overrides what a pipeline configures. Keep `.env`
to credentials:

```bash
cp .env.example .env  # then fill in TARGET_SOLVIMON_API_KEY
```

Two things are pinned on the extractor because its 0.1.0 release is old: Python 3.11
(it does not support 3.12+) and `setuptools==80.9.0` (it imports `pkg_resources`,
dropped in setuptools 81). It also reads `local.folders` rather than `local.paths`,
which is broken in that release.

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
