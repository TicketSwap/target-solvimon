# AGENTS.md - AI Agent Development Guide for target-solvimon

This document provides guidance for AI coding agents and developers working on this Singer target.

## Project Overview

- **Project Type**: Singer Target
- **Destination**: Solvimon
- **Serialization Method**: Per batch
- **Framework**: Meltano Singer SDK

## Architecture

This target follows the Singer specification and uses the Meltano Singer SDK to load data into Solvimon.

### Key Components

1. **Target Class** (`target_solvimon/target.py`): Main entry point, defines configuration and sinks
1. **Sink Class** (`target_solvimon/sinks.py`): Handles data loading and processing
1. **Configuration**: Defines connection parameters and loading options

## Development Guidelines for AI Agents

### Understanding Singer Target Concepts

Before making changes, ensure you understand these concepts:

- **Records**: Individual data items received from taps
- **Schemas**: JSON Schema definitions describing record structure
- **State**: Bookmark information passed through from taps
- **Batching**: Grouping records for efficient loading
- **MAX_SIZE**: Maximum records per batch

### Serialization Methods

This target uses **Per batch** serialization:

#### Per Batch Serialization

- `process_batch()`: Called for groups of records
- Records accumulated up to `max_size` (default 10000)
- Better throughput for bulk operations
- Higher memory usage
- Example use cases: Bulk APIs, file uploads, batch processing

Key methods to implement:

```python
def process_batch(self, context: dict) -> None:
    """Process a batch of records."""
    records = context["records"]
    # Transform batch
    # Send to destination
    # Handle partial failures
```

Important properties:

- `max_size`: Maximum records per batch
- `context["records"]`: List of records in current batch
  

### Common Tasks

#### Modifying Data Loading Logic

1. Override sink methods in `target_solvimon/sinks.py`
1. Add data transformation in pre-processing
1. Handle destination-specific formatting
1. Implement error handling and retries

#### Adding Configuration Options

Define new config properties in target class:

```python
class SolvimonTarget(Target):
    config_jsonschema = PropertiesList(
        Property("api_url", StringType, required=True),
        Property("api_key", StringType, required=True, secret=True),
        Property("timeout", IntegerType, default=300),
    ).to_dict()
```

Configuration best practices:

- Mark secrets with `secret=True`
- Provide sensible defaults
- Validate in `__init__` or `validate_config()`
- Document all options in README

#### Connection Management

For API/HTTP targets:

```python
# Use requests.Session for connection reuse
import requests

self.session = requests.Session()
self.session.headers.update({"Authorization": f"Bearer {api_key}"})
```

Implement retry logic for transient failures.

#### Error Handling

Implement robust error handling:

```python
try:
    # Load data
    result = self.load_data(records)
except RetryableError as e:
    # SDK will retry
    raise e
except FatalError as e:
    # Log and fail the sync
    self.logger.error(f"Fatal error: {e}")
    raise e
```

Types of errors:

- **Retryable**: Network issues, rate limits, temporary failures
- **Fatal**: Authentication errors, invalid data, configuration issues

#### Type Mapping

For non-SQL targets, handle type conversion:

```python
def prepare_record(self, record: dict) -> dict:
    """Convert types for destination."""
    if "timestamp" in record:
        record["timestamp"] = parse_datetime(record["timestamp"])
    return record
```

### Testing

Test your target implementation:

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest

# Test with sample data
cat sample_data.singer | target-solvimon --config config.json

# Test with a tap
tap-something --config tap_config.json | target-solvimon --config config.json
```

Create test fixtures:

```python
# tests/test_core.py
def test_target_loads_data():
    with open("tests/fixtures/input.singer") as f:
        lines = f.readlines()

    # Process lines
    # Verify data loaded correctly
```

### Performance Optimization

1. **Batching**: Use appropriate batch sizes

   - Too small: Many API calls, slow
   - Too large: Memory issues, timeouts
   - Start with 1000-5000, adjust based on record size

1. **Parallel Processing**: For multi-table targets

   - SDK handles streams in sequence by default
   - Consider async operations within batches

1. **Connection Pooling**: Reuse connections

   - Use `requests.Session` for HTTP
   - Connection pools for databases

1. **Memory Management**:

   - Don't accumulate records beyond batch size
   - Stream large files rather than loading into memory
   - Use generators where possible

### Schema Handling

For schema-aware targets:

- Validate records against schema
- Handle schema evolution
- Map nested objects appropriately
- Consider denormalization for flat destinations
  ### State Management

Targets receive and forward state:

- Don't modify state in targets
- Emit state messages as received
- State used for tap bookmarking

```python
def process_state_message(self, message_dict: dict) -> None:
    """Handle state message."""
    super().process_state_message(message_dict)
    # Optional: Checkpoint or log state
```

### Keeping meltano.yml and Target Settings in Sync

When this target is used with Meltano, the settings defined in `meltano.yml` must stay in sync with the `config_jsonschema` in the target class. Configuration drift between these two sources causes confusion and runtime errors.

**When to sync:**

- Adding new configuration properties to the target
- Removing or renaming existing properties
- Changing property types, defaults, or descriptions
- Marking properties as required or secret

**How to sync:**

1. Update `config_jsonschema` in `target_solvimon/target.py`
1. Update the corresponding `settings` block in `meltano.yml`
1. Update `.env.example` with the new environment variable

Example - adding a new `batch_size` setting:

```python
# target_solvimon/target.py
config_jsonschema = th.PropertiesList(
    th.Property("api_url", th.StringType, required=True),
    th.Property("api_key", th.StringType, required=True, secret=True),
    th.Property("batch_size", th.IntegerType, default=1000),  # New setting
).to_dict()
```

```yaml
# meltano.yml
plugins:
  loaders:
    - name: target-solvimon
      settings:
        - name: api_url
          kind: string
        - name: api_key
          kind: string
          sensitive: true
        - name: batch_size  # New setting
          kind: integer
          value: 1000
```

```bash
# .env.example
TARGET_SOLVIMON_API_URL=https://api.example.com
TARGET_SOLVIMON_API_KEY=your_api_key_here
TARGET_SOLVIMON_BATCH_SIZE=1000  # New setting
```

**Setting kind mappings:**

| Python Type | Meltano Kind |
|-------------|--------------|
| `StringType` | `string` |
| `IntegerType` | `integer` |
| `BooleanType` | `boolean` |
| `NumberType` | `number` |
| `DateTimeType` | `date_iso8601` |
| `ArrayType` | `array` |
| `ObjectType` | `object` |

Any properties with `secret=True` should be marked with `sensitive: true` in `meltano.yml`.

**Best practices:**

- Always update all three files (`target.py`, `meltano.yml`, `.env.example`) in the same commit
- Use the same default values in all locations
- Keep descriptions consistent between code docstrings and `meltano.yml` `description` fields

> **Note:** This guidance is consistent with tap and mapper templates in the Singer SDK. See the [SDK documentation](https://sdk.meltano.com) for canonical reference.

### Common Pitfalls

1. **Memory Leaks**: Clear batch data after processing
1. **Connection Limits**: Close connections properly
1. **Partial Failures**: Handle failed records in batch
1. **Schema Changes**: Handle additive schema changes
1. **Rate Limiting**: Implement backoff and retry
1. **Authentication**: Refresh tokens before expiry
1. **Timezone Issues**: Use UTC consistently

### SDK Resources

- [Singer SDK Documentation](https://sdk.meltano.com)
- [Singer Spec](https://hub.meltano.com/singer/spec)
- [SDK Reference](https://sdk.meltano.com/en/latest/reference.html)
- [Batch Context](https://sdk.meltano.com/en/latest/batch.html)

### Best Practices

1. **Logging**: Use structured logging with `self.logger`
1. **Idempotency**: Handle duplicate records gracefully
1. **Transactions**: Use transactions for consistency
1. **Validation**: Validate data before loading
1. **Documentation**: Update README with config options
1. **Type Safety**: Use type hints
1. **Testing**: Test with various data types and edge cases
1. **Error Messages**: Provide actionable error information

## File Structure

```
target-solvimon/
├── target_solvimon/
│   ├── __init__.py
│   ├── target.py       # Main target class
│   └── sinks.py        # Sink implementation
├── tests/
│   ├── __init__.py
│   └── test_core.py
├── config.json         # Example configuration
├── pyproject.toml      # Dependencies and metadata
└── README.md          # User documentation
```

## Additional Resources

- Project README: See `README.md` for setup and usage
- Singer SDK: https://sdk.meltano.com
- Meltano: https://meltano.com
- Singer Specification: https://hub.meltano.com/singer/spec

## Making Changes

When implementing changes:

1. Understand the data flow: records → processing → destination
1. Follow Singer and SDK patterns
1. Test with real data from various taps
1. Handle edge cases (nulls, large records, schema changes)
1. Update documentation
1. Ensure backward compatibility
1. Run linting and type checking

## Questions?

If you're uncertain about an implementation:

- Check SDK documentation for sink examples
- Review other Singer targets for patterns
- Test incrementally with sample data
- Validate against the Singer specification
- Consider data consistency and idempotency

## Bumping the Singer SDK Version

When upgrading the `singer-sdk` dependency in `pyproject.toml`, follow these steps to avoid breaking changes:

1. **Check the deprecation guide** before upgrading:
   https://sdk.meltano.com/en/latest/deprecation.html

   The deprecation page lists APIs scheduled for removal in each release, along with migration instructions. Review the entries for every version between your current version and the target version.

1. **Update the dependency** in `pyproject.toml`:

   ```toml
   [project]
   dependencies = [
       "singer-sdk~=X.Y",  # Bump to the new version
   ]
   ```

1. **Re-sync your environment** and run the full test suite:

   ```bash
   uv sync
   uv run pytest
   ```

1. **Address deprecation warnings**: Run with warnings enabled to catch anything that will become an error in a future release:

   ```bash
   uv run pytest -W error::DeprecationWarning
   ```

1. **Check the changelog** for any behavioral changes that affect your target, even if not surfaced by warnings (e.g. batching, sink processing, schema handling).

## Reporting SDK Issues

If you encounter a bug or missing feature in the **Meltano Singer SDK itself** (not in this target), please open an issue at https://github.com/meltano/sdk/issues/new/choose.

Use the appropriate issue template:

- **Bug Report** (`bug.yml`): For unexpected behavior, errors, or regressions in the SDK. Fill in the Singer SDK version, Python version, bug scope (e.g. "Targets"), and a clear description with reproduction steps.
- **Feature Request** (`feature.yml`): For new SDK capabilities you'd like to see. Select the relevant scope and describe the desired behavior.
- **Documentation** (`docs.yml`): For incorrect or missing documentation.

Before filing, search existing issues to avoid duplicates. Include the SDK version (`uv run target-solvimon --version`), Python version, and a minimal reproduction case when reporting bugs.
