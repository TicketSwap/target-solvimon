"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import atexit
from typing import Any

import pytest
from singer_sdk.testing import get_target_test_class

from target_solvimon.target import TargetSolvimon
from tests.stub_api import StubSolvimonAPI

# The standard suite feeds real records to the target, so point it at a stub API for
# the lifetime of the test session.
_stub_api = StubSolvimonAPI()
atexit.register(_stub_api.close)

SAMPLE_CONFIG: dict[str, Any] = {
    "api_key": "test-api-key",
    "api_url": _stub_api.url,
    # Keep the suite fast if the stub ever answers with a retryable status.
    "backoff_factor": 0,
}


# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetSolvimon,
    config=SAMPLE_CONFIG,
    # Never let a developer's TARGET_SOLVIMON_* environment redirect these tests at
    # the real Solvimon API.
    parse_env_config=False,
)


class TestTargetSolvimon(StandardTargetTests):  # type: ignore[misc, valid-type] # ty: ignore[unsupported-base]
    """Standard Target Tests."""

    @pytest.fixture(scope="class")
    @classmethod
    def resource(cls):  # ruff:ignore[missing-return-type-class-method]
        """Generic external resource.

        This fixture is useful for setup and teardown of external resources,
        such output folders, tables, buckets etc. for use during testing.

        Example usage can be found in the SDK samples test suite:
        https://github.com/meltano/sdk/tree/main/tests/packages

        Returns:
            The fixture value.
        """
        return "resource"
