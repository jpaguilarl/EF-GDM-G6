from __future__ import annotations

import os

import pytest


@pytest.fixture(autouse=True, scope="session")
def _lighter_spark_for_integration_tests() -> None:
    """Override production SparkClient defaults with lighter config for tests.

    SparkClient defaults to local[10] + 10g heap, which is overkill for
    integration tests with tiny data sets.  Setting these env vars before
    SparkClient.__init__() makes it create a session that starts faster
    and uses less memory.  Uses setdefault so a manual override (e.g.
    CI that needs more cores) still works.
    """
    os.environ.setdefault("SPARK_DRIVER_MEMORY", "2g")
    os.environ.setdefault("SPARK_MASTER_CORES", "2")
