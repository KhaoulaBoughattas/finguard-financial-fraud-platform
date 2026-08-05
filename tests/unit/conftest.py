import pytest

from src.streaming.spark.session import build_local_test_session


@pytest.fixture(scope="session")
def spark():
    """A real, local (no Delta) Spark session shared across the Phase 3
    test module for the session, torn down at the end of the run."""
    session = build_local_test_session()
    yield session
    session.stop()
