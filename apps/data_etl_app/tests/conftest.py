import pytest
from pure_utils.env_util import load_env

# Load .env once for the entire test session so env vars like
# GOOGLE_MAPS_API_KEY are available to integration tests.
load_env([])


@pytest.fixture
def sample_fixture():
    # This is a sample fixture that can be used in tests
    return {"key": "value"}


# Additional fixtures can be added here as needed.
