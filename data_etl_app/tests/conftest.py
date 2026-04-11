import pytest
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load .env once for the entire test session so env vars like
# GOOGLE_MAPS_API_KEY are available to integration tests.
load_data_etl_env(required=False)


@pytest.fixture
def sample_fixture():
    # This is a sample fixture that can be used in tests
    return {"key": "value"}


# Additional fixtures can be added here as needed.
