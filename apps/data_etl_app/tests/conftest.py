import pytest
from pure_utils.env_util import load_env

from data_etl_app.dependencies.env import ONTOLOGY_SCRIPT_ENV

# Entrypoint for the app test session: loads the root .env once for all tests.
load_env(ONTOLOGY_SCRIPT_ENV)


@pytest.fixture
def sample_fixture():
    # This is a sample fixture that can be used in tests
    return {"key": "value"}


# Additional fixtures can be added here as needed.
