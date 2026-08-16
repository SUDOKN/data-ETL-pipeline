import pytest
from beanie.odm.settings.document import DocumentSettings
from pure_utils.env_util import load_env

from data_etl_app.dependencies.env import ONTOLOGY_SCRIPT_ENV

# Entrypoint for the app test session: loads the root .env once for all tests.
load_env(ONTOLOGY_SCRIPT_ENV)


@pytest.fixture(autouse=True, scope="session")
def offline_document_settings():
    """Offline Beanie settings for any test constructing a Document.

    Beanie 2.0's ``init_beanie`` unconditionally runs ``buildInfo`` against a
    live server, but constructing and validating a Document only needs
    ``cls._document_settings`` populated — exactly what the synchronous
    ``Initializer.init_settings`` step does. This replicates that one step so
    document contracts (validators, round-trips) are testable without MongoDB;
    anything needing real collection I/O belongs in an ``integration`` test.
    """
    from data_etl_app.db_models import APP_DOCUMENT_MODELS

    for model in APP_DOCUMENT_MODELS:
        settings_class = getattr(model, "Settings")
        settings_vars = {
            attr: getattr(settings_class, attr)
            for attr in dir(settings_class)
            if not attr.startswith("__")
        }
        model._document_settings = DocumentSettings(**settings_vars)


@pytest.fixture
def sample_fixture():
    # This is a sample fixture that can be used in tests
    return {"key": "value"}


# Additional fixtures can be added here as needed.
