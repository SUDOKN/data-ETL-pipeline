import pytest

@pytest.fixture
def sample_fixture():
    # This is a sample fixture that can be used in tests
    return {"key": "value"}

# Additional fixtures can be added here as needed.