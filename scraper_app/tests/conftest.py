"""
Test configuration and fixtures for Chrome driver testing.
"""

import os
import sys
import tempfile
import shutil
import pytest
from unittest.mock import patch

# Add source paths for testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture(scope="session")
def chrome_test_env():
    """Session-scoped fixture for Chrome testing environment setup."""
    # Create a temporary directory for all Chrome tests
    test_base_dir = tempfile.mkdtemp(prefix="chrome_tests_")

    # Set up environment variables
    env_vars = {
        "CHROME_PROFILE_TMPDIR": test_base_dir,
        "PAGE_LOAD_TIMEOUT": "30",
        "SELENIUM_MANAGER_LOGLEVEL": "ERROR",
    }

    with patch.dict(os.environ, env_vars):
        yield {"test_base_dir": test_base_dir, "env_vars": env_vars}

    # Clean up after all tests
    if os.path.exists(test_base_dir):
        shutil.rmtree(test_base_dir, ignore_errors=True)


@pytest.fixture
def temp_profile_dir(chrome_test_env):
    """Fixture that provides a temporary profile directory for individual tests."""
    profile_dir = tempfile.mkdtemp(
        prefix="test_profile_", dir=chrome_test_env["test_base_dir"]
    )

    yield profile_dir

    # Clean up after test
    if os.path.exists(profile_dir):
        shutil.rmtree(profile_dir, ignore_errors=True)


@pytest.fixture
def mock_chrome_driver():
    """Fixture that provides a mocked Chrome driver."""
    from unittest.mock import Mock
    from selenium import webdriver

    mock_driver = Mock(spec=webdriver.Chrome)
    mock_driver.session_id = "test_session_12345"
    mock_driver.current_url = "about:blank"
    mock_driver.page_source = "<html><body>Test Page</body></html>"
    mock_driver.quit = Mock()
    mock_driver.get = Mock()
    mock_driver.set_page_load_timeout = Mock()

    return mock_driver


@pytest.fixture
def chrome_driver_manager():
    """Fixture that provides a ChromeDriverManager instance."""
    from scraper_app.utils.selenium.chrome_driver_manager import ChromeDriverManager

    # Create temporary directory for this test
    test_dir = tempfile.mkdtemp(prefix="test_chrome_manager_")

    with patch.dict(os.environ, {"CHROME_PROFILE_TMPDIR": test_dir}):
        manager = ChromeDriverManager(headless=True)
        yield manager

    # Clean up
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir, ignore_errors=True)


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (require --run-slow to run)"
    )
    config.addinivalue_line(
        "markers", "chrome_required: marks tests that require Chrome to be installed"
    )
    config.addinivalue_line("markers", "integration: marks tests as integration tests")


def pytest_collection_modifyitems(config, items):
    """Modify test collection based on command line options."""
    # Skip slow tests unless --run-slow is provided
    if not config.getoption("--run-slow", default=False):
        skip_slow = pytest.mark.skip(reason="need --run-slow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    # Skip Chrome tests unless Chrome is available or forced
    if not (
        config.getoption("--force-chrome", default=False)
        or os.getenv("RUN_CHROME_PROCESS_TESTS")
    ):
        skip_chrome = pytest.mark.skip(
            reason="need --force-chrome option or RUN_CHROME_PROCESS_TESTS=1"
        )
        for item in items:
            if "chrome_required" in item.keywords:
                item.add_marker(skip_chrome)


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--run-slow", action="store_true", default=False, help="run slow tests"
    )
    parser.addoption(
        "--force-chrome",
        action="store_true",
        default=False,
        help="force running Chrome tests even if Chrome might not be available",
    )


# Test utilities
class ChromeTestUtils:
    """Utility functions for Chrome driver tests."""

    @staticmethod
    def create_fake_chrome_structure(base_dir: str) -> str:
        """Create a fake Chrome installation structure for testing."""
        chrome_dir = os.path.join(base_dir, "chrome")
        os.makedirs(chrome_dir, exist_ok=True)

        # Create fake chrome binary
        if sys.platform == "win32":
            chrome_exe = os.path.join(chrome_dir, "chrome.exe")
        elif sys.platform == "darwin":
            chrome_exe = os.path.join(chrome_dir, "Google Chrome for Testing")
        else:
            chrome_exe = os.path.join(chrome_dir, "chrome")

        with open(chrome_exe, "w") as f:
            f.write("#!/bin/bash\necho 'Fake Chrome'")

        os.chmod(chrome_exe, 0o755)
        return chrome_exe

    @staticmethod
    def create_fake_chromedriver_structure(base_dir: str) -> str:
        """Create a fake chromedriver structure for testing."""
        driver_dir = os.path.join(base_dir, "driver")
        os.makedirs(driver_dir, exist_ok=True)

        # Create fake chromedriver binary
        if sys.platform == "win32":
            driver_exe = os.path.join(driver_dir, "chromedriver.exe")
        else:
            driver_exe = os.path.join(driver_dir, "chromedriver")

        with open(driver_exe, "w") as f:
            f.write("#!/bin/bash\necho 'Fake ChromeDriver'")

        os.chmod(driver_exe, 0o755)
        return driver_exe

    @staticmethod
    def simulate_chrome_profile_artifacts(profile_dir: str):
        """Create realistic Chrome profile artifacts for testing cleanup."""
        artifacts = [
            "Default/Preferences",
            "Default/History",
            "Default/Cookies",
            "Default/Web Data",
            "Default/Bookmarks",
            "Default/Cache/data_0",
            "Default/Cache/data_1",
            "Default/Cache/index",
            "Default/Sessions/Session_13",
            "Default/Sessions/Tabs_13",
            "ShaderCache/GPUCache/data_0",
            "ShaderCache/GPUCache/index",
            "CertificateTransparency/logs.pb",
            "Local State",
            "chrome_debug.log",
        ]

        for artifact in artifacts:
            artifact_path = os.path.join(profile_dir, artifact)
            os.makedirs(os.path.dirname(artifact_path), exist_ok=True)

            with open(artifact_path, "w") as f:
                f.write(f"Test data for {artifact}")


@pytest.fixture
def chrome_test_utils():
    """Fixture that provides ChromeTestUtils."""
    return ChromeTestUtils
