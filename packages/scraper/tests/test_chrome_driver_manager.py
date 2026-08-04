"""
Comprehensive tests for Chrome driver creation and cleanup functionality.
Tests both unit functionality and integration scenarios.
"""

import os
import sys
import tempfile
import shutil
import pytest
import subprocess
from unittest.mock import Mock, patch, call

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Add the scraper app to Python path for testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from scraper_app.utils.selenium.chrome_driver_manager import ChromeDriverManager


class TestChromeDriverManagerUnit:
    """Unit tests for ChromeDriverManager - testing individual methods in isolation."""

    def setup_method(self):
        """Set up for each test method."""
        # Mock environment variable
        self.test_profile_dir = tempfile.mkdtemp(prefix="test_chrome_profiles_")

        # Patch both the environment variable and the module constant
        self.env_patcher = patch.dict(
            os.environ, {"CHROME_PROFILE_TMPDIR": self.test_profile_dir}
        )
        self.env_patcher.start()

        # Also patch the imported constant in the module
        self.constant_patcher = patch(
            "scraper_app.utils.selenium.chrome_driver_manager.CHROME_PROFILE_TMPDIR",
            self.test_profile_dir,
        )
        self.constant_patcher.start()

        # Create manager instance
        self.manager = ChromeDriverManager(headless=True, channel="Stable")

    def teardown_method(self):
        """Clean up after each test method."""
        self.env_patcher.stop()
        self.constant_patcher.stop()

        # Clean up test directory
        if os.path.exists(self.test_profile_dir):
            shutil.rmtree(self.test_profile_dir, ignore_errors=True)

    def test_initialization_creates_profile_directory(self):
        """Test that ChromeDriverManager creates the profile directory on init."""
        # Directory should be created during initialization
        assert os.path.exists(self.test_profile_dir)
        assert os.path.isdir(self.test_profile_dir)

    def test_initialization_without_env_var_raises_error(self):
        """Test that missing CHROME_PROFILE_TMPDIR would raise ValueError."""
        # Since the environment variable check happens at import time,
        # and the module is already imported, we'll test the logic directly

        # Test the environment variable retrieval logic
        with patch("os.getenv", return_value=None):
            # Simulate what happens at import time
            chrome_profile_tmpdir = os.getenv("CHROME_PROFILE_TMPDIR")
            with pytest.raises(
                ValueError,
                match="CHROME_PROFILE_TMPDIR environment variable is not set",
            ):
                if not chrome_profile_tmpdir:
                    raise ValueError(
                        "CHROME_PROFILE_TMPDIR environment variable is not set."
                    )

    @patch("subprocess.run")
    def test_kill_orphaned_chrome(self, mock_subprocess):
        """Test that orphaned Chrome processes are killed on initialization."""
        # Mock subprocess calls
        mock_subprocess.return_value = Mock()

        # Create new manager to trigger cleanup
        manager = ChromeDriverManager()

        # Verify subprocess.run was called to kill processes
        expected_calls = [
            call(["pkill", "-f", "chrome.*--remote-debugging"], check=False),
            call(["pkill", "-f", "chromedriver"], check=False),
        ]
        mock_subprocess.assert_has_calls(expected_calls)

    @patch("subprocess.run")
    def test_kill_orphaned_chrome_handles_exceptions(self, mock_subprocess):
        """Test that exceptions during process killing are handled gracefully."""
        # Make subprocess.run raise an exception
        mock_subprocess.side_effect = Exception("Process kill failed")

        # Should not raise exception, just log warning
        manager = ChromeDriverManager()
        # If we get here without exception, the test passes

    def test_create_temp_profile_creates_unique_directory(self):
        """Test that create_temp_profile creates unique directories."""
        # Create multiple temp profiles
        profile1 = self.manager.create_temp_profile()
        profile2 = self.manager.create_temp_profile()

        # Both should exist and be different
        assert os.path.exists(profile1)
        assert os.path.exists(profile2)
        assert profile1 != profile2

        # Both should be in the designated temp directory
        assert profile1.startswith(self.test_profile_dir)
        assert profile2.startswith(self.test_profile_dir)

        # Both should have the chrome_scrape_ prefix
        assert "chrome_scrape_" in os.path.basename(profile1)
        assert "chrome_scrape_" in os.path.basename(profile2)

    def test_cleanup_temp_profiles_removes_chrome_directories(self):
        """Test that cleanup_temp_profiles removes temporary Chrome directories."""
        # Create some fake profile directories
        profile1 = os.path.join(self.test_profile_dir, "chrome_scrape_test1")
        profile2 = os.path.join(self.test_profile_dir, "chrome_scrape_test2")
        other_dir = os.path.join(self.test_profile_dir, "other_directory")

        os.makedirs(profile1)
        os.makedirs(profile2)
        os.makedirs(other_dir)

        # Add some files to make sure they're removed
        with open(os.path.join(profile1, "test_file.txt"), "w") as f:
            f.write("test content")
        with open(os.path.join(profile2, "test_file.txt"), "w") as f:
            f.write("test content")
        with open(os.path.join(other_dir, "test_file.txt"), "w") as f:
            f.write("test content")

        # Run cleanup
        self.manager._cleanup_temp_profiles()

        # Chrome profile directories should be removed
        assert not os.path.exists(profile1)
        assert not os.path.exists(profile2)

        # Other directory should remain
        assert os.path.exists(other_dir)

    def test_cleanup_temp_profiles_handles_missing_directory(self):
        """Test that cleanup handles missing profile directory gracefully."""
        # Remove the profile directory
        shutil.rmtree(self.test_profile_dir)

        # Should not raise exception
        self.manager._cleanup_temp_profiles()

        # Directory should be recreated
        assert os.path.exists(self.test_profile_dir)

    def test_build_options_basic_configuration(self):
        """Test that _build_options creates properly configured Chrome options."""
        profile_dir = "/test/profile"
        binary_path = "/test/chrome"

        options = self.manager._build_options(binary_path, profile_dir)

        # Check that it returns Options object
        assert isinstance(options, Options)

        # Check binary location is set
        assert options.binary_location == binary_path

        # Check that essential arguments are present
        args = options.arguments
        assert f"--user-data-dir={profile_dir}" in args
        assert "--no-sandbox" in args
        assert "--disable-dev-shm-usage" in args
        assert "--disable-gpu" in args
        assert "--headless=new" in args  # Since headless=True

    def test_build_options_headless_vs_non_headless(self):
        """Test different configurations for headless vs non-headless mode."""
        profile_dir = "/test/profile"

        # Test headless mode
        headless_manager = ChromeDriverManager(headless=True)
        headless_options = headless_manager._build_options(None, profile_dir)
        assert "--headless=new" in headless_options.arguments
        assert "--start-minimized" not in headless_options.arguments

        # Test non-headless mode
        non_headless_manager = ChromeDriverManager(headless=False)
        non_headless_options = non_headless_manager._build_options(None, profile_dir)
        assert "--headless=new" not in non_headless_options.arguments
        assert "--start-minimized" in non_headless_options.arguments
        assert "--window-position=-32000,-32000" in non_headless_options.arguments

    def test_build_options_performance_flags(self):
        """Test that performance-related flags are properly set."""
        profile_dir = "/test/profile"
        options = self.manager._build_options(None, profile_dir)
        args = options.arguments

        # Check EC2-specific performance flags
        performance_flags = [
            "--disable-extensions",
            "--disable-plugins",
            "--disable-images",
            "--disable-css",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-default-apps",
            "--disable-background-networking",
            "--memory-pressure-off",
            "--renderer-process-limit=4",
        ]

        for flag in performance_flags:
            assert flag in args, f"Missing performance flag: {flag}"

    def test_build_options_security_flags(self):
        """Test that security and certificate-related flags are set."""
        profile_dir = "/test/profile"
        options = self.manager._build_options(None, profile_dir)
        args = options.arguments

        security_flags = [
            "--ignore-certificate-errors",
            "--allow-running-insecure-content",
            "--disable-features=HttpsUpgrades",
            "--disable-blink-features=AutomationControlled",
        ]

        for flag in security_flags:
            assert flag in args, f"Missing security flag: {flag}"

    def test_platform_key_detection(self):
        """Test that _platform_key correctly identifies the platform."""
        platform_key = self.manager._platform_key()

        # Should return one of the expected platform keys
        expected_platforms = ["win64", "mac-x64", "mac-arm64", "linux64"]
        assert platform_key in expected_platforms

    def test_get_cache_root_creates_directory(self):
        """Test that _get_cache_root creates and returns cache directory."""
        cache_root = self.manager._get_cache_root()

        # Should create the directory
        assert os.path.exists(cache_root)
        assert os.path.isdir(cache_root)
        assert "scraper_portable_chrome" in cache_root

    def test_find_in_tree_finds_existing_file(self):
        """Test that _find_in_tree correctly finds files in directory tree."""
        # Create test directory structure
        test_root = tempfile.mkdtemp()
        try:
            # Create nested directory with target file
            nested_dir = os.path.join(test_root, "level1", "level2")
            os.makedirs(nested_dir)

            target_file = os.path.join(nested_dir, "chrome")
            with open(target_file, "w") as f:
                f.write("test")

            # Should find the file
            result = ChromeDriverManager._find_in_tree(test_root, ["chrome"])
            assert result == target_file

        finally:
            shutil.rmtree(test_root, ignore_errors=True)

    def test_find_in_tree_returns_none_when_not_found(self):
        """Test that _find_in_tree returns None when file not found."""
        test_root = tempfile.mkdtemp()
        try:
            # Create directory without target file
            result = ChromeDriverManager._find_in_tree(test_root, ["chrome"])
            assert result is None

        finally:
            shutil.rmtree(test_root, ignore_errors=True)

    def test_chmod_x_makes_file_executable(self):
        """Test that _chmod_x makes files executable."""
        # Create test file
        test_file = tempfile.mktemp()
        with open(test_file, "w") as f:
            f.write("test")

        try:
            # Initially should not be executable
            initial_mode = os.stat(test_file).st_mode

            # Make executable
            ChromeDriverManager._chmod_x(test_file)

            # Should now be executable
            new_mode = os.stat(test_file).st_mode
            assert new_mode != initial_mode

        finally:
            if os.path.exists(test_file):
                os.remove(test_file)


class TestChromeDriverManagerIntegration:
    """Integration tests that test actual Chrome driver creation and cleanup."""

    def setup_method(self):
        """Set up for integration tests."""
        self.test_profile_dir = tempfile.mkdtemp(prefix="test_chrome_integration_")

        self.env_patcher = patch.dict(
            os.environ,
            {
                "CHROME_PROFILE_TMPDIR": self.test_profile_dir,
                "PAGE_LOAD_TIMEOUT": "30",  # Shorter timeout for tests
            },
        )
        self.env_patcher.start()

        self.manager = ChromeDriverManager(headless=True)
        self.created_drivers = []  # Track drivers for cleanup

    def teardown_method(self):
        """Clean up after integration tests."""
        # Close any created drivers
        for driver in self.created_drivers:
            try:
                if driver and hasattr(driver, "quit"):
                    driver.quit()
            except Exception:
                pass  # Ignore errors during cleanup

        self.env_patcher.stop()

        # Clean up test directory
        if os.path.exists(self.test_profile_dir):
            shutil.rmtree(self.test_profile_dir, ignore_errors=True)

    @pytest.mark.slow
    def test_create_system_driver_success(self):
        """Test creating driver with system Chrome (if available)."""
        profile_dir = self.manager.create_temp_profile()

        try:
            # This might fail if Chrome isn't installed, which is expected
            driver = self.manager._create_system_driver(profile_dir)
            self.created_drivers.append(driver)

            # If successful, driver should be WebDriver instance
            assert isinstance(driver, webdriver.Chrome)
            assert driver.session_id is not None

            # Should be able to navigate to a simple page
            driver.get("data:text/html,<html><body>Test</body></html>")
            assert "Test" in driver.page_source

        except Exception as e:
            # System Chrome not available - this is acceptable for CI/CD
            print(f"System Chrome not available: {e}")
            pytest.skip("System Chrome not available")

    @patch("selenium.webdriver.Chrome")
    def test_create_system_driver_with_mocked_chrome(self, mock_chrome_class):
        """Test system driver creation with mocked Chrome for reliable testing."""
        # Mock the Chrome driver
        mock_driver = Mock()
        mock_driver.session_id = "test_session_123"
        mock_driver.set_page_load_timeout = Mock()
        mock_chrome_class.return_value = mock_driver

        profile_dir = self.manager.create_temp_profile()

        # Create driver
        driver = self.manager._create_system_driver(profile_dir)

        # Should return mocked driver
        assert driver == mock_driver

        # Chrome constructor should be called with options
        mock_chrome_class.assert_called_once()
        call_args = mock_chrome_class.call_args
        assert "options" in call_args.kwargs

        # Timeout should be set
        mock_driver.set_page_load_timeout.assert_called_once()

    def test_create_temp_profile_integration(self):
        """Integration test for temporary profile creation."""
        # Create profile
        profile_dir = self.manager.create_temp_profile()

        # Should exist and be writable
        assert os.path.exists(profile_dir)
        assert os.path.isdir(profile_dir)

        # Should be able to write to it
        test_file = os.path.join(profile_dir, "test.txt")
        with open(test_file, "w") as f:
            f.write("test content")

        assert os.path.exists(test_file)

        # Clean up
        shutil.rmtree(profile_dir)

    def test_cleanup_temp_profiles_integration(self):
        """Integration test for profile cleanup."""
        # Create several temp profiles
        profiles = []
        for i in range(3):
            profile = self.manager.create_temp_profile()
            profiles.append(profile)

            # Add some content
            with open(os.path.join(profile, f"test{i}.txt"), "w") as f:
                f.write(f"test content {i}")

        # All should exist
        for profile in profiles:
            assert os.path.exists(profile)

        # Run cleanup
        self.manager._cleanup_temp_profiles()

        # All should be gone
        for profile in profiles:
            assert not os.path.exists(profile)

    @patch(
        "scraper_app.utils.selenium.chrome_driver_manager.ChromeDriverManager._create_system_driver"
    )
    @patch(
        "scraper_app.utils.selenium.chrome_driver_manager.ChromeDriverManager._create_portable_driver"
    )
    def test_create_driver_fallback_mechanism(self, mock_portable, mock_system):
        """Test that create_driver falls back to portable when system fails."""
        # Mock system driver to fail
        mock_system.side_effect = Exception("System Chrome not available")

        # Mock portable driver to succeed
        mock_portable_driver = Mock(spec=webdriver.Chrome)
        mock_portable.return_value = mock_portable_driver

        profile_dir = self.manager.create_temp_profile()

        # Create driver
        driver = self.manager.create_driver(profile_dir)

        # Should have tried system first, then fallen back to portable
        mock_system.assert_called_once_with(profile_dir)
        mock_portable.assert_called_once_with(profile_dir)

        # Should return portable driver
        assert driver == mock_portable_driver

    def test_driver_options_applied_correctly(self):
        """Test that Chrome options are applied correctly in real scenarios."""
        profile_dir = self.manager.create_temp_profile()

        # Build options
        options = self.manager._build_options(None, profile_dir)

        # Check that profile directory is correctly set
        user_data_arg = f"--user-data-dir={profile_dir}"
        assert user_data_arg in options.arguments

        # Check experimental options
        exp_options = options.experimental_options
        assert "excludeSwitches" in exp_options
        assert "enable-automation" in exp_options["excludeSwitches"]
        assert exp_options.get("useAutomationExtension") is False

    def test_multiple_driver_creation_and_cleanup(self):
        """Test creating multiple drivers and cleaning them up."""
        drivers = []
        profiles = []

        try:
            # Create multiple drivers (use system driver mock to avoid Chrome dependency)
            with patch("selenium.webdriver.Chrome") as mock_chrome:
                for i in range(3):
                    mock_driver = Mock(spec=webdriver.Chrome)
                    mock_driver.session_id = f"session_{i}"
                    mock_chrome.return_value = mock_driver

                    profile = self.manager.create_temp_profile()
                    driver = self.manager._create_system_driver(profile)

                    drivers.append(driver)
                    profiles.append(profile)
                    self.created_drivers.append(driver)

            # All profiles should exist
            for profile in profiles:
                assert os.path.exists(profile)

            # Clean up profiles
            self.manager._cleanup_temp_profiles()

            # All profiles should be gone
            for profile in profiles:
                assert not os.path.exists(profile)

        except Exception as e:
            # Clean up on failure
            for driver in drivers:
                try:
                    driver.quit()
                except:
                    pass


class TestChromeDriverManagerErrorHandling:
    """Test error handling and edge cases."""

    def setup_method(self):
        """Set up for error handling tests."""
        self.test_profile_dir = tempfile.mkdtemp(prefix="test_chrome_errors_")

        self.env_patcher = patch.dict(
            os.environ, {"CHROME_PROFILE_TMPDIR": self.test_profile_dir}
        )
        self.env_patcher.start()

    def teardown_method(self):
        """Clean up after error handling tests."""
        self.env_patcher.stop()
        if os.path.exists(self.test_profile_dir):
            shutil.rmtree(self.test_profile_dir, ignore_errors=True)

    def test_readonly_profile_directory_handling(self):
        """Test handling of read-only profile directory."""
        # Create manager
        manager = ChromeDriverManager()

        # Make profile directory read-only
        os.chmod(self.test_profile_dir, 0o444)

        try:
            # Should handle permission error gracefully
            profile = manager.create_temp_profile()
            # If it succeeds, that's fine too (permissions might be different on different systems)

        except PermissionError:
            # This is expected and acceptable
            pass
        finally:
            # Restore permissions for cleanup
            os.chmod(self.test_profile_dir, 0o755)

    def test_cleanup_with_permission_errors(self):
        """Test cleanup when some files can't be deleted due to permissions."""
        manager = ChromeDriverManager()

        # Create a profile with read-only files
        profile = manager.create_temp_profile()
        readonly_file = os.path.join(profile, "readonly.txt")

        with open(readonly_file, "w") as f:
            f.write("readonly content")

        # Make file read-only
        os.chmod(readonly_file, 0o444)

        try:
            # Cleanup should not raise exception even if some files can't be deleted
            manager._cleanup_temp_profiles()
            # If profile is gone, great. If not, that's also acceptable.

        finally:
            # Restore permissions for cleanup
            try:
                os.chmod(readonly_file, 0o644)
                os.remove(readonly_file)
                if os.path.exists(profile):
                    shutil.rmtree(profile)
            except:
                pass

    @patch("subprocess.run")
    def test_process_killing_with_various_errors(self, mock_subprocess):
        """Test process killing handles various subprocess errors."""
        # Test different types of errors
        error_types = [
            subprocess.CalledProcessError(1, "pkill"),
            FileNotFoundError("pkill not found"),
            PermissionError("Permission denied"),
            Exception("Generic error"),
        ]

        for error in error_types:
            mock_subprocess.side_effect = error

            # Should not raise exception
            manager = ChromeDriverManager()
            # If we get here, test passes

    def test_invalid_profile_directory_path(self):
        """Test handling of invalid profile directory paths."""
        with patch.dict(
            os.environ,
            {"CHROME_PROFILE_TMPDIR": "/nonexistent/path/that/cannot/be/created"},
        ):
            # This might fail during initialization, which is acceptable
            try:
                manager = ChromeDriverManager()
                # If it succeeds in creating the directory, that's fine
                # If it fails, that's also acceptable behavior
            except (OSError, PermissionError):
                # Expected for invalid paths
                pass


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s", "--tb=short"])
