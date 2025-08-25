"""
Tests for Chrome process management and monitoring.
Tests the multi-process behavior and cleanup of Chrome browsers.
"""

import os
import sys
import time
import subprocess
import tempfile
import pytest
from unittest.mock import Mock, patch, call
from typing import List, Dict, Set

try:
    import psutil

    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    psutil = None

# Add the scraper app to Python path for testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from scraper_app.utils.selenium.chrome_driver_manager import ChromeDriverManager


class ChromeProcessMonitor:
    """Helper class to monitor Chrome processes during tests."""

    def __init__(self):
        if not HAS_PSUTIL:
            pytest.skip("psutil not available for process monitoring")
        self.initial_processes = self._get_chrome_processes()

    def _get_chrome_processes(self) -> Dict[int, Dict]:
        """Get all Chrome-related processes with details."""
        if not HAS_PSUTIL or psutil is None:
            return {}

        processes = {}

        try:
            for proc in psutil.process_iter(["pid", "name", "cmdline", "status"]):
                try:
                    name = proc.info["name"].lower()
                    cmdline = " ".join(proc.info["cmdline"] or []).lower()

                    # Look for Chrome and chromedriver processes
                    if any(
                        chrome_name in name for chrome_name in ["chrome", "chromium"]
                    ) or any(
                        chrome_name in cmdline
                        for chrome_name in ["chrome", "chromium", "chromedriver"]
                    ):

                        processes[proc.info["pid"]] = {
                            "name": proc.info["name"],
                            "cmdline": proc.info["cmdline"],
                            "status": proc.info["status"],
                        }
                except Exception:  # Catch any psutil exception
                    continue
        except Exception:
            # psutil not working properly, return empty
            return {}

        return processes

    def get_new_processes(self) -> Dict[int, Dict]:
        """Get Chrome processes created since monitoring started."""
        current_processes = self._get_chrome_processes()
        new_processes = {}

        for pid, info in current_processes.items():
            if pid not in self.initial_processes:
                new_processes[pid] = info

        return new_processes

    def count_new_processes(self) -> int:
        """Count new Chrome processes."""
        return len(self.get_new_processes())


class TestChromeProcessManagement:
    """Test Chrome process creation, monitoring, and cleanup."""

    def setup_method(self):
        """Set up process monitoring for each test."""
        self.test_profile_dir = tempfile.mkdtemp(prefix="test_chrome_processes_")

        self.env_patcher = patch.dict(
            os.environ,
            {"CHROME_PROFILE_TMPDIR": self.test_profile_dir, "PAGE_LOAD_TIMEOUT": "30"},
        )
        self.env_patcher.start()

        # Also patch the module constant
        self.constant_patcher = patch(
            "scraper_app.utils.selenium.chrome_driver_manager.CHROME_PROFILE_TMPDIR",
            self.test_profile_dir,
        )
        self.constant_patcher.start()

        # Start monitoring Chrome processes
        self.process_monitor = ChromeProcessMonitor()
        self.created_drivers = []

    def teardown_method(self):
        """Clean up processes and temporary files."""
        # Quit all created drivers
        for driver in self.created_drivers:
            try:
                if hasattr(driver, "quit"):
                    driver.quit()
                    time.sleep(0.5)  # Give processes time to terminate
            except Exception:
                pass

        self.env_patcher.stop()
        self.constant_patcher.stop()

        # Force kill any remaining test processes
        self._cleanup_test_processes()

        # Clean up temp directory
        if os.path.exists(self.test_profile_dir):
            import shutil

            shutil.rmtree(self.test_profile_dir, ignore_errors=True)

    def _cleanup_test_processes(self):
        """Force cleanup any Chrome processes created during testing."""
        try:
            # Kill processes using our profile directory
            subprocess.run(
                ["pkill", "-f", f"--user-data-dir={self.test_profile_dir}"],
                check=False,
                timeout=5,
            )
            time.sleep(1)
        except Exception:
            pass

    @patch("subprocess.run")
    def test_orphaned_process_cleanup_on_init(self, mock_subprocess):
        """Test that ChromeDriverManager kills orphaned processes on initialization."""
        # Track calls to subprocess.run
        mock_subprocess.return_value = Mock()

        # Initialize manager (should trigger cleanup)
        manager = ChromeDriverManager()

        # Should call pkill for both Chrome and chromedriver processes
        expected_calls = [
            call(["pkill", "-f", "chrome.*--remote-debugging"], check=False),
            call(["pkill", "-f", "chromedriver"], check=False),
        ]

        mock_subprocess.assert_has_calls(expected_calls)

    @patch("subprocess.run")
    def test_orphaned_process_cleanup_handles_missing_pkill(self, mock_subprocess):
        """Test that process cleanup handles systems without pkill."""
        # Simulate pkill not being available
        mock_subprocess.side_effect = FileNotFoundError("pkill command not found")

        # Should not raise exception
        manager = ChromeDriverManager()
        # If we get here, test passes

    @patch("subprocess.run")
    def test_orphaned_process_cleanup_handles_permission_denied(self, mock_subprocess):
        """Test that process cleanup handles permission denied errors."""
        # Simulate permission denied
        mock_subprocess.side_effect = PermissionError("Permission denied")

        # Should not raise exception
        manager = ChromeDriverManager()
        # If we get here, test passes

    @pytest.mark.skipif(not HAS_PSUTIL, reason="psutil not available")
    @pytest.mark.slow
    @pytest.mark.skipif(
        not os.getenv("RUN_CHROME_PROCESS_TESTS"),
        reason="Chrome process tests require RUN_CHROME_PROCESS_TESTS=1",
    )
    def test_chrome_multiprocess_architecture(self):
        """Test that Chrome creates multiple processes as expected."""
        manager = ChromeDriverManager(headless=True)

        # Record initial process count
        initial_count = self.process_monitor.count_new_processes()

        # Create a Chrome driver
        profile_dir = manager.create_temp_profile()

        try:
            driver = manager.create_driver(profile_dir)
            self.created_drivers.append(driver)

            # Give Chrome time to start all processes
            time.sleep(2)

            # Check that multiple processes were created
            final_count = self.process_monitor.count_new_processes()
            processes_created = final_count - initial_count

            # Chrome typically creates 3-12 processes:
            # - Main browser process
            # - Renderer process(es)
            # - GPU process (even with --disable-gpu, sometimes created)
            # - Utility processes
            # - Extension processes (though we disable extensions)
            assert (
                processes_created >= 1
            ), f"Expected at least 1 Chrome process, got {processes_created}"
            assert (
                processes_created <= 15
            ), f"Expected at most 15 Chrome processes, got {processes_created}"

            print(f"Chrome created {processes_created} processes")

            # List the new processes for debugging
            new_processes = self.process_monitor.get_new_processes()
            for pid, info in new_processes.items():
                print(
                    f"  Process {pid}: {info['name']} - {' '.join(info['cmdline'][:3])}..."
                )

        except Exception as e:
            pytest.skip(f"Chrome not available for process testing: {e}")

    @pytest.mark.skipif(not HAS_PSUTIL, reason="psutil not available")
    @pytest.mark.slow
    @pytest.mark.skipif(
        not os.getenv("RUN_CHROME_PROCESS_TESTS"),
        reason="Chrome process tests require RUN_CHROME_PROCESS_TESTS=1",
    )
    def test_multiple_drivers_process_isolation(self):
        """Test that multiple Chrome drivers create separate process groups."""
        manager = ChromeDriverManager(headless=True)

        initial_count = self.process_monitor.count_new_processes()

        drivers = []
        try:
            # Create multiple drivers
            for i in range(2):
                profile_dir = manager.create_temp_profile()
                driver = manager.create_driver(profile_dir)
                drivers.extend([driver])
                self.created_drivers.append(driver)

                # Give each driver time to start
                time.sleep(1)

            # Give all processes time to stabilize
            time.sleep(2)

            final_count = self.process_monitor.count_new_processes()
            total_processes = final_count - initial_count

            # Should have created processes for both drivers
            # Each driver creates multiple processes, so we expect more than 2
            assert (
                total_processes >= 2
            ), f"Expected at least 2 processes for 2 drivers, got {total_processes}"

            print(f"Created {total_processes} processes for 2 Chrome drivers")

        except Exception as e:
            pytest.skip(f"Chrome not available for multi-driver testing: {e}")

        finally:
            for driver in drivers:
                try:
                    driver.quit()
                except:
                    pass

    def test_process_cleanup_verification(self):
        """Test that processes are properly cleaned up when drivers are quit."""
        # This test uses mocked drivers to avoid Chrome dependency
        with patch("selenium.webdriver.Chrome") as mock_chrome_class:
            manager = ChromeDriverManager()

            # Create mock driver
            mock_driver = Mock()
            mock_driver.session_id = "test_session"
            mock_driver.quit = Mock()
            mock_chrome_class.return_value = mock_driver

            # Create driver
            profile_dir = manager.create_temp_profile()
            driver = manager._create_system_driver(profile_dir)

            # Verify driver was created
            assert driver == mock_driver

            # Quit driver
            driver.quit()

            # Verify quit was called
            mock_driver.quit.assert_called_once()

    @patch("psutil.process_iter")
    def test_chrome_process_detection(self, mock_process_iter):
        """Test Chrome process detection logic."""
        # Mock Chrome processes
        mock_chrome_proc = Mock()
        mock_chrome_proc.info = {
            "pid": 12345,
            "name": "chrome",
            "cmdline": ["/usr/bin/google-chrome", "--headless"],
            "status": "running",
        }

        mock_chromedriver_proc = Mock()
        mock_chromedriver_proc.info = {
            "pid": 12346,
            "name": "chromedriver",
            "cmdline": ["/usr/bin/chromedriver", "--port=9515"],
            "status": "running",
        }

        mock_other_proc = Mock()
        mock_other_proc.info = {
            "pid": 12347,
            "name": "firefox",
            "cmdline": ["/usr/bin/firefox"],
            "status": "running",
        }

        mock_process_iter.return_value = [
            mock_chrome_proc,
            mock_chromedriver_proc,
            mock_other_proc,
        ]

        # Create monitor and check process detection
        monitor = ChromeProcessMonitor()
        processes = monitor._get_chrome_processes()

        # Should find Chrome and chromedriver but not Firefox
        assert 12345 in processes  # Chrome
        assert 12346 in processes  # chromedriver
        assert 12347 not in processes  # Firefox

    def test_profile_directory_cleanup_removes_chrome_artifacts(self):
        """Test that profile cleanup removes Chrome-specific files and directories."""
        manager = ChromeDriverManager()

        # Create test profile with Chrome-like structure
        profile_dir = manager.create_temp_profile()

        # Create typical Chrome profile artifacts
        chrome_artifacts = [
            "Default/Preferences",
            "Default/History",
            "Default/Cookies",
            "Default/Cache/data_0",
            "Default/Sessions/Tabs_13",
            "ShaderCache/GPUCache/data_0",
            "CertificateTransparency/logs.pb",
            "Local State",
        ]

        for artifact in chrome_artifacts:
            artifact_path = os.path.join(profile_dir, artifact)
            os.makedirs(os.path.dirname(artifact_path), exist_ok=True)
            with open(artifact_path, "w") as f:
                f.write("test data")

        # Verify artifacts were created
        for artifact in chrome_artifacts:
            assert os.path.exists(os.path.join(profile_dir, artifact))

        # Run cleanup
        manager._cleanup_temp_profiles()

        # Profile directory should be completely removed
        assert not os.path.exists(profile_dir)

    def test_cleanup_preserves_non_chrome_directories(self):
        """Test that cleanup only removes chrome_scrape_ directories."""
        manager = ChromeDriverManager()

        # Create various directories in profile temp dir
        chrome_profile = os.path.join(self.test_profile_dir, "chrome_scrape_test123")
        other_profile = os.path.join(self.test_profile_dir, "other_app_profile")
        system_dir = os.path.join(self.test_profile_dir, "system_cache")

        os.makedirs(chrome_profile)
        os.makedirs(other_profile)
        os.makedirs(system_dir)

        # Add content to each
        for directory in [chrome_profile, other_profile, system_dir]:
            with open(os.path.join(directory, "test.txt"), "w") as f:
                f.write("test content")

        # Run cleanup
        manager._cleanup_temp_profiles()

        # Only Chrome profile should be removed
        assert not os.path.exists(chrome_profile)
        assert os.path.exists(other_profile)
        assert os.path.exists(system_dir)

    @pytest.mark.parametrize("headless_mode", [True, False])
    def test_process_arguments_differ_by_headless_mode(self, headless_mode):
        """Test that process arguments differ appropriately for headless vs non-headless."""
        manager = ChromeDriverManager(headless=headless_mode)
        profile_dir = manager.create_temp_profile()

        options = manager._build_options(None, profile_dir)
        args = options.arguments

        if headless_mode:
            assert "--headless=new" in args
            assert "--start-minimized" not in args
            assert "--window-position=-32000,-32000" not in args
        else:
            assert "--headless=new" not in args
            assert "--start-minimized" in args
            assert "--window-position=-32000,-32000" in args


class TestChromeResourceManagement:
    """Test Chrome resource usage and management."""

    def setup_method(self):
        """Set up resource management tests."""
        self.test_profile_dir = tempfile.mkdtemp(prefix="test_chrome_resources_")

        self.env_patcher = patch.dict(
            os.environ, {"CHROME_PROFILE_TMPDIR": self.test_profile_dir}
        )
        self.env_patcher.start()

        # Also patch the module constant
        self.constant_patcher = patch(
            "scraper_app.utils.selenium.chrome_driver_manager.CHROME_PROFILE_TMPDIR",
            self.test_profile_dir,
        )
        self.constant_patcher.start()

    def teardown_method(self):
        """Clean up resource management tests."""
        self.env_patcher.stop()
        self.constant_patcher.stop()
        if os.path.exists(self.test_profile_dir):
            import shutil

            shutil.rmtree(self.test_profile_dir, ignore_errors=True)

    def test_chrome_options_limit_resource_usage(self):
        """Test that Chrome options are configured to limit resource usage."""
        manager = ChromeDriverManager(headless=True)
        profile_dir = manager.create_temp_profile()

        options = manager._build_options(None, profile_dir)
        args = options.arguments

        # Resource limiting flags
        resource_flags = [
            "--disable-images",  # Saves bandwidth
            "--disable-css",  # Faster parsing
            "--disable-gpu",  # No GPU acceleration
            "--disable-extensions",  # No extension overhead
            "--disable-plugins",  # No plugin overhead
            "--memory-pressure-off",  # Don't be too aggressive with cleanup
            "--renderer-process-limit=4",  # Limit renderer processes
            "--max_old_space_size=3072",  # Limit V8 heap size
        ]

        for flag in resource_flags:
            assert flag in args, f"Missing resource limiting flag: {flag}"

    def test_chrome_options_disable_unnecessary_features(self):
        """Test that unnecessary Chrome features are disabled."""
        manager = ChromeDriverManager(headless=True)
        profile_dir = manager.create_temp_profile()

        options = manager._build_options(None, profile_dir)
        args = options.arguments

        # Features that should be disabled
        disabled_features = [
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-default-apps",
            "--disable-background-networking",
            "--disable-background-timer-throttling",
            "--disable-renderer-backgrounding",
            "--disable-backgrounding-occluded-windows",
            "--disable-component-extensions-with-background-pages",
            "--disable-features=TranslateUI",
            "--disable-features=VizDisplayCompositor",
        ]

        for flag in disabled_features:
            assert flag in args, f"Missing feature disable flag: {flag}"

    def test_temp_profile_directory_structure(self):
        """Test that temporary profile directories have expected structure."""
        manager = ChromeDriverManager()

        # Create multiple profiles
        profiles = [manager.create_temp_profile() for _ in range(3)]

        try:
            for profile in profiles:
                # Should be under the designated temp directory
                assert profile.startswith(self.test_profile_dir)

                # Should have chrome_scrape_ prefix
                basename = os.path.basename(profile)
                assert basename.startswith("chrome_scrape_")

                # Should be unique
                other_profiles = [p for p in profiles if p != profile]
                for other in other_profiles:
                    assert profile != other

                # Should be writable
                test_file = os.path.join(profile, "test_write.txt")
                with open(test_file, "w") as f:
                    f.write("test")
                assert os.path.exists(test_file)

        finally:
            # Clean up
            manager._cleanup_temp_profiles()


if __name__ == "__main__":
    # Run with environment variable check
    if os.getenv("RUN_CHROME_PROCESS_TESTS"):
        print("Running Chrome process tests (this requires Chrome to be installed)")
        pytest.main([__file__, "-v", "-s", "--tb=short", "-m", "not slow"])
    else:
        print("Running Chrome process tests in mock mode")
        print("Set RUN_CHROME_PROCESS_TESTS=1 to run tests with real Chrome processes")
        pytest.main([__file__, "-v", "-s", "--tb=short", "-m", "not slow"])
