"""
Driver factory interface and implementations for web scraping.
Provides abstraction for driver creation and lifecycle management.
"""

from abc import ABC, abstractmethod
import logging
import importlib
import os
import shutil
import sys
from selenium import webdriver

from scraper_app.utils.selenium.chrome_driver_manager import ChromeDriverManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class DriverFactory(ABC):
    """Abstract factory for creating web drivers."""

    @abstractmethod
    def create_driver(self) -> webdriver.Chrome:
        """Create a new driver instance."""
        pass

    @abstractmethod
    def cleanup_driver(self, driver: webdriver.Chrome) -> None:
        """Clean up driver and associated resources."""
        pass


class ChromeDriverFactory(DriverFactory):
    """Chrome driver factory with profile management."""

    def __init__(self, headless: bool = True, channel: str = "Stable"):
        self.manager = ChromeDriverManager(headless, channel)

    def create_driver(self) -> webdriver.Chrome:
        """Create a Chrome driver with temporary profile."""
        profile_dir = self.manager.create_temp_profile()
        driver = self.manager.create_driver(profile_dir)
        # Store profile dir on driver for cleanup
        setattr(driver, "_profile_dir", profile_dir)
        return driver

    def cleanup_driver(self, driver: webdriver.Chrome) -> None:
        """Clean up driver and remove temporary profile."""
        try:
            logger.info(
                f"Cleaning up driver: {getattr(driver, 'session_id', 'unknown')}"
            )
            driver.quit()
        except Exception:
            logger.warning(
                f"Failed to quit driver {getattr(driver, 'session_id', 'unknown')}",
                exc_info=True,
            )

        # Clean up temporary profile directory
        profile_dir = getattr(driver, "_profile_dir", None)
        logger.info(
            f"Removing profile directory: {profile_dir} for driver {getattr(driver, 'session_id', 'unknown')}"
        )
        if profile_dir:
            shutil.rmtree(profile_dir, ignore_errors=True)
            # check if the directory was removed
            if not os.path.exists(profile_dir):
                logger.info(
                    f"Successfully removed profile directory: {profile_dir} for driver {getattr(driver, 'session_id', 'unknown')}"
                )
            else:
                logger.error(
                    f"Failed to remove profile directory: {profile_dir} for driver {getattr(driver, 'session_id', 'unknown')}"
                )

    def cleanup_all_temp_profiles(self):
        """Clean up all temporary Chrome profiles."""
        self.manager._cleanup_temp_profiles()


class LegacyDriverFactory(DriverFactory):
    """Legacy factory that uses custom driver creation functions."""

    def __init__(self, module_name, headless: bool = True):
        mod = importlib.import_module(module_name)
        create_driver = getattr(mod, "create_driver", None)
        prepare_profile = getattr(mod, "prepare_temp_profile", None)

        if callable(create_driver) and callable(prepare_profile):
            logger.info(f"Using legacy driver factory from module: {module_name}")
            self.create_driver_func = create_driver
            self.prepare_profile_func = prepare_profile
        else:
            logger.warning(
                f"Module {module_name} missing required functions, using default factory"
            )

    def create_driver(self) -> webdriver.Chrome:
        """Create driver using legacy functions."""
        profile_dir = self.prepare_profile_func()
        driver = self.create_driver_func(profile_dir)
        setattr(driver, "_profile_dir", profile_dir)
        return driver

    def cleanup_driver(self, driver: webdriver.Chrome) -> None:
        """Clean up legacy driver."""
        try:
            logger.info(
                f"Cleaning up legacy driver: {getattr(driver, 'session_id', 'unknown')}"
            )
            driver.quit()
        except Exception:
            pass

        profile_dir = getattr(driver, "_profile_dir", None)
        logger.info(
            f"Removing profile directory: {profile_dir} for legacy driver {getattr(driver, 'session_id', 'unknown')}"
        )
        if profile_dir:
            shutil.rmtree(profile_dir, ignore_errors=True)
            # check if the directory was removed
            if not os.path.exists(profile_dir):
                logger.info(
                    f"Successfully removed profile directory: {profile_dir} for legacy driver {getattr(driver, 'session_id', 'unknown')}"
                )
            else:
                logger.error(
                    f"Failed to remove profile directory: {profile_dir} for legacy driver {getattr(driver, 'session_id', 'unknown')}"
                )
