"""
Driver factory interface and implementations for web scraping.
Provides abstraction for driver creation and lifecycle management.
"""

import shutil
from abc import ABC, abstractmethod
from typing import Optional
from selenium import webdriver
from .chrome_driver_manager import ChromeDriverManager


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
            driver.quit()
        except Exception:
            pass
        
        # Clean up temporary profile directory
        profile_dir = getattr(driver, "_profile_dir", None)
        if profile_dir:
            shutil.rmtree(profile_dir, ignore_errors=True)


class LegacyDriverFactory(DriverFactory):
    """Legacy factory that uses custom driver creation functions."""
    
    def __init__(self, create_driver_func, prepare_profile_func):
        self.create_driver_func = create_driver_func
        self.prepare_profile_func = prepare_profile_func
    
    def create_driver(self) -> webdriver.Chrome:
        """Create driver using legacy functions."""
        profile_dir = self.prepare_profile_func()
        driver = self.create_driver_func(profile_dir)
        setattr(driver, "_profile_dir", profile_dir)
        return driver
    
    def cleanup_driver(self, driver: webdriver.Chrome) -> None:
        """Clean up legacy driver."""
        try:
            driver.quit()
        except Exception:
            pass
        
        profile_dir = getattr(driver, "_profile_dir", None)
        if profile_dir:
            shutil.rmtree(profile_dir, ignore_errors=True)
