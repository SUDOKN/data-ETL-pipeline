"""
Selenium utilities for web scraping.
Provides driver management and factory patterns.
"""

from scraper_app.utils.selenium.chrome_driver_manager import ChromeDriverManager
from scraper_app.utils.selenium.driver_factory import (
    DriverFactory,
    ChromeDriverFactory,
    LegacyDriverFactory,
)

__all__ = [
    "ChromeDriverManager",
    "DriverFactory",
    "ChromeDriverFactory",
    "LegacyDriverFactory",
]
