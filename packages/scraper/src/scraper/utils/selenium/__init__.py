"""
Selenium utilities for web scraping.
Provides driver management and factory patterns.
"""

from packages.scraper.src.scraper.utils.selenium.chrome_driver_manager import (
    ChromeDriverManager,
)
from packages.scraper.src.scraper.utils.selenium.driver_factory import (
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
