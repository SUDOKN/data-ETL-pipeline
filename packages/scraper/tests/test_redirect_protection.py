"""
Tests for redirect-based social media blocking in ScraperService.
"""

import pytest
from unittest.mock import Mock

from scraper.services.url_scraper_service import ScraperService
from scraper.utils.social_media_blocker import social_media_blocker


@pytest.fixture
def scraper():
    return ScraperService(max_concurrent_browsers=1, max_depth=1, headless=True)


def _mock_driver(current_url: str) -> Mock:
    driver = Mock()
    driver.current_url = current_url
    driver.find_element.return_value.text = "Company information"
    driver.execute_script.return_value = "Company information"
    return driver


def test_redirect_to_social_media_is_blocked(scraper):
    """A legitimate-looking URL that redirects to Facebook must be rejected."""
    driver = _mock_driver("https://facebook.com/company")

    with pytest.raises(ValueError, match="redirected to blocked social media site"):
        scraper._extract_text_with_fallback(driver, "https://company.com/social")


def test_legitimate_redirect_is_allowed(scraper):
    """An internal redirect must still return extracted text."""
    driver = _mock_driver("https://company.com/about-us")

    result = scraper._extract_text_with_fallback(driver, "https://company.com/about")

    assert result == "Company information"


def test_empty_url_rejected(scraper):
    with pytest.raises(ValueError, match="URL cannot be empty"):
        scraper._extract_text_with_fallback(_mock_driver("https://company.com"), "")


@pytest.mark.parametrize(
    "final_url,should_block,description",
    [
        ("https://facebook.com/company", True, "Company social link -> Facebook"),
        ("https://twitter.com/business", True, "Business Twitter link -> Twitter"),
        ("https://instagram.com/manufacturer", True, "Follow link -> Instagram"),
        ("https://company.com/about", False, "Internal redirect"),
        ("https://business.org/contact-us", False, "Internal redirect"),
        ("https://news.company.com/latest", False, "Subdomain redirect"),
    ],
)
def test_redirect_scenarios(final_url, should_block, description):
    """Real-world redirect destinations are classified correctly."""
    assert (
        social_media_blocker.is_social_media_url(final_url) is should_block
    ), description
