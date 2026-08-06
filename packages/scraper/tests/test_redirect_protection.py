#!/usr/bin/env python3
"""
Test script to verify redirect-based social media blocking works correctly.
"""

import sys
import os
from unittest.mock import Mock, patch

# Add the src directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from scraper.services.url_scraper_service import ScraperService
from scraper.utils.social_media_blocker import social_media_blocker


def test_redirect_protection():
    """Test that redirects to social media are properly blocked."""
    print("🧪 TESTING REDIRECT PROTECTION")
    print("=" * 50)

    # We'll create a mock scenario where a legitimate URL redirects to social media
    scraper = ScraperService(max_concurrent_browsers=1, max_depth=1, headless=True)

    # Test the _extract_text_with_fallback method directly with a mock
    with patch(
        "scraper.services.url_scraper_service.webdriver"
    ) as mock_webdriver:
        # Create a mock driver
        mock_driver = Mock()

        # Simulate a redirect scenario
        # Original URL: https://company.com/social
        # Redirects to: https://facebook.com/company
        mock_driver.current_url = "https://facebook.com/company"

        try:
            # This should raise a ValueError due to social media blocking
            result = scraper._extract_text_with_fallback(
                mock_driver, "https://company.com/social"
            )
            print("❌ FAILED: Should have blocked redirect to Facebook")
        except ValueError as e:
            if "redirected to blocked social media site" in str(e):
                print("✅ SUCCESS: Blocked redirect to Facebook")
                print(f"   Error message: {e}")
            else:
                print(f"❌ FAILED: Wrong error type: {e}")
        except Exception as e:
            print(f"❌ FAILED: Unexpected error: {e}")

    # Test with legitimate redirect (should work)
    with patch(
        "scraper.services.url_scraper_service.webdriver"
    ) as mock_webdriver:
        mock_driver = Mock()

        # Simulate legitimate redirect
        # Original URL: https://company.com/about
        # Redirects to: https://company.com/about-us
        mock_driver.current_url = "https://company.com/about-us"
        mock_driver.find_element.return_value.text = "Company information"

        try:
            result = scraper._extract_text_with_fallback(
                mock_driver, "https://company.com/about"
            )
            print("✅ SUCCESS: Allowed legitimate redirect")
        except Exception as e:
            print(f"❌ FAILED: Blocked legitimate redirect: {e}")

    print("\n" + "=" * 50)
    print("REDIRECT PROTECTION TEST COMPLETE")


def test_redirect_scenarios():
    """Test various redirect scenarios that could happen in the real world."""
    print("\n🎯 TESTING REAL-WORLD REDIRECT SCENARIOS")
    print("=" * 50)

    # Scenarios to test
    redirect_scenarios = [
        # (original_url, final_url, should_block, description)
        (
            "https://company.com/social",
            "https://facebook.com/company",
            True,
            "Company social link → Facebook",
        ),
        (
            "https://business.org/twitter",
            "https://twitter.com/business",
            True,
            "Business Twitter link → Twitter",
        ),
        (
            "https://manufacturer.com/follow",
            "https://instagram.com/manufacturer",
            True,
            "Follow link → Instagram",
        ),
        (
            "https://company.com/redirect",
            "https://company.com/about",
            False,
            "Internal redirect (legitimate)",
        ),
        (
            "https://business.org/contact",
            "https://business.org/contact-us",
            False,
            "Internal redirect (legitimate)",
        ),
        (
            "https://company.com/news",
            "https://news.company.com/latest",
            False,
            "Subdomain redirect (legitimate)",
        ),
    ]

    for original_url, final_url, should_block, description in redirect_scenarios:
        is_blocked = social_media_blocker.is_social_media_url(final_url)

        if should_block:
            if is_blocked:
                print(f"✅ {description}: CORRECTLY BLOCKED")
            else:
                print(f"❌ {description}: SHOULD BE BLOCKED")
        else:
            if not is_blocked:
                print(f"✅ {description}: CORRECTLY ALLOWED")
            else:
                print(f"❌ {description}: SHOULD BE ALLOWED")

    print("\n" + "=" * 50)
    print("SCENARIO TESTING COMPLETE")


if __name__ == "__main__":
    test_redirect_protection()
    test_redirect_scenarios()
    print("\n🎉 ALL REDIRECT PROTECTION TESTS COMPLETED!")
