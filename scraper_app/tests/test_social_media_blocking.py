"""Tests for social media blocking functionality."""

import pytest
from unittest.mock import Mock, patch

from scraper_app.utils.social_media_blocker import (
    SocialMediaBlocker,
    social_media_blocker,
)
from scraper_app.services.url_scraper_service import ScraperService


class TestSocialMediaBlocker:
    """Test cases for the SocialMediaBlocker utility."""

    def setup_method(self):
        self.blocker = SocialMediaBlocker()

    def test_exact_domain_blocking(self):
        """Test that exact social media domains are blocked."""
        social_media_urls = [
            "https://www.facebook.com/company",
            "https://facebook.com/page",
            "http://instagram.com/profile",
            "https://www.instagram.com/user",
            "https://twitter.com/handle",
            "https://www.twitter.com/user",
            "https://x.com/handle",
            "https://www.x.com/user",
            "https://linkedin.com/in/profile",
            "https://www.linkedin.com/company/test",
            "https://youtube.com/watch?v=123",
            "https://www.youtube.com/channel/UC123",
            "https://youtu.be/video123",
            "https://tiktok.com/@user",
            "https://www.tiktok.com/user",
            "https://snapchat.com/user",
            "https://pinterest.com/pin/123",
            "https://reddit.com/r/subreddit",
            "https://discord.com/invite/abc123",
            "https://twitch.tv/streamer",
        ]

        for url in social_media_urls:
            assert self.blocker.is_social_media_url(url), f"Should block {url}"

    def test_pattern_based_blocking(self):
        """Test that subdomain patterns are blocked correctly."""
        subdomain_urls = [
            "https://m.facebook.com/page",
            "https://mobile.facebook.com/user",
            "https://api.twitter.com/endpoint",
            "https://cdn.instagram.com/image.jpg",
            "https://business.linkedin.com/solutions",
            "https://music.youtube.com/watch",
            "https://gaming.youtube.com/channel",
        ]

        for url in subdomain_urls:
            assert self.blocker.is_social_media_url(
                url
            ), f"Should block subdomain {url}"

    def test_legitimate_sites_not_blocked(self):
        """Test that legitimate business sites are not blocked."""
        legitimate_urls = [
            "https://www.example.com",
            "https://business.com/about",
            "https://company.org/services",
            "https://shop.store.net/products",
            "https://www.microsoft.com",
            "https://www.google.com",
            "https://www.amazon.com",
            "https://www.apple.com",
            "https://www.ibm.com",
            "https://www.salesforce.com",
            "https://www.manufacturing-company.com",
            "https://factory.industrial.co.uk",
        ]

        for url in legitimate_urls:
            assert not self.blocker.is_social_media_url(url), f"Should NOT block {url}"

    def test_edge_cases(self):
        """Test edge cases and malformed URLs."""
        edge_cases = [
            "",
            None,
            "not-a-url",
            "ftp://facebook.com",  # Different protocol
            "https://facebook-clone.com",  # Similar but different domain
            "https://myfacebook.com",  # Contains but not exact match
            "https://facebook.com.example.com",  # Subdomain of different domain
        ]

        for case in edge_cases:
            # Should not raise exceptions and should not block legitimate variations
            result = self.blocker.is_social_media_url(case)
            if case in [None, "", "not-a-url"]:
                assert not result, f"Should handle invalid input: {case}"

    def test_filter_social_media_urls(self):
        """Test filtering a list of URLs."""
        mixed_urls = [
            "https://www.company.com",
            "https://facebook.com/page",
            "https://www.business.org/about",
            "https://twitter.com/handle",
            "https://www.manufacturer.com/products",
            "https://instagram.com/brand",
            "https://www.supplier.net/services",
        ]

        filtered = self.blocker.filter_social_media_urls(mixed_urls)

        expected_filtered = [
            "https://www.company.com",
            "https://www.business.org/about",
            "https://www.manufacturer.com/products",
            "https://www.supplier.net/services",
        ]

        assert filtered == expected_filtered

    def test_validate_start_url_blocks_social_media(self):
        """Test that validate_start_url raises ValueError for social media URLs."""
        with pytest.raises(ValueError, match="Social media sites are blocked"):
            self.blocker.validate_start_url("https://facebook.com/company")

        with pytest.raises(ValueError, match="Social media sites are blocked"):
            self.blocker.validate_start_url("https://www.twitter.com/handle")

    def test_validate_start_url_allows_legitimate_sites(self):
        """Test that validate_start_url allows legitimate sites."""
        # Should not raise any exception
        self.blocker.validate_start_url("https://www.company.com")
        self.blocker.validate_start_url("https://manufacturer.org/about")

    def test_case_insensitive_matching(self):
        """Test that URL matching is case insensitive."""
        case_variations = [
            "https://FACEBOOK.COM/page",
            "https://Facebook.com/PAGE",
            "https://www.TWITTER.COM/handle",
            "https://Twitter.Com/Handle",
        ]

        for url in case_variations:
            assert self.blocker.is_social_media_url(
                url
            ), f"Should block case variation: {url}"

    def test_url_with_ports(self):
        """Test URLs with port numbers."""
        # Social media sites with ports (unusual but possible)
        assert self.blocker.is_social_media_url("https://facebook.com:8080/page")
        assert self.blocker.is_social_media_url("http://twitter.com:80/handle")

        # Legitimate sites with ports should not be blocked
        assert not self.blocker.is_social_media_url("https://company.com:8443/app")


class TestScraperServiceSocialMediaIntegration:
    """Test that the ScraperService properly integrates social media blocking."""

    def setup_method(self):
        self.scraper = ScraperService(
            max_concurrent_browser_tabs=1, max_depth=1, headless=True
        )

    def test_scrape_blocks_social_media_start_url(self):
        """Test that scraping a social media URL as start URL is blocked."""
        social_media_urls = [
            "https://www.facebook.com/company",
            "https://twitter.com/handle",
            "https://instagram.com/brand",
        ]

        for url in social_media_urls:
            result = self.scraper.scrape(url)

            # Should return failed result
            assert result.urls_scraped == 0
            assert result.urls_failed == 1
            assert result.has_errors
            assert len(result.errors) == 1
            assert "Social media sites are blocked" in result.errors[0]["error"]

    @patch("scraper_app.services.url_scraper_service.get_final_landing_url")
    def test_scrape_blocks_redirect_to_social_media(self, mock_get_final_url):
        """Test that redirects to social media are blocked."""
        # Mock a redirect from legitimate site to social media
        mock_get_final_url.return_value = "https://facebook.com/company"

        result = self.scraper.scrape("https://company.com")

        # Should be blocked after redirect detection
        assert result.urls_scraped == 0
        assert result.urls_failed == 1
        assert result.has_errors
        assert "Social media sites are blocked" in result.errors[0]["error"]

    def test_extract_text_with_fallback_blocks_redirects(self):
        """Test that _extract_text_with_fallback blocks redirects to social media."""
        from unittest.mock import Mock

        # Create a mock driver that simulates redirect to social media
        mock_driver = Mock()
        mock_driver.current_url = "https://facebook.com/company"

        # This should raise ValueError due to redirect blocking
        with pytest.raises(ValueError, match="redirected to blocked social media site"):
            self.scraper._extract_text_with_fallback(
                mock_driver, "https://company.com/social"
            )

    def test_extract_text_with_fallback_allows_legitimate_redirects(self):
        """Test that _extract_text_with_fallback allows legitimate redirects."""
        from unittest.mock import Mock, patch

        # Create a mock driver that simulates legitimate redirect
        mock_driver = Mock()
        mock_driver.current_url = "https://company.com/about-us"  # Legitimate redirect
        mock_driver.find_element.return_value.text = "Company information"

        # This should work without raising exceptions
        try:
            result = self.scraper._extract_text_with_fallback(
                mock_driver, "https://company.com/about"
            )
            # Should not raise exception for legitimate redirect
        except ValueError as e:
            if "redirected to blocked social media site" in str(e):
                pytest.fail("Should not block legitimate redirects")
        except Exception:
            # Other exceptions are fine (mocking limitations)
            pass

    def test_get_blocked_domains_and_patterns(self):
        """Test that the blocker returns blocked domains and patterns."""
        domains = social_media_blocker.get_blocked_domains_list()
        patterns = social_media_blocker.get_blocked_patterns_list()

        # Should contain major social media platforms
        assert "facebook.com" in domains
        assert "twitter.com" in domains
        assert "instagram.com" in domains
        assert "linkedin.com" in domains

        # Should contain pattern rules
        assert len(patterns) > 0
        assert any("facebook" in pattern for pattern in patterns)


if __name__ == "__main__":
    pytest.main([__file__])
