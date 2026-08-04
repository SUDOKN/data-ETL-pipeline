"""Utility functions for blocking social media sites from scraping."""

import re
import logging
from urllib.parse import urlparse
from typing import List, Set

from scraper_app.constants.scraping_constants import (
    BLOCKED_SOCIAL_MEDIA_DOMAINS,
    BLOCKED_SOCIAL_MEDIA_PATTERNS,
)

logger = logging.getLogger(__name__)


class SocialMediaBlocker:
    """
    A utility class to detect and block social media URLs from being scraped.
    Provides comprehensive checking using both exact domain matching and regex patterns.
    """

    def __init__(self):
        # Compile regex patterns for better performance
        self._compiled_patterns = [
            re.compile(pattern, re.IGNORECASE)
            for pattern in BLOCKED_SOCIAL_MEDIA_PATTERNS
        ]

    def is_social_media_url(self, url: str) -> bool:
        """
        Check if a URL belongs to a social media platform.

        Args:
            url: The URL to check

        Returns:
            bool: True if the URL is from a social media platform, False otherwise
        """
        if not url or not isinstance(url, str):
            return False

        try:
            parsed_url = urlparse(url.lower())
            domain = parsed_url.netloc

            # Remove port if present
            if ":" in domain:
                domain = domain.split(":")[0]

            # Remove leading 'www.' for consistent checking
            domain_without_www = domain.replace("www.", "")

            # Check exact domain matches (both with and without www)
            if (
                domain in BLOCKED_SOCIAL_MEDIA_DOMAINS
                or domain_without_www in BLOCKED_SOCIAL_MEDIA_DOMAINS
            ):
                logger.info(f"Blocked social media URL (exact domain match): {url}")
                return True

            # Check pattern matches
            for pattern in self._compiled_patterns:
                if pattern.match(domain):
                    logger.info(f"Blocked social media URL (pattern match): {url}")
                    return True

            return False

        except Exception as e:
            logger.warning(f"Error parsing URL for social media check: {url} - {e}")
            # In case of parsing errors, be conservative and don't block
            return False

    def filter_social_media_urls(self, urls: List[str]) -> List[str]:
        """
        Filter out social media URLs from a list of URLs.

        Args:
            urls: List of URLs to filter

        Returns:
            List[str]: URLs with social media sites removed
        """
        if not urls:
            return []

        filtered_urls = []
        blocked_count = 0

        for url in urls:
            if self.is_social_media_url(url):
                blocked_count += 1
            else:
                filtered_urls.append(url)

        if blocked_count > 0:
            logger.info(
                f"Blocked {blocked_count} social media URLs out of {len(urls)} total URLs"
            )

        return filtered_urls

    def validate_start_url(self, url: str) -> None:
        """
        Validate that a start URL is not from a social media platform.

        Args:
            url: The start URL to validate

        Raises:
            ValueError: If the URL is from a social media platform
        """
        if self.is_social_media_url(url):
            domain = urlparse(url).netloc
            raise ValueError(
                f"Social media sites are blocked from scraping. "
                f"Cannot scrape URL from domain: {domain}"
            )

    def get_blocked_domains_list(self) -> Set[str]:
        """
        Get the complete list of blocked social media domains.

        Returns:
            Set[str]: Set of blocked domains
        """
        return BLOCKED_SOCIAL_MEDIA_DOMAINS.copy()

    def get_blocked_patterns_list(self) -> List[str]:
        """
        Get the complete list of blocked social media URL patterns.

        Returns:
            List[str]: List of blocked patterns
        """
        return BLOCKED_SOCIAL_MEDIA_PATTERNS.copy()


# Global instance for easy importing
social_media_blocker = SocialMediaBlocker()
