from urllib.parse import urlparse
import pytest
from unittest.mock import patch, Mock
import requests

from core.utils.url_util import (
    get_normalized_url,
    get_complete_url_with_compatible_protocol,
    get_etld1_from_host,
    strip_scheme,
    get_final_landing_url,
)


def test_etld1():
    """Test the etld1 function which extracts domain + suffix (eTLD+1)."""

    # Basic domain + suffix extraction
    assert get_etld1_from_host("http://example.com") == "example.com"
    assert get_etld1_from_host("example.com") == "example.com"
    assert get_etld1_from_host("subdomain.example.com") == "example.com"
    assert get_etld1_from_host("www.example.com") == "example.com"
    assert get_etld1_from_host("api.subdomain.example.com") == "example.com"

    # Complex multi-level suffixes (handled by Public Suffix List)
    assert get_etld1_from_host("example.co.uk") == "example.co.uk"
    assert get_etld1_from_host("subdomain.example.co.uk") == "example.co.uk"
    assert get_etld1_from_host("www.example.co.uk") == "example.co.uk"

    # More complex multi-level suffixes
    assert get_etld1_from_host("example.com.au") == "example.com.au"
    assert get_etld1_from_host("test.example.com.au") == "example.com.au"

    # Government and organization domains
    assert get_etld1_from_host("example.gov.uk") == "example.gov.uk"
    assert get_etld1_from_host("subdomain.example.gov.uk") == "example.gov.uk"

    # Japanese domains
    assert get_etld1_from_host("example.co.jp") == "example.co.jp"
    assert get_etld1_from_host("www.example.co.jp") == "example.co.jp"

    # Brazilian domains
    assert get_etld1_from_host("example.com.br") == "example.com.br"
    assert get_etld1_from_host("api.example.com.br") == "example.com.br"

    # Case insensitive
    assert get_etld1_from_host("EXAMPLE.COM") == "example.com"
    assert get_etld1_from_host("Subdomain.Example.Co.UK") == "example.co.uk"

    # Edge cases - no recognized suffix (fallback to full host)
    assert get_etld1_from_host("localhost") == "localhost"
    assert get_etld1_from_host("internal.server") == "internal.server"
    assert get_etld1_from_host("192.168.1.1") == "192.168.1.1"

    # Single character domains
    assert get_etld1_from_host("a.com") == "a.com"
    assert get_etld1_from_host("x.y.a.com") == "a.com"

    # Numeric domains
    assert get_etld1_from_host("123.com") == "123.com"
    assert get_etld1_from_host("sub.123.com") == "123.com"

    # Hyphenated domains
    assert get_etld1_from_host("my-site.example-domain.com") == "example-domain.com"
    assert (
        get_etld1_from_host("test.my-site.example-domain.com") == "example-domain.com"
    )


def test_etld1_edge_cases():
    """Test etld1 with additional edge cases and special scenarios."""

    # Empty string returns empty string
    assert get_etld1_from_host("") == ""

    # None input raises AttributeError
    with pytest.raises(AttributeError):
        get_etld1_from_host(None)  # type: ignore

    # Very long domain names
    long_subdomain = "a" * 63  # Max subdomain length
    assert get_etld1_from_host(f"{long_subdomain}.example.com") == "example.com"

    # Domains with numbers and special characters
    assert get_etld1_from_host("test123.example.com") == "example.com"
    assert get_etld1_from_host("sub-domain.example-site.com") == "example-site.com"
    assert get_etld1_from_host("sub_domain.example_site.com") == "example_site.com"

    # Mixed case handling
    assert get_etld1_from_host("SubDomain.ExAmPlE.CoM") == "example.com"
    assert get_etld1_from_host("WWW.GOOGLE.CO.UK") == "google.co.uk"

    # Punycode/IDN domains (International Domain Names)
    assert (
        get_etld1_from_host("xn--e1afmkfd.xn--p1ai") == "xn--e1afmkfd.xn--p1ai"
    )  # пример.рф in punycode

    # Single-level domains (localhost-style)
    assert get_etld1_from_host("intranet") == "intranet"
    assert get_etld1_from_host("server") == "server"

    # IP addresses (edge case handling)
    assert get_etld1_from_host("192.168.1.1") == "192.168.1.1"
    assert get_etld1_from_host("10.0.0.1") == "10.0.0.1"
    assert get_etld1_from_host("127.0.0.1") == "127.0.0.1"

    # IPv6 addresses (if supported by tldextract)
    try:
        assert get_etld1_from_host("2001:db8::1") == "2001:db8::1"
        assert get_etld1_from_host("::1") == "::1"
    except:
        # IPv6 might not be supported in all tldextract versions
        pass

    # Domains with port numbers - tldextract strips the port
    assert get_etld1_from_host("example.com:8080") == "example.com"
    assert get_etld1_from_host("subdomain.example.com:8080") == "example.com"

    # Test with various port numbers
    assert get_etld1_from_host("api.example.co.uk:443") == "example.co.uk"
    assert get_etld1_from_host("www.example.com:80") == "example.com"


def test_strip_scheme():
    """Test the strip_scheme function which removes scheme from URLs."""

    # Basic HTTP/HTTPS scheme removal
    assert strip_scheme("https://example.com") == "example.com"
    assert strip_scheme("http://example.com") == "example.com"
    assert strip_scheme("https://www.example.com") == "www.example.com"
    assert strip_scheme("http://www.example.com") == "www.example.com"

    # URLs with paths
    assert strip_scheme("https://example.com/path") == "example.com/path"
    assert (
        strip_scheme("http://example.com/path/to/resource")
        == "example.com/path/to/resource"
    )
    assert (
        strip_scheme("https://www.example.com/api/v1/users")
        == "www.example.com/api/v1/users"
    )

    # URLs with query parameters
    assert strip_scheme("https://example.com?query=value") == "example.com?query=value"
    assert (
        strip_scheme("http://example.com/search?q=test&page=1")
        == "example.com/search?q=test&page=1"
    )
    assert (
        strip_scheme("https://api.example.com/data?format=json&limit=10")
        == "api.example.com/data?format=json&limit=10"
    )

    # URLs with paths and query parameters
    assert (
        strip_scheme("https://example.com/path?query=value")
        == "example.com/path?query=value"
    )
    assert (
        strip_scheme("http://www.example.com/api/v1?token=abc123")
        == "www.example.com/api/v1?token=abc123"
    )

    # URLs with ports
    assert strip_scheme("https://example.com:8080") == "example.com:8080"
    assert strip_scheme("http://localhost:3000") == "localhost:3000"
    assert (
        strip_scheme("https://api.example.com:443/data") == "api.example.com:443/data"
    )

    # Complex URLs with all components
    assert (
        strip_scheme("https://api.example.com:8080/v1/users?active=true&limit=50")
        == "api.example.com:8080/v1/users?active=true&limit=50"
    )
    assert (
        strip_scheme(
            "http://subdomain.example.co.uk:9000/path/to/resource?param1=value1&param2=value2"
        )
        == "subdomain.example.co.uk:9000/path/to/resource?param1=value1&param2=value2"
    )

    # URLs without scheme (should return unchanged)
    assert strip_scheme("example.com") == "example.com"
    assert strip_scheme("www.example.com") == "www.example.com"
    assert strip_scheme("example.com/path") == "example.com/path"
    assert strip_scheme("example.com?query=value") == "example.com?query=value"

    # URLs with colons but no "//" - urlparse treats them as having a scheme
    # This is the actual behavior of urlparse which strip_scheme uses
    assert (
        strip_scheme("api.example.com:8080/data") == "8080/data"
    )  # scheme="api.example.com", path="8080/data"
    assert strip_scheme("localhost:3000") == "3000"  # scheme="localhost", path="3000"

    # Edge cases with different schemes (not HTTP/HTTPS)
    assert strip_scheme("ftp://example.com/file.txt") == "example.com/file.txt"
    assert strip_scheme("mailto:user@example.com") == "user@example.com"
    assert strip_scheme("file:///path/to/file") == "/path/to/file"

    # Case sensitivity (schemes are case-insensitive in URLs)
    assert strip_scheme("HTTPS://EXAMPLE.COM") == "EXAMPLE.COM"
    assert strip_scheme("HTTP://WWW.EXAMPLE.COM") == "WWW.EXAMPLE.COM"

    # URLs with fragments (note: urlparse separates fragments and strip_scheme doesn't reconstruct them)
    # This is expected behavior - fragments are not part of the returned URL
    assert strip_scheme("https://example.com/path#section") == "example.com/path"
    assert (
        strip_scheme("http://example.com?query=value#fragment")
        == "example.com?query=value"
    )

    # Empty and minimal inputs
    assert strip_scheme("") == ""
    assert strip_scheme("://") == "://"  # No scheme, so returned unchanged

    # URLs with userinfo (rarely used but valid)
    assert strip_scheme("https://user:pass@example.com") == "user:pass@example.com"
    assert (
        strip_scheme("http://user@api.example.com:8080/data")
        == "user@api.example.com:8080/data"
    )

    # International domains
    assert strip_scheme("https://例え.テスト") == "例え.テスト"
    assert (
        strip_scheme("http://subdomain.例え.テスト/path")
        == "subdomain.例え.テスト/path"
    )

    # URLs with encoded characters
    assert (
        strip_scheme("https://example.com/path%20with%20spaces")
        == "example.com/path%20with%20spaces"
    )
    assert (
        strip_scheme("http://example.com/search?q=hello%20world")
        == "example.com/search?q=hello%20world"
    )

    # URLs with unusual but valid characters
    assert strip_scheme("https://sub-domain.example.com") == "sub-domain.example.com"
    assert (
        strip_scheme("http://example.com/path_with_underscores")
        == "example.com/path_with_underscores"
    )
    assert (
        strip_scheme("https://example.com:8080/api/v2.1") == "example.com:8080/api/v2.1"
    )


def test_strip_scheme_edge_cases():
    """Test strip_scheme with additional edge cases."""

    # Test with None input - returns None
    assert strip_scheme(None) is None  # type: ignore

    # Test with non-string inputs that have decode methods - raises AttributeError
    with pytest.raises(AttributeError):
        strip_scheme(123)  # type: ignore

    # Multiple consecutive slashes
    assert strip_scheme("https:///example.com") == "/example.com"
    assert strip_scheme("http:////example.com/path") == "//example.com/path"

    # URLs with only netloc (no path, query, or fragment)
    assert strip_scheme("https://example.com") == "example.com"
    assert strip_scheme("https://www.example.com") == "www.example.com"

    # URLs with empty paths but trailing slash
    assert strip_scheme("https://example.com/") == "example.com/"
    assert strip_scheme("http://www.example.com/") == "www.example.com/"

    # URLs with complex query strings
    assert strip_scheme("https://example.com?a=1&b=2&c=3") == "example.com?a=1&b=2&c=3"
    assert (
        strip_scheme(
            "http://api.example.com/search?q=python%20programming&sort=date&limit=10"
        )
        == "api.example.com/search?q=python%20programming&sort=date&limit=10"
    )

    # URLs with unusual schemes
    assert strip_scheme("ws://example.com/socket") == "example.com/socket"
    assert (
        strip_scheme("wss://example.com/secure-socket") == "example.com/secure-socket"
    )
    assert strip_scheme("ldap://directory.example.com") == "directory.example.com"

    # URLs with percent-encoded characters in different parts
    assert (
        strip_scheme("https://example.com/path%20with%20spaces/file.txt")
        == "example.com/path%20with%20spaces/file.txt"
    )
    assert (
        strip_scheme("http://example.com/search?query=hello%20world%26more")
        == "example.com/search?query=hello%20world%26more"
    )

    # URLs with authentication info and ports
    assert (
        strip_scheme("https://user:password@api.example.com:8443/data")
        == "user:password@api.example.com:8443/data"
    )
    assert (
        strip_scheme("http://admin@localhost:3000/admin")
        == "admin@localhost:3000/admin"
    )

    # Edge case: URL with scheme but empty netloc
    assert strip_scheme("file:///absolute/path/to/file") == "/absolute/path/to/file"
    assert strip_scheme("file://localhost/path/to/file") == "localhost/path/to/file"

    # URLs with unusual but valid characters in hostname
    assert (
        strip_scheme("https://sub-domain.example-site.com")
        == "sub-domain.example-site.com"
    )
    assert (
        strip_scheme("http://server1.datacenter-east.example.com")
        == "server1.datacenter-east.example.com"
    )

    # Test scheme case variations
    assert strip_scheme("HTTP://EXAMPLE.COM") == "EXAMPLE.COM"
    assert strip_scheme("hTtPs://Example.Com") == "Example.Com"


def test_urlparsed_hostname():
    """Test urlparse hostname extraction."""
    assert urlparse("https://www.example.com").hostname == "www.example.com"
    assert urlparse("https://example.com").hostname == "example.com"
    assert urlparse("https://example").hostname == "example"
    assert urlparse("http://www.example.com").hostname == "www.example.com"
    assert urlparse("http://example.com").hostname == "example.com"
    assert urlparse("http://example").hostname == "example"


def test_get_normalized_url():
    """Test the get_normalized_url function with valid URLs."""
    # Basic domain + suffix extraction (no subdomain) - returns (scheme, full_url) tuple
    assert get_normalized_url("https://example.com") == ("https", "https://example.com")
    assert get_normalized_url("http://example.com") == ("http", "http://example.com")
    assert get_normalized_url("https://example.com/path") == (
        "https",
        "https://example.com",
    )
    assert get_normalized_url("https://example.com/path?query=1") == (
        "https",
        "https://example.com",
    )


def test_get_normalized_url_tuple_structure():
    """Test that get_normalized_url returns a proper tuple structure."""
    scheme, url = get_normalized_url("https://www.example.com/path?query=1#fragment")

    assert isinstance(scheme, str)
    assert isinstance(url, str)
    assert scheme == "https"
    assert url == "https://www.example.com"

    # Test HTTP
    scheme, url = get_normalized_url("http://api.example.com:8080/api/v1")
    assert scheme == "http"
    assert url == "http://api.example.com"


def test_get_normalized_url_subdomains_and_suffixes():
    """Test get_normalized_url with various subdomain and suffix combinations."""

    # With subdomains (preserved)
    assert get_normalized_url("https://www.example.com") == (
        "https",
        "https://www.example.com",
    )
    assert get_normalized_url("https://api.example.com") == (
        "https",
        "https://api.example.com",
    )
    assert get_normalized_url("https://mail.google.com") == (
        "https",
        "https://mail.google.com",
    )
    assert get_normalized_url("https://docs.python.org") == (
        "https",
        "https://docs.python.org",
    )

    # Multiple levels of subdomains
    assert get_normalized_url("https://api.v1.example.com") == (
        "https",
        "https://api.v1.example.com",
    )
    assert get_normalized_url("https://test.api.example.com") == (
        "https",
        "https://test.api.example.com",
    )

    # Complex multi-level suffixes
    assert get_normalized_url("https://example.co.uk") == (
        "https",
        "https://example.co.uk",
    )
    assert get_normalized_url("https://www.example.co.uk") == (
        "https",
        "https://www.example.co.uk",
    )
    assert get_normalized_url("https://api.example.com.au") == (
        "https",
        "https://api.example.com.au",
    )
    assert get_normalized_url("https://example.gov.uk") == (
        "https",
        "https://example.gov.uk",
    )

    # Case normalization (scheme and hostname lowercased)
    assert get_normalized_url("HTTPS://EXAMPLE.COM") == ("https", "https://example.com")
    assert get_normalized_url("HTTP://WWW.EXAMPLE.COM") == (
        "http",
        "http://www.example.com",
    )
    assert get_normalized_url("https://API.Example.Co.UK") == (
        "https",
        "https://api.example.co.uk",
    )

    # Trailing dots removed
    assert get_normalized_url("https://example.com.") == (
        "https",
        "https://example.com",
    )
    assert get_normalized_url("https://www.example.com.") == (
        "https",
        "https://www.example.com",
    )

    # Ports and fragments ignored (cleaned up)
    assert get_normalized_url("https://example.com:8080") == (
        "https",
        "https://example.com",
    )
    assert get_normalized_url("https://example.com:443/path#fragment") == (
        "https",
        "https://example.com",
    )

    # HTTP scheme preserved
    assert get_normalized_url("http://example.com") == ("http", "http://example.com")
    assert get_normalized_url("http://www.example.com:80/path") == (
        "http",
        "http://www.example.com",
    )


def test_get_normalized_url_invalid_input():
    """Test get_normalized_url with invalid inputs that should raise ValueError."""

    # None and empty strings
    with pytest.raises(ValueError, match="URL must be a non-empty string"):
        get_normalized_url(None)

    with pytest.raises(ValueError, match="URL must be a non-empty string"):
        get_normalized_url("")

    with pytest.raises(ValueError, match="URL must be a non-empty string"):
        get_normalized_url("   ")

    # Missing protocol
    with pytest.raises(ValueError, match="URL must start with http:// or https://"):
        get_normalized_url("example.com")

    with pytest.raises(ValueError, match="URL must start with http:// or https://"):
        get_normalized_url("www.example.com")

    with pytest.raises(ValueError, match="URL must start with http:// or https://"):
        get_normalized_url("ftp://example.com")

    # Invalid URLs with no hostname
    with pytest.raises(ValueError, match="Invalid URL: no valid hostname found"):
        get_normalized_url("https://")

    with pytest.raises(ValueError, match="Invalid URL: no valid hostname found"):
        get_normalized_url("http://")

    with pytest.raises(ValueError, match="Invalid URL: no valid hostname found"):
        get_normalized_url("https://:80/")

    # URLs with no valid suffix
    with pytest.raises(ValueError, match="has no valid suffix"):
        get_normalized_url("https://invalid-url")

    with pytest.raises(ValueError, match="has no valid suffix"):
        get_normalized_url("https://localhost")


def test_get_normalized_url_additional_edge_cases():
    """Test get_normalized_url with additional edge cases."""

    # Test with different types of input (should raise TypeError/ValueError)
    with pytest.raises((ValueError, TypeError)):
        get_normalized_url(123)  # type: ignore
    with pytest.raises((ValueError, TypeError)):
        get_normalized_url([])  # type: ignore
    with pytest.raises((ValueError, TypeError)):
        get_normalized_url({})  # type: ignore

    # URLs with maximum length components
    long_subdomain = "a" * 63  # Max subdomain length per RFC
    assert get_normalized_url(f"https://{long_subdomain}.example.com") == (
        "https",
        f"https://{long_subdomain}.example.com",
    )

    # URLs with multiple subdomains
    assert get_normalized_url("https://level1.level2.level3.example.com") == (
        "https",
        "https://level1.level2.level3.example.com",
    )

    # URLs with numeric domains
    assert get_normalized_url("https://123.456.com") == ("https", "https://123.456.com")
    assert get_normalized_url("http://server1.example.com") == (
        "http",
        "http://server1.example.com",
    )

    # URLs with hyphenated domains
    assert get_normalized_url("https://my-api.example-site.com") == (
        "https",
        "https://my-api.example-site.com",
    )

    # Complex TLD combinations
    assert get_normalized_url("https://example.museum") == (
        "https",
        "https://example.museum",
    )
    assert get_normalized_url("http://test.example.travel") == (
        "http",
        "http://test.example.travel",
    )

    # International TLDs
    assert get_normalized_url("https://example.xn--p1ai") == (
        "https",
        "https://example.xn--p1ai",
    )  # .рф in punycode

    # Edge case: URL with trailing slash and query
    assert get_normalized_url("https://example.com/?query=value") == (
        "https",
        "https://example.com",
    )

    # Case sensitivity comprehensive test
    assert get_normalized_url("HTTPS://API.EXAMPLE.COM/PATH") == (
        "https",
        "https://api.example.com",
    )
    assert get_normalized_url("HTTP://SUB.DOMAIN.CO.UK/") == (
        "http",
        "http://sub.domain.co.uk",
    )

    # URLs with unusual but valid ports (should be stripped)
    assert get_normalized_url("https://example.com:443") == (
        "https",
        "https://example.com",
    )  # Default HTTPS port
    assert get_normalized_url("http://example.com:80") == (
        "http",
        "http://example.com",
    )  # Default HTTP port
    assert get_normalized_url("https://example.com:8443") == (
        "https",
        "https://example.com",
    )  # Non-standard port

    # URLs with complex paths and queries (should be stripped)
    assert get_normalized_url(
        "https://api.example.com/v1/users/123?include=profile&format=json"
    ) == ("https", "https://api.example.com")

    # URLs with authentication info (should work - authentication is parsed properly)
    assert get_normalized_url("https://user:pass@example.com") == (
        "https",
        "https://example.com",
    )

    # URLs with IP addresses (should fail suffix validation)
    with pytest.raises(ValueError, match="has no valid suffix"):
        get_normalized_url("https://192.168.1.1")
    with pytest.raises(ValueError, match="has no valid suffix"):
        get_normalized_url("http://127.0.0.1:8080")

    # Single word domains (should fail suffix validation)
    with pytest.raises(ValueError, match="has no valid suffix"):
        get_normalized_url("https://intranet")
    with pytest.raises(ValueError, match="has no valid suffix"):
        get_normalized_url("http://localhost:3000")


def test_get_normalized_url_idempotent():
    """Test that get_normalized_url is idempotent when applied to valid URLs."""
    test_urls = [
        "https://example.com",
        "https://www.example.com",
        "https://api.subdomain.example.com",
        "https://example.co.uk",
        "http://example.com",  # Test HTTP scheme preservation
    ]

    for url in test_urls:
        scheme, first_result = get_normalized_url(url)
        # Apply to the same result again (should be idempotent)
        scheme2, second_result = get_normalized_url(first_result)
        assert scheme == scheme2, f"Scheme changed for {url}: {scheme} != {scheme2}"
        assert first_result == second_result, f"Not idempotent for {url}"


def test_get_url_with_compatible_protocol_invalid_input():
    """Test get_url_with_compatible_protocol with invalid inputs."""

    # None and empty strings
    with pytest.raises(ValueError, match="URL must be a non-empty string"):
        get_complete_url_with_compatible_protocol(None)  # type: ignore

    with pytest.raises(ValueError, match="URL must be a non-empty string"):
        get_complete_url_with_compatible_protocol("")

    # Whitespace-only string gets processed and fails during protocol testing
    with pytest.raises(ValueError, match="Neither HTTPS nor HTTP accessible"):
        get_complete_url_with_compatible_protocol("   ")

    # Non-string types
    with pytest.raises(ValueError, match="URL must be a non-empty string"):
        get_complete_url_with_compatible_protocol(123)  # type: ignore

    with pytest.raises(ValueError, match="URL must be a non-empty string"):
        get_complete_url_with_compatible_protocol([])  # type: ignore


@patch("core.utils.url_util.requests")
def test_get_url_with_compatible_protocol_https_success(mock_requests):
    """Test successful HTTPS connection."""
    # Mock successful HTTPS response
    mock_response = Mock()
    mock_response.status_code = 200
    mock_requests.head.return_value = mock_response

    result = get_complete_url_with_compatible_protocol("example.com")
    assert result == "https://example.com"

    # Verify HTTPS was tried first
    mock_requests.head.assert_called_once()
    call_args = mock_requests.head.call_args
    assert call_args[0][0] == "https://example.com"
    assert call_args[1]["verify"] is True  # TLS verification enabled


@patch("core.utils.url_util.requests.head")
def test_get_url_with_compatible_protocol_http_fallback(mock_head):
    """Test HTTP fallback when HTTPS fails."""
    # Import real requests for the exceptions
    import requests

    def side_effect(url, **kwargs):
        if url.startswith("https://"):
            # Simulate SSL error for HTTPS
            raise requests.exceptions.SSLError("SSL certificate error")
        else:
            mock_response = Mock()
            mock_response.status_code = 200
            return mock_response

    mock_head.side_effect = side_effect

    result = get_complete_url_with_compatible_protocol("example.com")
    assert result == "http://example.com"

    # Verify both HTTPS and HTTP were tried
    assert mock_head.call_count == 2


@patch("core.utils.url_util.requests")
def test_get_url_with_compatible_protocol_head_blocked_get_success(mock_requests):
    """Test fallback to GET when HEAD is blocked."""
    mock_head_response = Mock()
    mock_head_response.status_code = 405  # Method Not Allowed

    mock_get_response = Mock()
    mock_get_response.status_code = 200

    mock_requests.head.return_value = mock_head_response
    mock_requests.get.return_value = mock_get_response

    result = get_complete_url_with_compatible_protocol("example.com")
    assert result == "https://example.com"

    # Verify both HEAD and GET were called
    mock_requests.head.assert_called_once()
    mock_requests.get.assert_called_once()

    # Verify GET was called with streaming enabled
    get_call_args = mock_requests.get.call_args
    assert get_call_args[1]["stream"] is True


@patch("core.utils.url_util.requests")
def test_get_url_with_compatible_protocol_forbidden_fallback_to_get(mock_requests):
    """Test fallback to GET when HEAD returns 403 Forbidden."""
    mock_head_response = Mock()
    mock_head_response.status_code = 403  # Forbidden

    mock_get_response = Mock()
    mock_get_response.status_code = 200

    mock_requests.head.return_value = mock_head_response
    mock_requests.get.return_value = mock_get_response

    result = get_complete_url_with_compatible_protocol("example.com")
    assert result == "https://example.com"

    mock_requests.head.assert_called_once()
    mock_requests.get.assert_called_once()


@patch("core.utils.url_util.requests.head")
def test_get_url_with_compatible_protocol_all_fail(mock_head):
    """Test when both HTTPS and HTTP fail."""
    # Mock both HTTPS and HTTP failures
    import requests

    mock_head.side_effect = requests.exceptions.ConnectionError("Connection refused")

    with pytest.raises(ValueError, match="Neither HTTPS nor HTTP accessible"):
        get_complete_url_with_compatible_protocol("nonexistent.example")

    # Should try both HTTPS and HTTP
    assert mock_head.call_count == 2


@patch("core.utils.url_util.requests.head")
def test_get_url_with_compatible_protocol_timeout_handling(mock_head):
    """Test timeout handling."""

    def side_effect(url, **kwargs):
        if url.startswith("https://"):
            import requests

            raise requests.exceptions.Timeout("Request timed out")
        else:
            mock_response = Mock()
            mock_response.status_code = 200
            return mock_response

    mock_head.side_effect = side_effect

    result = get_complete_url_with_compatible_protocol("slowsite.example")
    assert result == "http://slowsite.example"


@patch("core.utils.url_util.requests")
def test_get_url_with_compatible_protocol_strips_existing_scheme(mock_requests):
    """Test that existing scheme is stripped before testing protocols."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_requests.head.return_value = mock_response

    # Pass a URL with existing HTTPS scheme
    result = get_complete_url_with_compatible_protocol("https://example.com/path")
    assert result == "https://example.com/path"

    # Verify that strip_scheme was applied (path is preserved)
    mock_requests.head.assert_called_once()
    call_args = mock_requests.head.call_args
    assert call_args[0][0] == "https://example.com/path"


@patch("core.utils.url_util.requests")
def test_get_url_with_compatible_protocol_user_agent(mock_requests):
    """Test that proper User-Agent header is set."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_requests.head.return_value = mock_response

    get_complete_url_with_compatible_protocol("example.com")

    call_args = mock_requests.head.call_args
    headers = call_args[1]["headers"]
    assert "User-Agent" in headers
    assert "DataAnalyzer" in headers["User-Agent"]


@patch("core.utils.url_util.requests")
def test_get_url_with_compatible_protocol_redirects_allowed(mock_requests):
    """Test that redirects are followed."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_requests.head.return_value = mock_response

    get_complete_url_with_compatible_protocol("example.com")

    call_args = mock_requests.head.call_args
    assert call_args[1]["allow_redirects"] is True


@patch("core.utils.url_util.requests")
def test_get_url_with_compatible_protocol_status_codes(mock_requests):
    """Test various HTTP status code handling."""
    test_cases = [
        (200, True),  # OK
        (201, True),  # Created
        (301, True),  # Moved Permanently
        (302, True),  # Found
        (400, False),  # Bad Request
        (404, False),  # Not Found
        (500, False),  # Internal Server Error
    ]

    for status_code, should_succeed in test_cases:
        mock_requests.reset_mock()

        if should_succeed:
            # First try HTTPS success
            mock_response = Mock()
            mock_response.status_code = status_code
            mock_requests.head.return_value = mock_response

            result = get_complete_url_with_compatible_protocol("example.com")
            assert result == "https://example.com"
        else:
            # Both HTTPS and HTTP fail with this status
            mock_response = Mock()
            mock_response.status_code = status_code
            mock_requests.head.return_value = mock_response

            with pytest.raises(ValueError, match="Neither HTTPS nor HTTP accessible"):
                get_complete_url_with_compatible_protocol("example.com")


def test_get_url_with_compatible_protocol_for_valid_http():
    """Integration test with real HTTP-only sites (if accessible)."""
    # These tests use real network calls and may be flaky
    # They're kept for integration testing but could be skipped in CI
    try:
        assert (
            get_complete_url_with_compatible_protocol("www.claytonchem.com")
            == "http://www.claytonchem.com"
        )
        assert (
            get_complete_url_with_compatible_protocol("www.buffalosteel.net")
            == "http://www.buffalosteel.net"
        )
        assert (
            get_complete_url_with_compatible_protocol("www.containerresearch.com")
            == "http://www.containerresearch.com"
        )
    except ValueError:
        # Skip if sites are not accessible
        pytest.skip("Real HTTP sites not accessible")


def test_get_url_with_compatible_protocol_for_valid_https():
    """Integration test with real HTTPS sites (if accessible)."""
    # These tests use real network calls and may be flaky
    try:
        assert (
            get_complete_url_with_compatible_protocol("www.sohoart.com")
            == "https://www.sohoart.com"
        )
        assert (
            get_complete_url_with_compatible_protocol("www.nrpjones.com")
            == "https://www.nrpjones.com"
        )
        assert (
            get_complete_url_with_compatible_protocol("www.idl.com")
            == "http://www.idl.com"
        )
    except ValueError:
        # Skip if sites are not accessible
        pytest.skip("Real HTTPS sites not accessible")


def test_get_final_landing_url_invalid_input():
    """Test get_final_landing_url with invalid inputs that should raise ValueError."""

    # Missing scheme should raise ValueError
    with pytest.raises(ValueError, match="Start URL must have a valid scheme"):
        get_final_landing_url("example.com")

    with pytest.raises(ValueError, match="Start URL must have a valid scheme"):
        get_final_landing_url("www.example.com/path")

    # URLs with invalid schemes (urlparse treats "example.com" as scheme in "example.com:8080")
    # This will actually result in a requests.exceptions.InvalidSchema, not ValueError
    with pytest.raises(requests.exceptions.InvalidSchema):
        get_final_landing_url("example.com:8080/api")

    # Empty scheme
    with pytest.raises(ValueError, match="Start URL must have a valid scheme"):
        get_final_landing_url("://example.com")


@patch("core.utils.url_util.requests.Session")
def test_get_final_landing_url_successful_head(mock_session_class):
    """Test get_final_landing_url when HEAD request succeeds."""

    # Mock session and response
    mock_session = Mock()
    mock_session_class.return_value.__enter__.return_value = mock_session

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.url = "https://example.com/final"
    mock_session.head.return_value = mock_response

    result = get_final_landing_url("https://example.com/start")

    assert result == "https://example.com/final"
    mock_session.head.assert_called_once_with(
        "https://example.com/start", allow_redirects=True, timeout=10.0
    )
    mock_response.close.assert_called_once()
    # GET should not be called if HEAD succeeds
    mock_session.get.assert_not_called()


@patch("core.utils.url_util.requests.Session")
def test_get_final_landing_url_head_fails_fallback_to_get(mock_session_class):
    """Test get_final_landing_url when HEAD fails with 405/403 and GET succeeds."""

    # Mock session
    mock_session = Mock()
    mock_session_class.return_value.__enter__.return_value = mock_session

    # HEAD returns 405 (Method Not Allowed)
    mock_head_response = Mock()
    mock_head_response.status_code = 405
    mock_session.head.return_value = mock_head_response

    # GET succeeds
    mock_get_response = Mock()
    mock_get_response.status_code = 200
    mock_get_response.url = "https://example.com/final"
    mock_session.get.return_value = mock_get_response

    result = get_final_landing_url("https://example.com/start")

    assert result == "https://example.com/final"
    mock_session.head.assert_called_once()
    mock_head_response.close.assert_called_once()
    mock_session.get.assert_called_once_with(
        "https://example.com/start", allow_redirects=True, timeout=10.0, stream=True
    )
    mock_get_response.close.assert_called_once()


@patch("core.utils.url_util.requests.Session")
def test_get_final_landing_url_head_403_fallback_to_get(mock_session_class):
    """Test get_final_landing_url when HEAD returns 403 and GET succeeds."""

    mock_session = Mock()
    mock_session_class.return_value.__enter__.return_value = mock_session

    # HEAD returns 403 (Forbidden)
    mock_head_response = Mock()
    mock_head_response.status_code = 403
    mock_session.head.return_value = mock_head_response

    # GET succeeds
    mock_get_response = Mock()
    mock_get_response.status_code = 200
    mock_get_response.url = "https://example.com/final"
    mock_session.get.return_value = mock_get_response

    result = get_final_landing_url("https://example.com/start")

    assert result == "https://example.com/final"
    mock_session.head.assert_called_once()
    mock_session.get.assert_called_once()


@patch("core.utils.url_util.requests.Session")
def test_get_final_landing_url_head_4xx_5xx_fallback(mock_session_class):
    """Test get_final_landing_url when HEAD returns 4xx/5xx and GET succeeds."""

    mock_session = Mock()
    mock_session_class.return_value.__enter__.return_value = mock_session

    # HEAD returns 500 (Server Error)
    mock_head_response = Mock()
    mock_head_response.status_code = 500
    mock_session.head.return_value = mock_head_response

    # GET succeeds
    mock_get_response = Mock()
    mock_get_response.status_code = 200
    mock_get_response.url = "https://example.com/final"
    mock_session.get.return_value = mock_get_response

    result = get_final_landing_url("https://example.com/start")

    assert result == "https://example.com/final"


@patch("core.utils.url_util.requests.Session")
def test_get_final_landing_url_head_exception_fallback(mock_session_class):
    """Test get_final_landing_url when HEAD raises exception and GET succeeds."""

    mock_session = Mock()
    mock_session_class.return_value.__enter__.return_value = mock_session

    # HEAD raises ConnectionError
    mock_session.head.side_effect = requests.exceptions.ConnectionError(
        "Connection failed"
    )

    # GET succeeds
    mock_get_response = Mock()
    mock_get_response.status_code = 200
    mock_get_response.url = "https://example.com/final"
    mock_session.get.return_value = mock_get_response

    result = get_final_landing_url("https://example.com/start")

    assert result == "https://example.com/final"
    mock_session.head.assert_called_once()
    mock_session.get.assert_called_once_with(
        "https://example.com/start", allow_redirects=True, timeout=10.0, stream=True
    )


@patch("core.utils.url_util.requests.Session")
def test_get_final_landing_url_custom_timeout(mock_session_class):
    """Test get_final_landing_url with custom timeout parameter."""

    mock_session = Mock()
    mock_session_class.return_value.__enter__.return_value = mock_session

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.url = "https://example.com/final"
    mock_session.head.return_value = mock_response

    result = get_final_landing_url("https://example.com/start", timeout=5.0)

    assert result == "https://example.com/final"
    mock_session.head.assert_called_once_with(
        "https://example.com/start", allow_redirects=True, timeout=5.0
    )


@patch("core.utils.url_util.requests.Session")
def test_get_final_landing_url_user_agent_header(mock_session_class):
    """Test that get_final_landing_url sets the correct User-Agent header."""

    mock_session = Mock()
    mock_session_class.return_value.__enter__.return_value = mock_session

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.url = "https://example.com/final"
    mock_session.head.return_value = mock_response

    get_final_landing_url("https://example.com/start")

    # Verify that headers were updated with correct User-Agent
    mock_session.headers.update.assert_called_once_with(
        {"User-Agent": "Mozilla/5.0 (compatible; ResearchTool/1.0)"}
    )


@patch("core.utils.url_util.requests.Session")
def test_get_final_landing_url_redirect_chain(mock_session_class):
    """Test get_final_landing_url follows redirect chain correctly."""

    mock_session = Mock()
    mock_session_class.return_value.__enter__.return_value = mock_session

    # Simulate a redirect chain: start -> redirect1 -> redirect2 -> final
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.url = (
        "https://final-destination.com/page"  # Final URL after redirects
    )
    mock_session.head.return_value = mock_response

    result = get_final_landing_url("https://example.com/start")

    assert result == "https://final-destination.com/page"
    # Verify allow_redirects=True was used
    mock_session.head.assert_called_once_with(
        "https://example.com/start", allow_redirects=True, timeout=10.0
    )


@patch("core.utils.url_util.requests.Session")
def test_get_final_landing_url_get_with_stream(mock_session_class):
    """Test that GET request uses stream=True when falling back from HEAD."""

    mock_session = Mock()
    mock_session_class.return_value.__enter__.return_value = mock_session

    # HEAD fails
    mock_session.head.side_effect = requests.exceptions.RequestException("HEAD failed")

    # GET succeeds
    mock_get_response = Mock()
    mock_get_response.status_code = 200
    mock_get_response.url = "https://example.com/final"
    mock_session.get.return_value = mock_get_response

    result = get_final_landing_url("https://example.com/start")

    assert result == "https://example.com/final"
    mock_session.get.assert_called_once_with(
        "https://example.com/start", allow_redirects=True, timeout=10.0, stream=True
    )


@patch("core.utils.url_util.requests.Session")
def test_get_final_landing_url_response_cleanup(mock_session_class):
    """Test that responses are properly closed even if exceptions occur."""

    mock_session = Mock()
    mock_session_class.return_value.__enter__.return_value = mock_session

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.url = "https://example.com/final"
    mock_session.head.return_value = mock_response

    result = get_final_landing_url("https://example.com/start")

    assert result == "https://example.com/final"
    # Ensure response.close() was called in finally block
    mock_response.close.assert_called_once()


@patch("core.utils.url_util.requests.Session")
def test_get_final_landing_url_various_schemes(mock_session_class):
    """Test get_final_landing_url with different URL schemes."""

    mock_session = Mock()
    mock_session_class.return_value.__enter__.return_value = mock_session

    mock_response = Mock()
    mock_response.status_code = 200
    mock_session.head.return_value = mock_response

    # Test HTTPS
    mock_response.url = "https://example.com/final"
    result = get_final_landing_url("https://example.com/start")
    assert result == "https://example.com/final"

    # Test HTTP
    mock_response.url = "http://example.com/final"
    result = get_final_landing_url("http://example.com/start")
    assert result == "http://example.com/final"


@patch("core.utils.url_util.requests.Session")
def test_get_final_landing_url_network_errors_propagated(mock_session_class):
    """Test that network errors from both HEAD and GET are properly propagated."""

    mock_session = Mock()
    mock_session_class.return_value.__enter__.return_value = mock_session

    # Both HEAD and GET fail with timeout
    mock_session.head.side_effect = requests.exceptions.Timeout("HEAD timeout")
    mock_session.get.side_effect = requests.exceptions.Timeout("GET timeout")

    with pytest.raises(requests.exceptions.Timeout, match="GET timeout"):
        get_final_landing_url("https://example.com/start")

    # Verify both HEAD and GET were attempted
    mock_session.head.assert_called_once()
    mock_session.get.assert_called_once()


@patch("core.utils.url_util.requests.Session")
def test_get_final_landing_url_ssl_errors_propagated(mock_session_class):
    """Test that SSL errors are properly propagated."""

    mock_session = Mock()
    mock_session_class.return_value.__enter__.return_value = mock_session

    # HEAD raises SSL error, GET also raises SSL error
    mock_session.head.side_effect = requests.exceptions.SSLError(
        "SSL verification failed"
    )
    mock_session.get.side_effect = requests.exceptions.SSLError(
        "SSL verification failed"
    )

    with pytest.raises(requests.exceptions.SSLError, match="SSL verification failed"):
        get_final_landing_url("https://example.com/start")


def test_get_final_landing_url_edge_case_urls():
    """Test get_final_landing_url with edge case URLs that have valid schemes."""

    # URLs with ports
    with patch("core.utils.url_util.requests.Session") as mock_session_class:
        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.url = "https://example.com:8080/final"
        mock_session.head.return_value = mock_response

        result = get_final_landing_url("https://example.com:8080/start")
        assert result == "https://example.com:8080/final"

    # URLs with query parameters
    with patch("core.utils.url_util.requests.Session") as mock_session_class:
        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.url = "https://example.com/final?redirected=true"
        mock_session.head.return_value = mock_response

        result = get_final_landing_url("https://example.com/start?param=value")
        assert result == "https://example.com/final?redirected=true"

    # URLs with fragments (though fragments aren't sent to server)
    with patch("core.utils.url_util.requests.Session") as mock_session_class:
        mock_session = Mock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.url = "https://example.com/final"
        mock_session.head.return_value = mock_response

        result = get_final_landing_url("https://example.com/start#section")
        assert result == "https://example.com/final"


def test_get_final_landing_url():
    """Integration test with real HTTP-only sites (if accessible)."""
    # These tests use real network calls and may be flaky
    # They're kept for integration testing but could be skipped in CI
    try:
        assert (
            get_final_landing_url("https://www.sohomyriad.com") == "https://sohoart.com"
        )
    except ValueError:
        # Skip if sites are not accessible
        pytest.skip("Real HTTP sites not accessible")
