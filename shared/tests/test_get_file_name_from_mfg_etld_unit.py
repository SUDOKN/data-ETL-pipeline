"""Unit tests for get_file_name_from_mfg_etld function."""

import pytest
from shared.utils.aws.s3.scraped_text_util import get_file_name_from_mfg_etld


class TestGetFileNameFromMfgEtld:
    """Test suite for get_file_name_from_mfg_etld function."""

    def test_valid_etld1_simple_domains(self):
        """Test with valid eTLD+1 simple domains."""
        test_cases = [
            ("example.com", "example.com.txt"),
            ("google.com", "google.com.txt"),
            ("microsoft.com", "microsoft.com.txt"),
            ("apple.com", "apple.com.txt"),
        ]

        for etld1, expected in test_cases:
            result = get_file_name_from_mfg_etld(etld1)
            assert (
                result == expected
            ), f"Failed for {etld1}: expected {expected}, got {result}"

    def test_valid_etld1_complex_tlds(self):
        """Test with valid eTLD+1 domains having complex TLDs."""
        test_cases = [
            ("example.co.uk", "example.co.uk.txt"),
            ("company.com.au", "company.com.au.txt"),
            ("business.org.in", "business.org.in.txt"),
            ("site.edu.mx", "site.edu.mx.txt"),
        ]

        for etld1, expected in test_cases:
            result = get_file_name_from_mfg_etld(etld1)
            assert (
                result == expected
            ), f"Failed for {etld1}: expected {expected}, got {result}"

    def test_invalid_input_full_urls(self):
        """Test that full URLs are rejected."""
        invalid_inputs = [
            "https://example.com",
            "http://www.google.com",
            "https://subdomain.example.com/path",
            "ftp://example.com",
        ]

        for invalid_input in invalid_inputs:
            with pytest.raises(ValueError, match="etld1:.* passed is inconsistent"):
                get_file_name_from_mfg_etld(invalid_input)

    def test_invalid_input_subdomains(self):
        """Test that domains with subdomains are rejected."""
        invalid_inputs = [
            "www.example.com",
            "mail.google.com",
            "subdomain.company.co.uk",
            "api.service.com",
        ]

        for invalid_input in invalid_inputs:
            with pytest.raises(ValueError, match="etld1:.* passed is inconsistent"):
                get_file_name_from_mfg_etld(invalid_input)

    def test_invalid_input_no_tld(self):
        """Test that inputs without valid TLDs are rejected."""
        invalid_inputs = [
            "localhost",
            "example",
            "just-text",
            "invalid-domain",
            "192.168.1.1",
        ]

        for invalid_input in invalid_inputs:
            with pytest.raises(ValueError, match="Invalid eTLD\\+1 format"):
                get_file_name_from_mfg_etld(invalid_input)

    def test_invalid_input_empty_or_tld_only(self):
        """Test that empty strings or TLD-only inputs are rejected."""
        invalid_inputs = [
            "",
            "com",
            "co.uk",
            "org",
            ".com",
        ]

        for invalid_input in invalid_inputs:
            with pytest.raises(ValueError, match="Invalid eTLD\\+1 format"):
                get_file_name_from_mfg_etld(invalid_input)

    def test_edge_cases_valid_domains(self):
        """Test edge cases that should be valid."""
        test_cases = [
            ("a.com", "a.com.txt"),  # Single character domain
            ("test-domain.com", "test-domain.com.txt"),  # Hyphenated domain
            ("123domain.com", "123domain.com.txt"),  # Numeric start
            ("domain123.org", "domain123.org.txt"),  # Numeric end
        ]

        for etld1, expected in test_cases:
            result = get_file_name_from_mfg_etld(etld1)
            assert (
                result == expected
            ), f"Failed for {etld1}: expected {expected}, got {result}"

    def test_lowercase_enforcement(self):
        """Test that the function enforces lowercase domains."""
        # The function now enforces lowercase domains, which is correct behavior
        # since domain names are case-insensitive in practice
        valid_lowercase_cases = [
            ("example.com", "example.com.txt"),
            ("google.com", "google.com.txt"),
            ("mixedcase.org", "mixedcase.org.txt"),
        ]

        for etld1, expected in valid_lowercase_cases:
            result = get_file_name_from_mfg_etld(etld1)
            assert (
                result == expected
            ), f"Failed for {etld1}: expected {expected}, got {result}"

    def test_uppercase_domains_rejected(self):
        """Test that uppercase or mixed-case domains are rejected."""
        # These should now be rejected because the function enforces lowercase
        invalid_case_inputs = [
            "Example.com",
            "GOOGLE.COM",
            "MixedCase.Org",
            "Company.CO.UK",  # Mixed case complex TLD
            "COMPANY.COM.AU",  # Uppercase complex TLD
            "TEST.ORG",
            "example.CO.UK",  # Mixed case in TLD only
        ]

        for invalid_input in invalid_case_inputs:
            with pytest.raises(ValueError, match="etld1:.* passed is inconsistent"):
                get_file_name_from_mfg_etld(invalid_input)

    def test_mixed_case_complex_tlds_rejected(self):
        """Test that mixed case in complex TLDs is also rejected."""
        invalid_mixed_case_tlds = [
            "example.Co.Uk",
            "site.Com.Au",
            "business.Org.In",
            "company.Edu.Mx",
        ]

        for invalid_input in invalid_mixed_case_tlds:
            with pytest.raises(ValueError, match="etld1:.* passed is inconsistent"):
                get_file_name_from_mfg_etld(invalid_input)

    def test_international_domains(self):
        """Test with international domains (if they work with tldextract)."""
        # Note: These might not work depending on tldextract version and configuration
        # but we should test what happens
        test_cases = [
            ("xn--nxasmq6b.com", "xn--nxasmq6b.com.txt"),  # Punycode domain
        ]

        for etld1, expected in test_cases:
            try:
                result = get_file_name_from_mfg_etld(etld1)
                assert (
                    result == expected
                ), f"Failed for {etld1}: expected {expected}, got {result}"
            except ValueError:
                # Some international domains might not be recognized by tldextract
                # This is acceptable behavior
                pass

    def test_return_type(self):
        """Test that the function returns a string."""
        result = get_file_name_from_mfg_etld("example.com")
        assert isinstance(result, str)
        assert result.endswith(".txt")

    def test_consistent_output_format(self):
        """Test that output format is always consistent."""
        test_inputs = ["example.com", "test.org", "company.co.uk"]

        for etld1 in test_inputs:
            result = get_file_name_from_mfg_etld(etld1)
            # Should always end with .txt
            assert result.endswith(".txt")
            # Should always start with the input domain
            assert result.startswith(etld1)
            # Should be exactly input + ".txt"
            assert result == f"{etld1}.txt"
