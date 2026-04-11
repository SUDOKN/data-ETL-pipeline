"""
Tests for data_etl_app.utils.lat_lng_util

Unit tests mock `_get_gmaps()` via monkeypatch so the Google Maps client is never
instantiated (no GOOGLE_MAPS_API_KEY needed at import or test time).

Integration tests (marked `pytest.mark.integration`) hit the real API and require
GOOGLE_MAPS_API_KEY to be set in the environment.
"""

import pytest
import data_etl_app.utils.lat_lng_util as lat_lng_module
from data_etl_app.utils.lat_lng_util import get_lat_lng_from_address
from core.models.db.manufacturer import Address


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_gmaps_mock(results):
    """Return a fake gmaps client whose geocode() always returns `results`."""

    class FakeGmaps:
        def geocode(self, query):
            return results

    return FakeGmaps()


def _make_gmaps_result(lat, lng):
    return [{"geometry": {"location": {"lat": lat, "lng": lng}}}]


def _patch_gmaps(monkeypatch, mock_client):
    monkeypatch.setattr(lat_lng_module, "_get_gmaps", lambda: mock_client)


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

def test_returns_existing_coords_without_calling_api(monkeypatch):
    """If addr already has lat/lng, skip the API call entirely."""
    called = []

    class NeverCallGmaps:
        def geocode(self, query):
            called.append(query)
            return []

    _patch_gmaps(monkeypatch, NeverCallGmaps())

    addr = Address(city="Phoenix", state="AZ", country="US", latitude=33.44, longitude=-112.07)
    result = get_lat_lng_from_address(addr)

    assert result == (33.44, -112.07)
    assert called == [], "API should not have been called when coords already exist"


def test_geocodes_full_address(monkeypatch):
    """Happy path: full address geocoded on first attempt."""
    _patch_gmaps(monkeypatch, _make_gmaps_mock(_make_gmaps_result(33.4484, -112.0740)))

    addr = Address(
        address_lines=["1 S Washington St"],
        city="Phoenix",
        state="AZ",
        postal_code="85004",
        country="US",
    )
    result = get_lat_lng_from_address(addr)

    assert result is not None
    assert result == (33.4484, -112.0740)


def test_falls_back_to_shorter_query_on_no_results(monkeypatch):
    """If the first (full) query returns no results, retry with fewer fields."""
    call_count = [0]

    class PartialFallbackGmaps:
        def geocode(self, query):
            call_count[0] += 1
            # Only return a result on the second (shorter) query
            if call_count[0] == 1:
                return []
            return _make_gmaps_result(33.4484, -112.0740)

    _patch_gmaps(monkeypatch, PartialFallbackGmaps())

    addr = Address(
        address_lines=["Unknown Street 999"],
        city="Phoenix",
        state="AZ",
        country="US",
    )
    result = get_lat_lng_from_address(addr)

    assert result == (33.4484, -112.0740)
    assert call_count[0] == 2, "Should have retried once with a shorter query"


def test_returns_none_when_all_queries_return_no_results(monkeypatch):
    """If every fallback query returns nothing, return None."""
    _patch_gmaps(monkeypatch, _make_gmaps_mock([]))

    addr = Address(city="Nowhere", state="XX", country="ZZ")
    result = get_lat_lng_from_address(addr)

    assert result is None


def test_returns_none_when_api_raises_exception(monkeypatch):
    """Exceptions from the API are caught; function returns None."""

    class ErrorGmaps:
        def geocode(self, query):
            raise RuntimeError("network error")

    _patch_gmaps(monkeypatch, ErrorGmaps())

    addr = Address(city="Phoenix", state="AZ", country="US")
    result = get_lat_lng_from_address(addr)

    assert result is None


def test_excludes_not_applicable_state_from_query(monkeypatch):
    """State == 'Not Applicable' must not be included in the geocoding query."""
    captured_queries = []

    class CapturingGmaps:
        def geocode(self, query):
            captured_queries.append(query)
            return _make_gmaps_result(40.7128, -74.0060)

    _patch_gmaps(monkeypatch, CapturingGmaps())

    addr = Address(city="New York", state="Not Applicable", country="US")
    get_lat_lng_from_address(addr)

    assert captured_queries, "geocode should have been called"
    for q in captured_queries:
        assert "Not Applicable" not in q


def test_address_with_no_useful_fields_returns_none(monkeypatch):
    """An address whose only fields are empty/Not Applicable builds no query and returns None."""
    _patch_gmaps(monkeypatch, _make_gmaps_mock([]))

    addr = Address(city="", state="Not Applicable", country="")
    result = get_lat_lng_from_address(addr)

    assert result is None


def test_uses_only_first_address_line(monkeypatch):
    """Only address_lines[0] should appear in the query, not subsequent lines."""
    captured_queries = []

    class CapturingGmaps:
        def geocode(self, query):
            captured_queries.append(query)
            return _make_gmaps_result(33.0, -112.0)

    _patch_gmaps(monkeypatch, CapturingGmaps())

    addr = Address(
        address_lines=["100 Main St", "Suite 200", "Floor 3"],
        city="Tempe",
        state="AZ",
        country="US",
    )
    get_lat_lng_from_address(addr)

    first_query = captured_queries[0]
    assert "100 Main St" in first_query
    assert "Suite 200" not in first_query
    assert "Floor 3" not in first_query


# ---------------------------------------------------------------------------
# Integration test — hits real Google Maps API
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_integration_geocodes_real_address():
    """Sanity-check the live API with a well-known address."""
    addr = Address(
        address_lines=["1600 Amphitheatre Pkwy"],
        city="Mountain View",
        state="CA",
        postal_code="94043",
        country="US",
    )
    result = get_lat_lng_from_address(addr)

    assert result is not None, "Real address should geocode successfully"
    lat, lng = result
    # Google HQ is approximately 37.42°N, 122.08°W
    assert 37.0 < lat < 38.0, f"Unexpected latitude: {lat}"
    assert -123.0 < lng < -121.0, f"Unexpected longitude: {lng}"
