import pytest
from core.models.db.manufacturer import Address
from core.utils.address_util import (
    dedupe_addresses,
    can_addresses_A_and_B_merge,
    merge_addresses_A_and_B,
)


# ============================================================================
# Test Fixtures
# ============================================================================


@pytest.fixture
def base_address():
    """Basic address with required fields only."""
    return Address(city="Phoenix", state="AZ", country="US")


@pytest.fixture
def full_address():
    """Address with all fields populated."""
    return Address(
        city="Phoenix",
        state="AZ",
        country="US",
        name="Main Office",
        address_lines=["123 Main St", "Suite 100"],
        county="Maricopa",
        postal_code="85001",
        latitude=33.4484,
        longitude=-112.0740,
        phone_numbers=["602-555-0100", "602-555-0101"],
        fax_numbers=["602-555-0200"],
    )


@pytest.fixture
def address_with_phone():
    """Address with phone numbers."""
    return Address(
        city="Phoenix",
        state="AZ",
        country="US",
        phone_numbers=["602-555-0100"],
    )


@pytest.fixture
def address_with_fax():
    """Address with fax numbers."""
    return Address(
        city="Phoenix",
        state="AZ",
        country="US",
        fax_numbers=["602-555-0200"],
    )


# ============================================================================
# Tests for can_addresses_A_and_B_merge
# ============================================================================


def test_can_merge_identical_addresses(base_address):
    """Test that identical addresses can merge."""
    address_copy = Address(city="Phoenix", state="AZ", country="US")
    assert can_addresses_A_and_B_merge(base_address, address_copy) is True


def test_can_merge_different_base_hash():
    """Test that addresses with different base_hash cannot merge."""
    address_a = Address(city="Phoenix", state="AZ", country="US")
    address_b = Address(city="Tucson", state="AZ", country="US")
    assert can_addresses_A_and_B_merge(address_a, address_b) is False


def test_can_merge_different_states():
    """Test that addresses in different states cannot merge."""
    address_a = Address(city="Phoenix", state="AZ", country="US")
    address_b = Address(city="Phoenix", state="CA", country="US")
    assert can_addresses_A_and_B_merge(address_a, address_b) is False


def test_can_merge_different_countries():
    """Test that addresses in different countries cannot merge."""
    address_a = Address(city="Phoenix", state="AZ", country="US")
    address_b = Address(city="Phoenix", state="AZ", country="CA")
    assert can_addresses_A_and_B_merge(address_a, address_b) is False


def test_can_merge_different_postal_codes():
    """Test that addresses with different postal codes cannot merge."""
    address_a = Address(city="Phoenix", state="AZ", country="US", postal_code="85001")
    address_b = Address(city="Phoenix", state="AZ", country="US", postal_code="85002")
    assert can_addresses_A_and_B_merge(address_a, address_b) is False


def test_can_merge_same_postal_codes():
    """Test that addresses with same postal codes can merge."""
    address_a = Address(city="Phoenix", state="AZ", country="US", postal_code="85001")
    address_b = Address(city="Phoenix", state="AZ", country="US", postal_code="85001")
    assert can_addresses_A_and_B_merge(address_a, address_b) is True


def test_can_merge_one_missing_postal_code():
    """Test that addresses can merge when one has postal code and other doesn't."""
    address_a = Address(city="Phoenix", state="AZ", country="US", postal_code="85001")
    address_b = Address(city="Phoenix", state="AZ", country="US")
    assert can_addresses_A_and_B_merge(address_a, address_b) is True


def test_can_merge_both_missing_postal_codes():
    """Test that addresses can merge when both lack postal codes."""
    address_a = Address(city="Phoenix", state="AZ", country="US")
    address_b = Address(city="Phoenix", state="AZ", country="US")
    assert can_addresses_A_and_B_merge(address_a, address_b) is True


def test_can_merge_no_address_lines():
    """Test that addresses without address_lines can merge."""
    address_a = Address(city="Phoenix", state="AZ", country="US")
    address_b = Address(city="Phoenix", state="AZ", country="US")
    assert can_addresses_A_and_B_merge(address_a, address_b) is True


def test_can_merge_one_missing_address_lines():
    """Test that addresses can merge when one has address_lines and other doesn't."""
    address_a = Address(
        city="Phoenix", state="AZ", country="US", address_lines=["123 Main St"]
    )
    address_b = Address(city="Phoenix", state="AZ", country="US")
    assert can_addresses_A_and_B_merge(address_a, address_b) is True


def test_can_merge_identical_address_lines():
    """Test that addresses with identical address_lines can merge."""
    address_a = Address(
        city="Phoenix",
        state="AZ",
        country="US",
        address_lines=["123 Main St", "Suite 100"],
    )
    address_b = Address(
        city="Phoenix",
        state="AZ",
        country="US",
        address_lines=["123 Main St", "Suite 100"],
    )
    assert can_addresses_A_and_B_merge(address_a, address_b) is True


def test_can_merge_different_order_address_lines():
    """Test that addresses with address_lines in different order can merge (set comparison)."""
    address_a = Address(
        city="Phoenix",
        state="AZ",
        country="US",
        address_lines=["123 Main St", "Suite 100"],
    )
    address_b = Address(
        city="Phoenix",
        state="AZ",
        country="US",
        address_lines=["Suite 100", "123 Main St"],
    )
    assert can_addresses_A_and_B_merge(address_a, address_b) is True


def test_cannot_merge_different_address_lines():
    """Test that addresses with different address_lines cannot merge."""
    address_a = Address(
        city="Phoenix", state="AZ", country="US", address_lines=["123 Main St"]
    )
    address_b = Address(
        city="Phoenix", state="AZ", country="US", address_lines=["456 Oak Ave"]
    )
    assert can_addresses_A_and_B_merge(address_a, address_b) is False


def test_can_merge_empty_address_lines():
    """Test that addresses with empty address_lines list can merge."""
    address_a = Address(city="Phoenix", state="AZ", country="US", address_lines=[])
    address_b = Address(city="Phoenix", state="AZ", country="US", address_lines=[])
    assert can_addresses_A_and_B_merge(address_a, address_b) is True


def test_can_merge_one_empty_address_lines():
    """Test that address with empty list is treated as missing."""
    address_a = Address(city="Phoenix", state="AZ", country="US", address_lines=[])
    address_b = Address(city="Phoenix", state="AZ", country="US")
    assert can_addresses_A_and_B_merge(address_a, address_b) is True


# ============================================================================
# Tests for merge_addresses_A_and_B
# ============================================================================


def test_merge_returns_none_for_unmergeable():
    """Test that merge returns None when addresses cannot be merged."""
    address_a = Address(city="Phoenix", state="AZ", country="US")
    address_b = Address(city="Tucson", state="AZ", country="US")
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result is None


def test_merge_basic_addresses(base_address):
    """Test merging two basic identical addresses."""
    address_copy = Address(city="Phoenix", state="AZ", country="US")
    result = merge_addresses_A_and_B(base_address, address_copy)
    assert result is not None
    assert result.city == "Phoenix"
    assert result.state == "AZ"
    assert result.country == "US"


def test_merge_preserves_address_lines_from_A():
    """Test that address_lines from A is preserved when B doesn't have it."""
    address_a = Address(
        city="Phoenix",
        state="AZ",
        country="US",
        address_lines=["123 Main St", "Suite 100"],
    )
    address_b = Address(city="Phoenix", state="AZ", country="US")
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.address_lines == ["123 Main St", "Suite 100"]


def test_merge_uses_address_lines_from_B():
    """Test that address_lines from B is used when A doesn't have it."""
    address_a = Address(city="Phoenix", state="AZ", country="US")
    address_b = Address(
        city="Phoenix",
        state="AZ",
        country="US",
        address_lines=["456 Oak Ave"],
    )
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.address_lines == ["456 Oak Ave"]


def test_merge_prefers_A_address_lines_when_both_have():
    """Test that address_lines from A is preferred when both have it."""
    address_a = Address(
        city="Phoenix", state="AZ", country="US", address_lines=["123 Main St"]
    )
    address_b = Address(
        city="Phoenix", state="AZ", country="US", address_lines=["123 Main St"]
    )
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.address_lines == ["123 Main St"]


def test_merge_preserves_name_from_A():
    """Test that name from A is preserved when B doesn't have it."""
    address_a = Address(city="Phoenix", state="AZ", country="US", name="Main Office")
    address_b = Address(city="Phoenix", state="AZ", country="US")
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.name == "Main Office"


def test_merge_uses_name_from_B():
    """Test that name from B is used when A doesn't have it."""
    address_a = Address(city="Phoenix", state="AZ", country="US")
    address_b = Address(
        city="Phoenix", state="AZ", country="US", name="Warehouse Location"
    )
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.name == "Warehouse Location"


def test_merge_preserves_county_from_A():
    """Test that county from A is preserved when B doesn't have it."""
    address_a = Address(city="Phoenix", state="AZ", country="US", county="Maricopa")
    address_b = Address(city="Phoenix", state="AZ", country="US")
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.county == "Maricopa"


def test_merge_uses_county_from_B():
    """Test that county from B is used when A doesn't have it."""
    address_a = Address(city="Phoenix", state="AZ", country="US")
    address_b = Address(city="Phoenix", state="AZ", country="US", county="Maricopa")
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.county == "Maricopa"


def test_merge_preserves_postal_code_from_A():
    """Test that postal_code from A is preserved when B doesn't have it."""
    address_a = Address(city="Phoenix", state="AZ", country="US", postal_code="85001")
    address_b = Address(city="Phoenix", state="AZ", country="US")
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.postal_code == "85001"


def test_merge_uses_postal_code_from_B():
    """Test that postal_code from B is used when A doesn't have it."""
    address_a = Address(city="Phoenix", state="AZ", country="US")
    address_b = Address(city="Phoenix", state="AZ", country="US", postal_code="85002")
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.postal_code == "85002"


def test_merge_preserves_latitude_longitude_from_A():
    """Test that geolocation from A is preserved."""
    address_a = Address(
        city="Phoenix", state="AZ", country="US", latitude=33.4484, longitude=-112.0740
    )
    address_b = Address(city="Phoenix", state="AZ", country="US")
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.latitude == 33.4484
    assert result.longitude == -112.0740


def test_merge_combines_phone_numbers():
    """Test that phone numbers from both addresses are combined."""
    address_a = Address(
        city="Phoenix",
        state="AZ",
        country="US",
        phone_numbers=["602-555-0100", "602-555-0101"],
    )
    address_b = Address(
        city="Phoenix",
        state="AZ",
        country="US",
        phone_numbers=["602-555-0102"],
    )
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.phone_numbers
    assert len(result.phone_numbers) == 3
    assert set(result.phone_numbers) == {
        "602-555-0100",
        "602-555-0101",
        "602-555-0102",
    }


def test_merge_deduplicates_phone_numbers():
    """Test that duplicate phone numbers are removed."""
    address_a = Address(
        city="Phoenix",
        state="AZ",
        country="US",
        phone_numbers=["602-555-0100", "602-555-0101"],
    )
    address_b = Address(
        city="Phoenix",
        state="AZ",
        country="US",
        phone_numbers=["602-555-0100"],
    )
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.phone_numbers
    assert len(result.phone_numbers) == 2
    assert set(result.phone_numbers) == {"602-555-0100", "602-555-0101"}


def test_merge_handles_missing_phone_numbers():
    """Test merging when one or both addresses lack phone numbers."""
    address_a = Address(
        city="Phoenix", state="AZ", country="US", phone_numbers=["602-555-0100"]
    )
    address_b = Address(city="Phoenix", state="AZ", country="US")
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.phone_numbers
    assert result.phone_numbers == ["602-555-0100"]


def test_merge_handles_both_missing_phone_numbers():
    """Test merging when both addresses lack phone numbers."""
    address_a = Address(city="Phoenix", state="AZ", country="US")
    address_b = Address(city="Phoenix", state="AZ", country="US")
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.phone_numbers == []


def test_merge_combines_fax_numbers():
    """Test that fax numbers from both addresses are combined."""
    address_a = Address(
        city="Phoenix",
        state="AZ",
        country="US",
        fax_numbers=["602-555-0200", "602-555-0201"],
    )
    address_b = Address(
        city="Phoenix",
        state="AZ",
        country="US",
        fax_numbers=["602-555-0202"],
    )
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.fax_numbers
    assert len(result.fax_numbers) == 3
    assert set(result.fax_numbers) == {"602-555-0200", "602-555-0201", "602-555-0202"}


def test_merge_deduplicates_fax_numbers():
    """Test that duplicate fax numbers are removed."""
    address_a = Address(
        city="Phoenix",
        state="AZ",
        country="US",
        fax_numbers=["602-555-0200", "602-555-0201"],
    )
    address_b = Address(
        city="Phoenix",
        state="AZ",
        country="US",
        fax_numbers=["602-555-0200"],
    )
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.fax_numbers
    assert len(result.fax_numbers) == 2
    assert set(result.fax_numbers) == {"602-555-0200", "602-555-0201"}


def test_merge_handles_missing_fax_numbers():
    """Test merging when one or both addresses lack fax numbers."""
    address_a = Address(
        city="Phoenix", state="AZ", country="US", fax_numbers=["602-555-0200"]
    )
    address_b = Address(city="Phoenix", state="AZ", country="US")
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.fax_numbers
    assert result.fax_numbers == ["602-555-0200"]


def test_merge_handles_both_missing_fax_numbers():
    """Test merging when both addresses lack fax numbers."""
    address_a = Address(city="Phoenix", state="AZ", country="US")
    address_b = Address(city="Phoenix", state="AZ", country="US")
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.fax_numbers == []


def test_merge_full_addresses(full_address):
    """Test merging two fully populated addresses."""
    address_b = Address(
        city="Phoenix",
        state="AZ",
        country="US",
        name="Secondary Office",
        address_lines=["123 Main St", "Suite 100"],
        county="Maricopa",
        postal_code="85001",
        latitude=33.4500,
        longitude=-112.0800,
        phone_numbers=["602-555-0103"],
        fax_numbers=["602-555-0203"],
    )
    result = merge_addresses_A_and_B(full_address, address_b)
    assert result is not None
    assert result.name == "Main Office"  # Prefers A
    assert result.county == "Maricopa"
    assert result.postal_code == "85001"
    assert result.latitude == 33.4484  # From A
    assert result
    assert result.phone_numbers
    assert result.fax_numbers
    assert len(result.phone_numbers) == 3  # Combined and deduplicated
    assert len(result.fax_numbers) == 2  # Combined and deduplicated


def test_merge_empty_phone_fax_lists():
    """Test merging with empty phone/fax lists."""
    address_a = Address(
        city="Phoenix", state="AZ", country="US", phone_numbers=[], fax_numbers=[]
    )
    address_b = Address(city="Phoenix", state="AZ", country="US")
    result = merge_addresses_A_and_B(address_a, address_b)
    assert result
    assert result.phone_numbers == []
    assert result.fax_numbers == []


# ============================================================================
# Tests for dedupe_addresses
# ============================================================================


def test_dedupe_empty_list():
    """Test deduplication of an empty list."""
    addresses = []
    dedupe_addresses(addresses)
    assert addresses == []


def test_dedupe_single_address():
    """Test deduplication with a single address."""
    addresses = [Address(city="Phoenix", state="AZ", country="US")]
    dedupe_addresses(addresses)
    assert len(addresses) == 1


def test_dedupe_two_identical_addresses():
    """Test deduplication of two identical addresses."""
    addresses = [
        Address(city="Phoenix", state="AZ", country="US"),
        Address(city="Phoenix", state="AZ", country="US"),
    ]
    dedupe_addresses(addresses)
    assert len(addresses) == 1
    assert addresses[0].city == "Phoenix"


def test_dedupe_two_different_addresses():
    """Test that different addresses are not merged."""
    addresses = [
        Address(city="Phoenix", state="AZ", country="US"),
        Address(city="Tucson", state="AZ", country="US"),
    ]
    dedupe_addresses(addresses)
    assert len(addresses) == 2


def test_dedupe_multiple_duplicates():
    """Test deduplication with multiple duplicate pairs."""
    addresses = [
        Address(city="Phoenix", state="AZ", country="US", name="Office 1"),
        Address(city="Phoenix", state="AZ", country="US", name="Office 2"),
        Address(city="Tucson", state="AZ", country="US", name="Office 3"),
        Address(city="Tucson", state="AZ", country="US", name="Office 4"),
    ]
    dedupe_addresses(addresses)
    # Should merge Phoenix pair and Tucson pair
    assert len(addresses) == 2
    assert addresses[0].city == "Phoenix"
    assert addresses[1].city == "Tucson"


def test_dedupe_with_phone_numbers():
    """Test deduplication combines phone numbers."""
    addresses = [
        Address(
            city="Phoenix",
            state="AZ",
            country="US",
            phone_numbers=["602-555-0100"],
        ),
        Address(
            city="Phoenix",
            state="AZ",
            country="US",
            phone_numbers=["602-555-0101"],
        ),
    ]
    dedupe_addresses(addresses)
    assert len(addresses) == 1
    assert addresses[0].phone_numbers
    assert len(addresses[0].phone_numbers) == 2
    assert set(addresses[0].phone_numbers) == {"602-555-0100", "602-555-0101"}


def test_dedupe_with_fax_numbers():
    """Test deduplication combines fax numbers."""
    addresses = [
        Address(
            city="Phoenix",
            state="AZ",
            country="US",
            fax_numbers=["602-555-0200"],
        ),
        Address(
            city="Phoenix",
            state="AZ",
            country="US",
            fax_numbers=["602-555-0201"],
        ),
    ]
    dedupe_addresses(addresses)
    assert len(addresses) == 1
    assert addresses[0].fax_numbers
    assert len(addresses[0].fax_numbers) == 2
    assert set(addresses[0].fax_numbers) == {"602-555-0200", "602-555-0201"}


def test_dedupe_preserves_non_duplicates():
    """Test that non-duplicate addresses remain separate."""
    addresses = [
        Address(city="Phoenix", state="AZ", country="US"),
        Address(city="Phoenix", state="AZ", country="US"),
        Address(city="Tucson", state="AZ", country="US"),
    ]
    dedupe_addresses(addresses)
    assert len(addresses) == 2
    cities = {addr.city for addr in addresses}
    assert cities == {"Phoenix", "Tucson"}


def test_dedupe_three_mergeable_addresses():
    """Test deduplication with three consecutive mergeable addresses."""
    addresses = [
        Address(
            city="Phoenix",
            state="AZ",
            country="US",
            phone_numbers=["602-555-0100"],
        ),
        Address(
            city="Phoenix",
            state="AZ",
            country="US",
            phone_numbers=["602-555-0101"],
        ),
        Address(
            city="Phoenix",
            state="AZ",
            country="US",
            phone_numbers=["602-555-0102"],
        ),
    ]
    dedupe_addresses(addresses)
    # The function processes pairs sequentially, so it should merge first two, then that result with third
    # After first merge: [merged(0,1), original(2)]
    # After second merge: [merged(merged(0,1), 2)]
    assert len(addresses) == 1
    # All three phone numbers should be present
    assert addresses[0].phone_numbers
    assert len(addresses[0].phone_numbers) == 3


def test_dedupe_alternating_addresses():
    """Test deduplication with alternating address patterns."""
    addresses = [
        Address(city="Phoenix", state="AZ", country="US", name="A"),
        Address(city="Tucson", state="AZ", country="US", name="B"),
        Address(city="Phoenix", state="AZ", country="US", name="C"),
        Address(city="Tucson", state="AZ", country="US", name="D"),
    ]
    dedupe_addresses(addresses)
    # With the sequential comparison logic, Phoenix A won't merge with Tucson B,
    # then Tucson B won't merge with Phoenix C, etc.
    # The function only merges consecutive pairs
    assert len(addresses) == 4


def test_dedupe_modifies_list_in_place():
    """Test that deduplication modifies the list in place."""
    addresses = [
        Address(city="Phoenix", state="AZ", country="US"),
        Address(city="Phoenix", state="AZ", country="US"),
    ]
    original_list = addresses
    dedupe_addresses(addresses)
    assert addresses is original_list
    assert len(addresses) == 1


def test_dedupe_complex_scenario():
    """Test a complex deduplication scenario."""
    addresses = [
        Address(
            city="Phoenix",
            state="AZ",
            country="US",
            address_lines=["123 Main St"],
            phone_numbers=["602-555-0100"],
        ),
        Address(
            city="Phoenix",
            state="AZ",
            country="US",
            address_lines=["123 Main St"],
            postal_code="85001",
        ),
        Address(
            city="Phoenix",
            state="AZ",
            country="US",
            address_lines=["123 Main St"],
            fax_numbers=["602-555-0200"],
        ),
    ]
    dedupe_addresses(addresses)
    assert len(addresses) == 1
    # All fields should be combined
    assert addresses[0].address_lines == ["123 Main St"]
    assert addresses[0].postal_code == "85001"
    assert addresses[0].phone_numbers == ["602-555-0100"]
    assert addresses[0].fax_numbers == ["602-555-0200"]


def test_dedupe_handles_different_postal_codes():
    """Test that addresses with different postal codes are not merged."""
    addresses = [
        Address(city="Phoenix", state="AZ", country="US", postal_code="85001"),
        Address(city="Phoenix", state="AZ", country="US", postal_code="85002"),
    ]
    dedupe_addresses(addresses)
    assert len(addresses) == 2


def test_dedupe_handles_different_address_lines():
    """Test that addresses with different address_lines are not merged."""
    addresses = [
        Address(
            city="Phoenix", state="AZ", country="US", address_lines=["123 Main St"]
        ),
        Address(
            city="Phoenix", state="AZ", country="US", address_lines=["456 Oak Ave"]
        ),
    ]
    dedupe_addresses(addresses)
    assert len(addresses) == 2
