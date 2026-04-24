import os
from typing import Optional
import googlemaps

from core.models.db.manufacturer import Address

_gmaps: Optional[googlemaps.Client] = None


def _get_gmaps() -> googlemaps.Client:
    global _gmaps
    if _gmaps is None:
        _gmaps = googlemaps.Client(key=os.getenv("GOOGLE_MAPS_API_KEY"))
    return _gmaps


def get_geocode_result_from_address(addr: Address) -> Optional[tuple[dict, str]]:
    """
    Given an Address, call the Google Maps Geocoding API and return a tuple of
    (raw first result dict, query string sent to the API).
    Returns None if the address cannot be geocoded.
    """

    query_parts = []
    if addr.address_lines:
        query_parts.extend(addr.address_lines)
    if addr.city:
        query_parts.append(addr.city)
    if addr.postal_code:
        query_parts.append(addr.postal_code)
    if addr.state and addr.state != "Not Applicable":
        query_parts.append(addr.state)
    if addr.country:
        query_parts.append(addr.country)

    for i in range(len(query_parts)):
        query = ", ".join(query_parts[i:])
        try:
            results = _get_gmaps().geocode(query)  # type: ignore[attr-defined]
            print(f"_get_gmaps().geocode(query) results:{results}")
            if results:
                loc = results[0]["geometry"]["location"]
                place_id = results[0].get("place_id")
                print(
                    f"  Geocoded: {query} -> ({loc['lat']}, {loc['lng']}), place_id={place_id}"
                )
                return (results[0], query)
            else:
                print(f"  No results for: {query}")
        except Exception as e:
            print(f"  Geocoding error for {query[:50]}...: {e}")

    return None


def get_lat_lng_from_address(
    addr: Address, force: Optional[bool] = False
) -> Optional[tuple[float, float, Optional[str]]]:
    """
    Given an Address, return (latitude, longitude, place_id) using the Google Maps Geocoding API.
    Returns None if the address cannot be geocoded.
    place_id is Google's globally unique identifier for the geocoded location (may be None if absent).
    """
    if (
        not force
        and addr.latitude is not None
        and addr.longitude is not None
        and addr.place_id is not None
    ):
        return (addr.latitude, addr.longitude, addr.place_id)

    # force=True because the cache check above already handled the not-forced case
    geocode = get_geocode_result_from_address(addr)
    if geocode is None:
        return None

    result, _query = geocode
    loc = result["geometry"]["location"]
    place_id = result.get("place_id")
    return (loc["lat"], loc["lng"], place_id)
