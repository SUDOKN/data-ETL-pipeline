import os
from typing import Optional
import googlemaps

from core.models.db.manufacturer import Address

_gmaps: Optional[googlemaps.Client] = None


def _get_gmaps() -> googlemaps.Client:
    global _gmaps
    if _gmaps is None:
        _gmaps = googlemaps.Client(key=os.environ["GOOGLE_MAPS_API_KEY"])
    return _gmaps


def get_lat_lng_from_address(addr: Address) -> Optional[tuple[float, float]]:
    """
    Given an Address, return (latitude, longitude) using the Google Maps Geocoding API.
    Returns None if the address cannot be geocoded.
    """
    if addr.latitude is not None and addr.longitude is not None:
        return (addr.latitude, addr.longitude)

    query_parts = []
    if addr.address_lines:
        query_parts.append(addr.address_lines[0])
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
            if results:
                loc = results[0]["geometry"]["location"]
                print(f"  Geocoded: {query} -> ({loc['lat']}, {loc['lng']})")
                return (loc["lat"], loc["lng"])
            else:
                print(f"  No results for: {query}")
        except Exception as e:
            print(f"  Geocoding error for {query[:50]}...: {e}")

    return None
