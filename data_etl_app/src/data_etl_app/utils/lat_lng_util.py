from typing import Optional
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

from core.models.db.manufacturer import Address

_geolocator = Nominatim(user_agent="sudokn_geocoder")


def get_lat_lng_from_address(addr: Address) -> Optional[tuple[float, float]]:
    """
    Given a string address, return the latitude and longitude using geopy's Nominatim geocoder.
    Returns None if the address cannot be geocoded.
    """
    if addr.latitude is None or addr.longitude is None:
        # Build geocoding query
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

        # query = ", ".join(query_parts)

        if query_parts:
            try:
                for i in range(len(query_parts)):
                    # Add small delay to respect rate limits (max 1 request per second for Nominatim)
                    query = ", ".join(query_parts[i:])

                    location = _geolocator.geocode(query)

                    if location:
                        # addr.latitude = location.latitude  # type: ignore
                        # addr.longitude = location.longitude  # type: ignore
                        print(
                            f"  Geocoded: {query}... -> ({location.latitude}, {location.longitude})"  # type: ignore
                        )
                        return (location.latitude, location.longitude)  # type: ignore
                    else:
                        print(f"  No results for: {query}...")

            except (GeocoderTimedOut, GeocoderServiceError) as e:
                print(f"  Geocoding error for {query[:50]}...: {e}")
            except Exception as e:
                print(f"  Unexpected error geocoding {query[:50]}...: {e}")
