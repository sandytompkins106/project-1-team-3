import requests
from typing import Optional

from config.config import (
    NOMINATIM_BASE_URL,
    NOMINATIM_TIMEOUT,
    NOMINATIM_USER_AGENT,
    NOMINATIM_ZOOM,
)
from loguru import logger


class NominatimClient:
    """Wrapper for OpenStreetMap Nominatim reverse geocoding API."""

    def __init__(self, timeout: Optional[int] = None):
        self.base_url = NOMINATIM_BASE_URL
        self.timeout = timeout if timeout is not None else NOMINATIM_TIMEOUT
        self.zoom = NOMINATIM_ZOOM
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": NOMINATIM_USER_AGENT}
        )

    def reverse_geocode(self, latitude: float, longitude: float) -> Optional[dict]:
        """
        Reverse geocode a coordinate to get address components.

        Returns:
            dict with 'address' key containing address components, or None on error.
        """
        try:
            response = self.session.get(
                f"{self.base_url}/reverse",
                params={
                    "format": "jsonv2",
                    "lat": latitude,
                    "lon": longitude,
                    "zoom": self.zoom,
                    "addressdetails": 1,
                },
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.warning(
                f"Nominatim reverse geocode failed for ({latitude}, {longitude}): {e}"
            )
            return None

    @staticmethod
    def extract_city_state(address: dict) -> tuple[Optional[str], Optional[str]]:
        """Extract city and state from Nominatim address payload."""
        city = (
            address.get("city")
            or address.get("town")
            or address.get("village")
            or address.get("municipality")
            or address.get("county")
        )
        state = address.get("state")
        return city, state
