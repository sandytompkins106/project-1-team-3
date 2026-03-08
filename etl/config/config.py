import os
from dotenv import load_dotenv

load_dotenv()

OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
OPENAQ_BASE_URL = os.getenv("OPENAQ_BASE_URL", "https://api.openaq.org/v3")

NOMINATIM_BASE_URL = os.getenv("NOMINATIM_BASE_URL", "https://nominatim.openstreetmap.org")
NOMINATIM_TIMEOUT = int(os.getenv("NOMINATIM_TIMEOUT", "20"))
NOMINATIM_USER_AGENT = os.getenv("NOMINATIM_USER_AGENT", "openaq-data-pipeline/1.0")
NOMINATIM_ZOOM = int(os.getenv("NOMINATIM_ZOOM", "10"))

if not OPENAQ_API_KEY:
    raise ValueError("OPENAQ_API_KEY is not set.")