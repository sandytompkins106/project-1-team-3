import requests
import time
from config.config import OPENAQ_API_KEY, OPENAQ_BASE_URL


class OpenAQClient:

    def __init__(self, timeout: int = 30):
        self.base_url = OPENAQ_BASE_URL
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "X-API-Key": OPENAQ_API_KEY
        })

    def get(self, endpoint: str, params: dict):
        url = f"{self.base_url}/{endpoint}"

        while True:
            response = self.session.get(url, params=params, timeout=self.timeout)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 10))
                time.sleep(retry_after)
                continue

            if response.status_code != 200:
                raise Exception(
                    f"OpenAQ API error {response.status_code}: {response.text}"
                )

            return response.json()