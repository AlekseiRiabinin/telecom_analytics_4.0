import requests
from requests.adapters import HTTPAdapter, Retry


class Downloader:
    def __init__(self, timeout=30):
        retries = Retry(total=5, backoff_factor=1)
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.timeout = timeout

    def get(self, url, params=None, headers=None):
        response = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
        response.raise_for_status()
        return response
