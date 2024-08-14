import configparser
from datetime import datetime
from typing import Dict
import requests


class InvalidOperationError(Exception):
    pass


class APIHandler:
    def __init__(
        self,
        request_params: Dict[str, str],
        base_url: str = 'https://www.alphavantage.co/query?',
    ):
        self.base_url = base_url
        self.request_params = request_params
        self.api_key = self._read_api_key()

    def _read_api_key(self) -> str:
        config = configparser.ConfigParser()
        config.read(
            './godata2023/config/config.ini'
        )
        api_key = config.get('API', 'api_key')
        return api_key

    def get_endpoint(self) -> str:
        params = ""
        for key, value in self.request_params.items():
            params += f"{key}={value}&"
        api_key = self._read_api_key()
        url = self.base_url + params + f"apikey={api_key}"
        return url

    def request_data(self, url: str) -> dict:
        r = requests.get(url)
        if r.status_code != 200:
            raise InvalidOperationError("Request failed")
        data = r.json()
        return data
