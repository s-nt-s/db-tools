import urllib.request
import json
from typing import List, Dict, Union
from functools import cache


class GitHub:
    def get(url: str) -> str:
        with urllib.request.urlopen(url) as response:
            if response.status != 200:
                raise RuntimeError(f"{url} status code {response.status}")
            return response.read().decode()

    def json(url) -> Union[List, Dict]:
        txt = GitHub.get(url)
        return json.loads(txt)

    @staticmethod
    @cache
    def get_asset(repo: str, sufix: str) -> str:
        url_api = f"https://api.github.com/repos/{repo}/releases/latest"
        data = GitHub.json(url_api)
        for asset in data['assets']:
            if asset['name'].endswith(sufix):
                return asset['browser_download_url']
        raise RuntimeError(f"{url_api} asset *{sufix} not found")