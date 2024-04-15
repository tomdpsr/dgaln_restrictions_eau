import os
from typing import List, Optional


import requests
from urllib3.util import Url


def download_files(
    list_urls: List[Url],
):
    for url in list_urls:
        with requests.get(
            url["url"],
            stream=True,
        ) as r:
            r.raise_for_status()
            print(os.getcwd())
            with open(f"{url['dest_path']}{url['dest_name']}", "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)