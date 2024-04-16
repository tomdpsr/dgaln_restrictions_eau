import os
from typing import List


import requests


def download_files(
    list_urls: List[dict],
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