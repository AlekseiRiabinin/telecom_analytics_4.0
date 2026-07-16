"""
Scraper for Dubai administrative districts (adm_div subtype=district).
Produces data/urbi_districts.json for use by urbi_cadastral_plots.py.
"""

import json
import math
import requests
from pathlib import Path
from typing import Any


API_URL = "https://pro-api.2gis.ru/items"
OUTPUT_PATH = Path("data/urbi_districts.json")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

AUTH_TOKEN = (
    "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJsQWhVMnpFV0dPM1dldklQ"
    "a05TMjBRTWhkVzQxWDFHTDgwY0VQR2ZPaXRJIn0."
    "eyJleHAiOjE3Njg5NTk5OTMsImlhdCI6MTc2ODk1ODE5MywiYXV0aF90aW1lIjoxNzY4MzAz"
    "ODkxLCJqdGkiOiJmYTY2ZTkzNC04ZjQyLTRmNzUtOWRkYi0yNDg4YjQ3NDZhN2MiLCJpc3Mi"
    "OiJodHRwczovL2lkLnBsYXRmb3JtLnVyYmkuYWUvcmVhbG1zL3VyYmkiLCJzdWIiOiI2MjA5"
    "NTc4NyIsInR5cCI6IkJlYXJlciIsImF6cCI6InByby11aSIsIm5vbmNlIjoiZGIwOWY5MmUt"
    "ZTEwYy00OTBhLTkyOTEtNGJkYjk1YWUxZDhjIiwic2Vzc2lvbl9zdGF0ZSI6ImQxMTdjNzY1"
    "LWJjMzYtNGRkYS05ZWY0LTY0ZTI0NDhmY2NkMCIsInNjb3BlIjoib3BlbmlkIGVtYWlsIDJn"
    "aXMtcHJvOl9pbXBlcnNvbmF0ZSBuYW1lIHBob25lIiwic2lkIjoiZDExN2M3NjUtYmMzNi00"
    "ZGRhLTllZjQtNjRlMjQ0OGZjY2QwIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImVtYWlsIjoi"
    "ZGFuaXRhdHRAcmVhbC1kZWFsLmFlIn0."
    "gLuRM4sJve_1mmbvVORyE89BZhuCWkI4ZIvDR9MSSDLBRcBqFFq13C63c0EXvCv7Cs5JCnwN"
    "T-GxgBv1A2oxMLUVzFqorOFr6GDegaQWhvqwMBNc0YtmKnr9iRER9hA_U8PKXeHatWCJHBjh"
    "WANGUHj4M0QK1HNFBHuYF06iYGnMy4AyzyyMKu8r8IfcHcZ7A14BxosGz2QaeG5hGTbj4xjT"
    "LcEUrLGQmxlTTh2BOf2C-YxkEFHw_TX-4tptw4PD6IjTwTEZucWl4L11akswp4qF0OwIhw18"
    "Zm2QmXr1Z_NZjxxlf1fxHTSRW-CQ2kuVefT8sWDBV2fllZp_kvFenQ"
)

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://pro.urbi.ae",
    "Referer": "https://pro.urbi.ae/",
    "User-Agent": "Mozilla/5.0",
    "X-Brand": "urbi",
    "Authorization": f"Bearer {AUTH_TOKEN}",
}

DUBAI_CITY_ID = "13933634117435393"


def build_payload(page: int) -> dict:
    return {
        "assets": ["adm_div"],
        "page": page,
        "page_size": 50,  # URBI limit
        "attributes": [],
        "geo_filters": [
            {
                "$type": "operator:or",
                "items": [
                    {
                        "$type": "named_filter",
                        "enabled": True,
                        "name": "Dubai",
                        "id": DUBAI_CITY_ID,
                        "filter": {
                            "$type": "filter:adm_div",
                            "adm_div_ids": [DUBAI_CITY_ID],
                        },
                    }
                ],
            }
        ],
        "locale": "en_AE",
    }


def fetch_page(page: int, session: requests.Session) -> dict:
    payload = build_payload(page)
    resp = session.post(API_URL, json=payload, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def main() -> None:
    print("Fetching Dubai administrative districts...")

    session = requests.Session()

    # Fetch first page
    first_payload = build_payload(1)
    first_resp = session.post(API_URL, json=first_payload, headers=HEADERS, timeout=30)
    first_resp.raise_for_status()

    data: dict[str, Any] = first_resp.json()
    total: int = data.get("total", 0)
    page_size: int = 50
    pages: int = math.ceil(total / page_size)

    print(f"Total adm_div items: {total}, pages: {pages}")

    all_items: list[dict[str, Any]] = data.get("items", [])

    # Fetch remaining pages
    for page in range(2, pages + 1):
        print(f"Fetching page {page}/{pages}...")
        payload = build_payload(page)
        resp = session.post(API_URL, json=payload, headers=HEADERS, timeout=30)
        resp.raise_for_status()

        page_data: dict[str, Any] = resp.json()
        items: list[dict[str, Any]] = page_data.get("items", [])
        all_items.extend(items)

    print(f"Total items collected: {len(all_items)}")

    # Filter only subtype='district'
    districts: list[dict[str, Any]] = [
        {
            "id": item["id"],
            "name": item["name"],
            "name_ar": item.get("name[ar]"),
            "point": item.get("point"),
            "wkt": item.get("wkt"),
        }
        for item in all_items
        if item.get("subtype") == "district"
    ]

    print(f"Districts extracted: {len(districts)}")

    OUTPUT_PATH.write_text(json.dumps(districts, ensure_ascii=False, indent=2))
    print(f"Saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
