"""
Docstring for ingestion.scrapers.urbi_cadastral_plots
"""

import json
import math
import time
import requests
from pathlib import Path
from typing import Any


API_URL = "https://pro-api.2gis.ru/items"

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

HEADERS: dict[str, str] = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://pro.urbi.ae",
    "Referer": "https://pro.urbi.ae/",
    "User-Agent": "Mozilla/5.0",
    "X-Brand": "urbi",
    "Authorization": f"Bearer {AUTH_TOKEN}",
}

DISTRICTS_PATH: Path = Path("data/urbi_districts.json")
OUTPUT_ROOT: Path = Path("data/urbi_cadastral_plots")
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)


def build_payload(district_id: str, page: int) -> dict[str, Any]:
    return {
        "assets": ["cadastral_plot"],
        "page": page,
        "page_size": 50,
        "attributes": [],
        "geo_filters": [
            {
                "$type": "operator:or",
                "items": [
                    {
                        "$type": "named_filter",
                        "enabled": True,
                        "name": "District",
                        "id": district_id,
                        "filter": {
                            "$type": "filter:adm_div",
                            "adm_div_ids": [district_id],
                        },
                    }
                ],
            }
        ],
        "locale": "en_AE",
    }


def fetch_page(session: requests.Session, district_id: str, page: int) -> dict[str, Any]:
    payload = build_payload(district_id, page)
    resp = session.post(API_URL, json=payload, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def scrape_district(session: requests.Session, district: dict[str, Any]) -> None:
    raw_name: Any = district.get("name", "")
    name_str: str = str(raw_name)

    district_name: str = (
        name_str
        .replace("/", "_")
        .replace("\\", "_")
        .replace(" ", "_")
    )

    district_id: str = str(district["id"])

    print(f"\n=== Scraping district: {district_name} ({district_id}) ===")

    out_dir: Path = OUTPUT_ROOT / district_name
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file: Path = out_dir / "plots.json"

    # Fetch first page
    first_page: dict[str, Any] = fetch_page(session, district_id, 1)
    total: int = first_page.get("total", 0)
    pages: int = math.ceil(total / 50)

    print(f"Total plots: {total}, pages: {pages}")

    all_items: list[dict[str, Any]] = first_page.get("items", [])

    # Fetch remaining pages
    for page in range(2, pages + 1):
        print(f"  Page {page}/{pages}")
        page_data: dict[str, Any] = fetch_page(session, district_id, page)
        items: list[dict[str, Any]] = page_data.get("items", [])
        all_items.extend(items)
        time.sleep(0.2)

    out_file.write_text(json.dumps(all_items, ensure_ascii=False, indent=2))
    print(f"Saved {len(all_items)} plots → {out_file}")


def main() -> None:
    print("Loading district list...")
    districts: list[dict[str, Any]] = json.loads(DISTRICTS_PATH.read_text())

    print(f"Loaded {len(districts)} districts.")

    session = requests.Session()

    for district in districts:
        scrape_district(session, district)


if __name__ == "__main__":
    main()
