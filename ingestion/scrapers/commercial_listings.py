"""
Dubai Commercial Listings (Office, Retail, Industrial)
Source: Bayut
"""

import os
import json
import requests
from typing import Optional
from bs4 import BeautifulSoup
from bs4.element import Tag


# -------------------------------------------------------------------
# Resolve project root and data directory
# -------------------------------------------------------------------

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "listings")
OUTPUT_JSON = os.path.join(DATA_DIR, "dubai_commercial_listings.json")


# -------------------------------------------------------------------
# Bayut commercial URLs
# -------------------------------------------------------------------

BAYUT_URLS = {
    "commercial_mixed": "https://www.bayut.com/for-sale/commercial/dubai/",
    "commercial_buildings": "https://www.bayut.com/for-sale/commercial-buildings/dubai/",
    "offices_min_500sqft": "https://www.bayut.com/s/offices-sale-from-500-sqft-dubai/",
    "offices_min_1m": "https://www.bayut.com/s/offices-sale-aed-1000000-dubai/",
    "industrial_rent_dic": "https://www.bayut.com/to-rent/commercial/dubai/dubai-industrial-city/",
    "dip_commercial": "https://www.bayut.com/for-sale/commercial/dubai/dubai-investment-park-dip/",
}


# -------------------------------------------------------------------
# Utility functions
# -------------------------------------------------------------------

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def fetch_html(url: str) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def parse_listings(soup: BeautifulSoup, source_url: str) -> list[dict]:
    listings: list[dict] = []

    cards: list[Tag] = soup.select("[data-testid='listing-card']")
    if not cards:
        return listings

    for card in cards:
        title_el: Optional[Tag] = card.select_one("[data-testid='listing-title']")
        price_el: Optional[Tag] = card.select_one("[data-testid='listing-price']")
        location_el: Optional[Tag] = card.select_one("[data-testid='listing-location']")

        title = title_el.get_text(strip=True) if title_el else None
        price = price_el.get_text(strip=True) if price_el else None
        location = location_el.get_text(strip=True) if location_el else None

        listings.append({
            "title": title,
            "price": price,
            "location": location,
            "source_url": source_url,
        })

    return listings


def scrape_category(base_url: str) -> list[dict]:
    page = 1
    results: list[dict] = []

    while True:
        separator = "&" if "?" in base_url else "?"
        url = f"{base_url}{separator}page={page}"
        print(f"Scraping: {url}")

        html = fetch_html(url)
        soup = BeautifulSoup(html, "html.parser")

        page_results = parse_listings(soup, url)
        if not page_results:
            break

        results.extend(page_results)
        page += 1

    return results


def save_json(path: str, data: list[dict]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# -------------------------------------------------------------------
# Main runner
# -------------------------------------------------------------------

def run() -> None:
    all_results: list[dict] = []

    for category, url in BAYUT_URLS.items():
        print(f"\n=== Scraping category: {category} ===")
        listings = scrape_category(url)

        for item in listings:
            item["category"] = category

        all_results.extend(listings)

    print(f"\nTotal listings scraped: {len(all_results)}")
    print(f"Saving to: {OUTPUT_JSON}")
    save_json(OUTPUT_JSON, all_results)


if __name__ == "__main__":
    run()


# Load the scraped data in Jupyter

# import json

# with open("data/listings/dubai_commercial_listings.json") as f:
#     listings = json.load(f)

# listings[:3]
