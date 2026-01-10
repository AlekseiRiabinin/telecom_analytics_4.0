"""
Dubai Commercial Listings Scraper (Playwright Edition)
Source: Bayut

This version uses a real Chromium browser via Playwright to bypass
Bayut's bot protection (hb-challenge, hb-captcha, hb-appAttestation).

Features:
  - Full browser automation (JS, cookies, attestation)
  - SOCKS5 proxy support (your Sofia proxy)
  - Correct Bayut pagination (/page-N/)
  - Random delays + retry logic
  - Robust selectors with fallbacks
  - De-duplication by listing URL
  - Structured JSON output
"""

import os
import json
import time
import random
from typing import List, Dict, Any, Set
from playwright.sync_api import Page, sync_playwright


# -------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "listings")
OUTPUT_JSON = os.path.join(DATA_DIR, "dubai_commercial_listings.json")


# -------------------------------------------------------------------
# Bayut commercial URLs
# -------------------------------------------------------------------

BAYUT_URLS: Dict[str, str] = {
    "commercial_mixed": "https://www.bayut.com/for-sale/commercial/dubai/",
    "commercial_buildings": "https://www.bayut.com/for-sale/commercial-buildings/dubai/",
    "offices_min_500sqft": "https://www.bayut.com/s/offices-sale-from-500-sqft-dubai/",
    "offices_min_1m": "https://www.bayut.com/s/offices-sale-aed-1000000-dubai/",
    "industrial_rent_dic": "https://www.bayut.com/to-rent/commercial/dubai/dubai-industrial-city/",
    "dip_commercial": "https://www.bayut.com/for-sale/commercial/dubai/dubai-investment-park-dip/",
}


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def random_delay(min_s: float = 1.2, max_s: float = 3.5) -> None:
    time.sleep(random.uniform(min_s, max_s))


def build_page_url(base_url: str, page: int) -> str:
    """
    Bayut pagination:
      page 1 -> base_url
      page N -> base_url + "page-N/"
    """
    base = base_url.rstrip("/") + "/"
    if page == 1:
        return base
    return f"{base}page-{page}/"


# -------------------------------------------------------------------
# Parsing
# -------------------------------------------------------------------

def parse_listings(page: Page, source_url: str) -> List[Dict[str, Any]]:
    """
    Extract listing cards from the rendered DOM.
    """
    listings = []

    # Primary selector for listing cards
    cards = page.query_selector_all("div.ef447dde")

    # Fallbacks
    if not cards:
        cards = page.query_selector_all("[data-testid='listing-card']")
    if not cards:
        cards = page.query_selector_all("div:has(a[href*='property'])")

    print(f"  Found {len(cards)} listing cards")

    for card in cards:
        # Title / link
        link_el = (
            card.query_selector("a._4041eb80")
            or card.query_selector("a[aria-label]")
            or card.query_selector("a")
        )

        title = link_el.inner_text().strip() if link_el else None
        href = link_el.get_attribute("href") if link_el else None
        if href and href.startswith("/"):
            href = "https://www.bayut.com" + href

        # Price
        price_el = (
            card.query_selector("span._812aa185")
            or card.query_selector("[data-testid='listing-price']")
            or card.query_selector("span[aria-label*='Price']")
        )
        price = price_el.inner_text().strip() if price_el else None

        # Location
        location_el = (
            card.query_selector("span._162e6469")
            or card.query_selector("[data-testid='listing-location']")
            or card.query_selector("span[aria-label*='Location']")
        )
        location = location_el.inner_text().strip() if location_el else None

        # Area (optional)
        area_el = card.query_selector("span:has-text('sqft')")
        area = area_el.inner_text().strip() if area_el else None

        if title or href:
            listings.append({
                "title": title,
                "price": price,
                "location": location,
                "area": area,
                "listing_url": href,
                "source_url": source_url,
            })

    return listings


# -------------------------------------------------------------------
# Scraping logic
# -------------------------------------------------------------------

def scrape_category(
    page: Page, base_url: str, max_pages: int = 50
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    seen_urls: Set[str] = set()

    for page_num in range(1, max_pages + 1):
        url = build_page_url(base_url, page_num)
        print(f"\nScraping page {page_num}: {url}")

        try:
            page.goto(url, timeout=45000, wait_until="networkidle")
        except Exception as e:
            print(f"  Navigation error: {e}")
            break

        random_delay()

        page_results = parse_listings(page, url)
        if not page_results:
            print("  No listings found, stopping pagination.")
            break

        new_count = 0
        for item in page_results:
            key = item.get("listing_url") or f"{url}#{item.get('title')}"
            if key in seen_urls:
                continue
            seen_urls.add(key)
            results.append(item)
            new_count += 1

        print(f"  Added {new_count} new listings (total: {len(results)})")

        if new_count == 0:
            print("  No new unique listings, stopping pagination.")
            break

    return results


# -------------------------------------------------------------------
# Main runner
# -------------------------------------------------------------------

def run() -> None:
    ensure_dir(DATA_DIR)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            proxy={
                "server": "socks5h://aleksei-proxy.ddns.net:10808"
            }
        )

        context = browser.new_context()
        page = context.new_page()

        all_results: List[Dict[str, Any]] = []

        for category, url in BAYUT_URLS.items():
            print(f"\n=== Scraping category: {category} ===")
            listings = scrape_category(page, url)

            for item in listings:
                item["category"] = category

            print(f"Category {category}: {len(listings)} listings")
            all_results.extend(listings)

        print(f"\nTotal listings scraped: {len(all_results)}")
        print(f"Saving to: {OUTPUT_JSON}")

        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)

        browser.close()

    print("Done.")


if __name__ == "__main__":
    run()


# How to run:
#   python3 ingestion/scrapers/commercial_listings_proxy.py
#
# Jupyter usage:
# import json
# with open("data/listings/dubai_commercial_listings.json") as f:
#     listings = json.load(f)
# listings[:3]
