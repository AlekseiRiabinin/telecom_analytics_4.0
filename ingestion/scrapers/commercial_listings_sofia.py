"""
Dubizzle Commercial for Rent (Dubai) scraper using real Chrome + CDP pipe.

Requires:
  - Google Chrome or Chromium
  - Python 3.10+ (for text I/O buffering)

Run:
  python3 ingestion/scrapers/dubizzle_cdp_pipe_commercial_rent.py
"""

import os
import json
import asyncio
import random
from typing import List, Dict, Any

from playwright.async_api import Page, async_playwright

# --- CONFIG ---

IMOT_BASE_URL = "https://imoti.info"
IMOT_SEARCH_URL = (
    "https://imoti.info/en/for-sale/grad-sofiya/business-properties?pubtype=7&pubtype=14"
)

MAX_PAGES = 10

# Bulgarian residential proxy
PROXY_SERVER = "socks5://aleksei-proxy.ddns.net:10808"

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "listings")
OUTPUT_JSON = os.path.join(DATA_DIR, "imoti_info_commercial_sofia.json")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def random_delay(a: float = 0.8, b: float = 1.8) -> float:
    return random.uniform(a, b)


async def extract_listings(page: Page) -> List[Dict[str, Any]]:
    """
    Extract listing cards from imoti.info.
    """
    listings = []

    # imoti.info uses <div class="estate"> for each listing
    cards = await page.query_selector_all("div.estate")
    for card in cards:
        # Title + URL
        link = await card.query_selector("a.title")
        if not link:
            continue

        title = (await link.inner_text()).strip()
        href = await link.get_attribute("href")
        url = href if href.startswith("http") else IMOT_BASE_URL + href

        # Price
        price_el = await card.query_selector(".price")
        price = (await price_el.inner_text()).strip() if price_el else None

        # Location
        loc_el = await card.query_selector(".location")
        location = (await loc_el.inner_text()).strip() if loc_el else None

        # Size (m²)
        size_el = await card.query_selector(".params span")
        size = (await size_el.inner_text()).strip() if size_el else None

        listings.append(
            {
                "title": title,
                "listing_url": url,
                "price": price,
                "location": location,
                "size": size,
            }
        )

    return listings


async def go_to_page(page: Page, page_num: int) -> str:
    """
    imoti.info uses ?page=N for pagination.
    """
    if page_num == 1:
        await page.goto(IMOT_SEARCH_URL, wait_until="networkidle")
        return page.url

    url = IMOT_SEARCH_URL + f"&page={page_num}"
    await page.goto(url, wait_until="networkidle")
    return page.url


async def scrape_imoti_info() -> None:
    ensure_dir(DATA_DIR)
    all_results: List[Dict[str, Any]] = []
    seen = set()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=False,
            proxy={"server": PROXY_SERVER},
        )
        context = await browser.new_context()
        page = await context.new_page()

        for page_num in range(1, MAX_PAGES + 1):
            print(f"\nScraping page {page_num}...")
            url = await go_to_page(page, page_num)
            print("At URL:", url)

            # Wait for dynamic listings to render
            try:
                await page.wait_for_selector("div.estate", timeout=10000)
            except:
                print("Listings did not load — stopping.")
                break

            # Optional: dump HTML for debugging
            # html = await page.content()
            # with open(f"debug_page_{page_num}.html", "w", encoding="utf-8") as f:
            #     f.write(html)

            page_listings = await extract_listings(page)
            if not page_listings:
                print("No listings found, stopping.")
                break

            new_count = 0
            for item in page_listings:
                key = item["listing_url"]
                if key not in seen:
                    seen.add(key)
                    all_results.append(item)
                    new_count += 1

            print(f"Added {new_count} new listings (total: {len(all_results)})")

            if new_count == 0:
                print("No new unique listings, stopping.")
                break

            await page.wait_for_timeout(int(random_delay(1500, 3000)))

        await browser.close()

    print(f"\nSaving {len(all_results)} listings to {OUTPUT_JSON}")
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    print("Done.")


if __name__ == "__main__":
    asyncio.run(scrape_imoti_info())


# How to run:
#   python3 ingestion/scrapers/commercial_listings_sofia.py
#
# Jupyter usage:
# import json
# with open("data/listings/imot_bg_commercial_sofia.json") as f:
#     listings = json.load(f)
# listings[:3]
