"""
IMOT.BG Commercial Listings scraper (Bulgaria)
Human-in-the-loop Playwright scraper using Firefox persistent profile.
"""

import asyncio
import json
import os
import re
from typing import Optional
from playwright.async_api import Page, async_playwright
from bs4 import BeautifulSoup, Tag


# ==============================
# PATH CONFIGURATION
# ==============================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))

DATA_DIR = os.path.join(PROJECT_ROOT, "data", "listings")
URLS_FILE = os.path.join(DATA_DIR, "imoti_urls.txt")
OUTPUT_JSON = os.path.join(DATA_DIR, "imoti_commercial.json")

os.makedirs(DATA_DIR, exist_ok=True)


# ==============================
# HELPERS
# ==============================
async def safe_goto(page: Page, url: str):
    for attempt in range(3):
        try:
            await page.goto(url, wait_until="load", timeout=60000)
            return
        except Exception as e:
            print(f"Retry {attempt+1}/3 for {url}: {e}")
            await asyncio.sleep(2)

    raise RuntimeError(f"Failed to load {url}")


def extract_text(soup: BeautifulSoup, label: str) -> Optional[str]:
    """
    Extract value from 'label: value' rows.
    """
    cell: Optional[Tag] = soup.find("td", string=re.compile(label, re.I))
    
    if not cell:
        return None

    value_cell: Optional[Tag] = cell.find_next_sibling("td")
    
    return value_cell.get_text(strip=True) if value_cell else None


def parse_listing(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    def text_or_none(el):
        return el.get_text(strip=True) if el else None

    title = soup.find("h1")
    price = soup.find("div", class_="price")

    description = soup.find("div", class_="description")

    details = {}
    for row in soup.select(".property-params li"):
        label = row.find("span", class_="label")
        value = row.find("span", class_="value")
        if label and value:
            details[label.get_text(strip=True).lower()] = value.get_text(strip=True)

    # Attempt to extract coordinates if present
    lat = lon = None
    map_div = soup.find("div", {"data-lat": True, "data-lng": True})
    if map_div:
        lat = map_div.get("data-lat")
        lon = map_div.get("data-lng")

    return {
        "source": "imoti.info",
        "url": url,
        "title": text_or_none(title),
        "price": text_or_none(price),
        "property_type": details.get("property type"),
        "area_sqm": details.get("area"),
        "city": details.get("city"),
        "district": details.get("district"),
        "description": text_or_none(description),
        "latitude": lat,
        "longitude": lon,
    }


async def scrape_listing(page: Page, url: str) -> dict:
    print(f"Opening: {url}")
    await safe_goto(page, url)

    html = await page.content()
    return parse_listing(html, url)


# ==============================
# MAIN PIPELINE
# ==============================
async def main():
    if not os.path.exists(URLS_FILE):
        raise FileNotFoundError(f"URLs file not found: {URLS_FILE}")

    with open(URLS_FILE, "r", encoding="utf-8") as f:
        urls = [u.strip() for u in f if u.strip()]

    print(f"Loaded {len(urls)} URLs")

    results = []

    async with async_playwright() as p:
        FIREFOX_PROFILE_PATH = os.path.expanduser(
            "~/snap/firefox/common/.mozilla/firefox/c80a2gz4.imot_real"
        )

        browser = await p.firefox.launch_persistent_context(
            user_data_dir=FIREFOX_PROFILE_PATH,
            headless=False,
            viewport={"width": 1366, "height": 768},
            locale="bg-BG",
            slow_mo=50,
            proxy={"server": "socks5://aleksei-proxy.ddns.net:10808"}
        )


        page = await browser.new_page()

        print("\nBrowser launched.")
        print("Interact manually for 10-20 seconds if desired.")
        input("Press ENTER to start scraping...")

        for idx, url in enumerate(urls, start=1):
            try:
                listing = await scrape_listing(page, url)
                results.append(listing)
                print(f"[{idx}/{len(urls)}] Collected")

                await asyncio.sleep(2)

            except Exception as e:
                print(f"ERROR on {url}: {e}")
                input("Fix manually and press ENTER to continue...")

        await browser.close()

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(results)} listings to:")
    print(OUTPUT_JSON)


if __name__ == "__main__":
    asyncio.run(main())
