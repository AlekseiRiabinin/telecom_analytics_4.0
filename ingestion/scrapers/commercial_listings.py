"""
Dubizzle Commercial for Rent (Dubai) scraper using real Chrome + CDP pipe.

Requires:
  - Google Chrome or Chromium
  - Python 3.10+ (for text I/O buffering)

Run:
  python3 ingestion/scrapers/dubizzle_cdp_pipe_commercial_rent.py
"""

import asyncio
import json
import os
import time

from playwright.async_api import Page, async_playwright
from bs4 import BeautifulSoup

# ==============================
# PATH CONFIGURATION
# ==============================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))

DATA_DIR = os.path.join(PROJECT_ROOT, "data", "listings")
URLS_FILE = os.path.join(DATA_DIR, "dubizzle_urls.txt")
OUTPUT_JSON = os.path.join(DATA_DIR, "dubizzle_commercial.json")

os.makedirs(DATA_DIR, exist_ok=True)

# ==============================
# EXTRACTION HELPERS
# ==============================
def extract_next_data(html: str) -> dict:
    """
    Extract and parse __NEXT_DATA__ JSON from page HTML.
    """
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")

    if not script or not script.string:
        raise ValueError("__NEXT_DATA__ not found in page")

    return json.loads(script.string)


async def scrape_listing(page: Page, url: str) -> dict:
    """
    Load a single listing page and return the listing JSON.
    """
    print(f"Opening: {url}")
    await page.goto(url, wait_until="networkidle", timeout=60000)

    html = await page.content()
    data = extract_next_data(html)

    try:
        listing = data["props"]["pageProps"]["listing"]
    except KeyError:
        raise KeyError("Listing object not found in __NEXT_DATA__")

    return listing

# ==============================
# MAIN PIPELINE
# ==============================
async def main():
    if not os.path.exists(URLS_FILE):
        raise FileNotFoundError(f"URLs file not found: {URLS_FILE}")

    with open(URLS_FILE, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    print(f"Loaded {len(urls)} URLs")

    all_listings = []

    async with async_playwright() as p:
        # ==============================
        # FIREFOX PERSISTENT PROFILE SETUP
        # ==============================
        # TODO: Update this path to your actual Firefox profile path
        # On Linux: "~/.mozilla/firefox/your_profile.default-release"
        # On Windows: "C:\\Users\\<User>\\AppData\\Roaming\\Mozilla\\Firefox\\Profiles\\xxxx.default-release"
        FIREFOX_PROFILE_PATH = os.path.expanduser("~/.mozilla/firefox/dubizzle_real")

        browser = await p.firefox.launch_persistent_context(
            user_data_dir=FIREFOX_PROFILE_PATH,
            headless=False,  # keep headful
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            slow_mo=50       # human-like pacing
        )

        page = await browser.new_page()

        # ==============================
        # Human-in-the-loop warmup
        # ==============================
        print("\nBrowser launched with your persistent profile.")
        print("Please interact manually for 10-30 seconds if needed (scroll, open a listing).")
        input("Press ENTER to start scraping...")

        # ==============================
        # Scraping loop
        # ==============================
        for idx, url in enumerate(urls, start=1):
            try:
                listing = await scrape_listing(page, url)

                all_listings.append(listing)
                print(
                    f"[{idx}/{len(urls)}] Collected listing: "
                    f"{listing.get('property_id', listing.get('id', 'N/A'))}"
                )

                # polite delay
                time.sleep(2)

            except Exception as e:
                print(f"ERROR on {url}: {e}")
                print("Pause and fix manually if needed.")
                input("Press ENTER to continue...")

        await browser.close()

    # ==============================
    # SAVE OUTPUT
    # ==============================
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_listings, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(all_listings)} listings to:")
    print(OUTPUT_JSON)


if __name__ == "__main__":
    asyncio.run(main())



# How to run:
#   python3 ingestion/scrapers/commercial_listings.py
#
# Jupyter usage:
# import json
# with open("data/listings/dubizzle_commercial_rent.json") as f:
#     listings = json.load(f)
# listings[:3]
