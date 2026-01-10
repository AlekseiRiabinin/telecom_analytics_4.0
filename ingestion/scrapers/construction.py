"""
Construction Permits Scraper (Dubai Municipality)
Downloads the Building_Permits.csv dataset from Dubai Pulse.
"""

import os
from base.downloader import Downloader
from base.storage import Storage


# -------------------------------------------------------------------
# Resolve project root and data directory
# -------------------------------------------------------------------

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "construction")
OUTPUT_CSV = os.path.join(DATA_DIR, "building_permits.csv")


# -------------------------------------------------------------------
# Dubai Pulse dataset URL (CSV file)
# -------------------------------------------------------------------

CSV_URL = (
    "https://www.dubaipulse.gov.ae/api/3/action/package_show?"
    "id=dm_building_permits-open"
)

# NOTE:
# The actual CSV file URL is inside the dataset metadata.
# We must fetch the metadata first, then extract the resource URL.


class ConstructionPermitScraper:

    def __init__(self):
        self.downloader = Downloader()
        self.storage = Storage()

    def ensure_dir(self, path: str):
        os.makedirs(path, exist_ok=True)

    def get_csv_url(self):
        """
        Fetch dataset metadata and extract the CSV resource URL.
        """
        response = self.downloader.get(CSV_URL)
        data = response.json()

        resources = data["result"]["resources"]
        for r in resources:
            if r["format"].lower() == "csv":
                return r["url"]

        raise RuntimeError("CSV resource not found in dataset metadata")

    def run(self):
        self.ensure_dir(DATA_DIR)

        print("Fetching dataset metadata...")
        csv_url = self.get_csv_url()
        print(f"CSV file URL: {csv_url}")

        print("Downloading Building_Permits.csv...")
        response = self.downloader.get(csv_url)

        print(f"Saving to: {OUTPUT_CSV}")
        self.storage.save_bytes(OUTPUT_CSV, response.content)

        print("Done.")


if __name__ == "__main__":
    ConstructionPermitScraper().run()
