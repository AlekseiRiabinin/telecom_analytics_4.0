"""Dubai Land Department (DLD) — Parcel Boundaries"""

from ingestion.base.downloader import Downloader
from ingestion.base.storage import Storage


class DLDParcelScraper:
    URL = "https://services.arcgis.com/.../FeatureServer/0/query"

    def run(self):
        dl = Downloader()
        st = Storage()

        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "geojson"
        }

        response = dl.get(self.URL, params=params)
        st.save_json("data/dld/parcels.geojson", response.json())
