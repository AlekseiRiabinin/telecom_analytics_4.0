"""Dubai Municipality — Zoning Layers"""

from base.downloader import Downloader
from base.storage import Storage


class ZoningScraper:
    URL = "https://services.arcgis.com/.../Zoning/FeatureServer/0/query"

    def run(self):
        dl = Downloader()
        st = Storage()

        params = {"where": "1=1", "outFields": "*", "f": "geojson"}
        response = dl.get(self.URL, params=params)
        st.save_json("data/zoning/zoning.geojson", response.json())
