"""Points of Interest (POIs)"""

from base.downloader import Downloader
from base.storage import Storage


class POIScraper:
    URL = "https://api.dubaipulse.gov.ae/data/poi/poi.geojson"

    def run(self):
        dl = Downloader()
        st = Storage()

        response = dl.get(self.URL)
        st.save_json("data/poi/poi.geojson", response.json())
