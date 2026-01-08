"""Safety & Emergency Infrastructure"""

from base.downloader import Downloader
from base.storage import Storage


class SafetyScraper:
    URL = "https://api.dubaipulse.gov.ae/data/safety/emergency_facilities.geojson"

    def run(self):
        dl = Downloader()
        st = Storage()

        response = dl.get(self.URL)
        st.save_json("data/safety/emergency.geojson", response.json())
