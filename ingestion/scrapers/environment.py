"""Environmental & Urban Quality"""

from base.downloader import Downloader
from base.storage import Storage


class EnvironmentScraper:
    URL = "https://api.dubaipulse.gov.ae/data/environment/air_quality.json"

    def run(self):
        dl = Downloader()
        st = Storage()

        response = dl.get(self.URL)
        st.save_json("data/environment/air_quality.json", response.json())
