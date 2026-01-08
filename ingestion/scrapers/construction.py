"""Construction Permits"""

from base.downloader import Downloader
from base.storage import Storage


class ConstructionPermitScraper:
    URL = "https://api.dubaipulse.gov.ae/data/construction/permits.json"

    def run(self):
        dl = Downloader()
        st = Storage()

        response = dl.get(self.URL)
        st.save_json("data/construction/permits.json", response.json())
