"""Dubai Pulse — Citywide Open Data"""

from base.downloader import Downloader
from base.storage import Storage


class DubaiPulseScraper:
    BASE = "https://api.dubaipulse.gov.ae/data/"

    def run(self):
        dl = Downloader()
        st = Storage()

        datasets = ["population", "schools", "parks", "health_facilities"]

        for ds in datasets:
            url = f"{self.BASE}/{ds}.json"
            response = dl.get(url)
            st.save_json(f"data/pulse/{ds}.json", response.json())
