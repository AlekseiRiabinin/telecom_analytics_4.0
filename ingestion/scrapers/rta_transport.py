"""RTA — Roads, Metro, Bus Stops"""

from base.downloader import Downloader
from base.storage import Storage


class RTATransportScraper:
    BASE = "https://api.dubaipulse.gov.ae/data/transport/"

    def run(self):
        dl = Downloader()
        st = Storage()

        endpoints = {
            "roads": "roads.geojson",
            "metro_stations": "metro_stations.geojson",
            "bus_stops": "bus_stops.geojson"
        }

        for name, file in endpoints.items():
            url = self.BASE + file
            response = dl.get(url)
            st.save_json(f"data/rta/{name}.geojson", response.json())
