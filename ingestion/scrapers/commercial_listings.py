"""Commercial Listings (Office, Retail, Industrial)"""

from bs4 import BeautifulSoup
from base.downloader import Downloader
from base.storage import Storage


class CommercialListingScraper:
    URL = "https://example.com/commercial-properties"

    def run(self):
        dl = Downloader()
        st = Storage()

        html = dl.get(self.URL).text
        soup = BeautifulSoup(html, "html.parser")

        listings = []
        for item in soup.select(".listing"):
            listings.append({
                "title": item.select_one(".title").get_text(strip=True),
                "price": item.select_one(".price").get_text(strip=True),
                "location": item.select_one(".location").get_text(strip=True),
            })

        st.save_json("data/listings/commercial.json", listings)
