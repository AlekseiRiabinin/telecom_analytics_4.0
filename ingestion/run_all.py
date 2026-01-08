from scrapers.dld_parcels import DLDParcelScraper
from scrapers.zoning import ZoningScraper
from scrapers.rta_transport import RTATransportScraper
from scrapers.building_footprints import BuildingFootprintScraper
from scrapers.commercial_listings import CommercialListingScraper
from scrapers.dubai_pulse import DubaiPulseScraper
from scrapers.environment import EnvironmentScraper
from scrapers.construction import ConstructionPermitScraper
from scrapers.poi import POIScraper
from scrapers.safety import SafetyScraper


def run_all():
    DLDParcelScraper().run()
    ZoningScraper().run()
    RTATransportScraper().run()
    BuildingFootprintScraper().run()
    CommercialListingScraper().run()
    DubaiPulseScraper().run()
    EnvironmentScraper().run()
    ConstructionPermitScraper().run()
    POIScraper().run()
    SafetyScraper().run()


if __name__ == "__main__":
    run_all()
