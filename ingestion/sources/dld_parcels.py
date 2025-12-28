import geopandas as gpd
from ingestion.config.settings import settings


def load_dld_parcels() -> "gpd.GeoDataFrame":
    # Example: URL or local file
    gdf = gpd.read_file(settings.dld_parcels_url)
    gdf = gdf.to_crs(epsg=4326)
    return gdf
