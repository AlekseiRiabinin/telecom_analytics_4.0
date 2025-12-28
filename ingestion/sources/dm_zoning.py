import geopandas as gpd
from ingestion.config.settings import settings


def load_dm_zoning() -> "gpd.GeoDataFrame":
    # Could be ArcGIS REST, WFS, or GeoJSON
    gdf = gpd.read_file(settings.dm_zoning_url)
    gdf = gdf.to_crs(epsg=4326)
    return gdf
