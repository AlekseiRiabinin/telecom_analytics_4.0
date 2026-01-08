"""GADM UAE admin boundaries"""

import os
import geopandas as gpd


GADM_URL = "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_ARE_1.json"
OUTPUT_DIR = "data/boundaries/gadm_dubai/"
OUTPUT_SHP = os.path.join(OUTPUT_DIR, "dubai_admin.shp")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def fetch_dubai_admin() -> gpd.GeoDataFrame:
    """
    Download UAE level-1 admin boundaries from GADM
    and return only the Dubai emirate in EPSG:4326.
    """
    uae = gpd.read_file(GADM_URL)
    dubai = uae[uae["NAME_1"] == "Dubai"].to_crs(4326)
    return dubai


def save_as_shapefile(gdf: gpd.GeoDataFrame, shp_path: str = OUTPUT_SHP) -> None:
    ensure_dir(os.path.dirname(shp_path))
    gdf.to_file(shp_path, driver="ESRI Shapefile")


def run():
    dubai = fetch_dubai_admin()
    save_as_shapefile(dubai)


if __name__ == "__main__":
    run()

# python scrapers/gadm_dubai_admin.py
# gpd.read_file("data/boundaries/gadm_dubai/dubai_admin.shp")
