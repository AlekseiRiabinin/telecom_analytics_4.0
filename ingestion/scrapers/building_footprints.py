"""
Developer Building Footprints
(https://github.com/microsoft/GlobalMLBuildingFootprints)
"""

import os
import pandas as pd
import geopandas as gpd
import requests
from shapely.geometry import box


COVERAGE_URL = "https://minedbuildings.z5.web.core.windows.net/global-buildings/buildings-coverage.geojson"
BASE_TILE_URL = "https://minedbuildings.z5.web.core.windows.net/global-buildings/"

OUTPUT_DIR = "data/buildings/"
OUTPUT_SHP = os.path.join(OUTPUT_DIR, "building_footprints.shp")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def download_geojson(url: str) -> dict:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def load_coverage() -> gpd.GeoDataFrame:
    """
    Loads the global coverage file that lists all tiles.
    """
    coverage_json = download_geojson(COVERAGE_URL)
    gdf = gpd.GeoDataFrame.from_features(coverage_json["features"], crs="EPSG:4326")
    return gdf


def filter_tiles_for_dubai(coverage: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Filters tiles that intersect the Dubai bounding box.
    """
    # Dubai bounding box (approx)
    dubai_bbox = box(55.0, 24.5, 55.7, 25.4)

    return coverage[coverage.intersects(dubai_bbox)]


def download_tile(tile_name: str) -> gpd.GeoDataFrame:
    """
    Downloads a single tile GeoJSON and returns it as a GeoDataFrame.
    """
    url = BASE_TILE_URL + tile_name
    tile_json = download_geojson(url)

    gdf = gpd.GeoDataFrame.from_features(tile_json["features"], crs="EPSG:4326")
    return gdf


def merge_tiles(tile_gdfs: list[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    """
    Merges multiple tile GeoDataFrames into one.
    """
    if not tile_gdfs:
        raise ValueError("No building footprint tiles found for Dubai")

    merged = gpd.GeoDataFrame(
        pd.concat(tile_gdfs, ignore_index=True),
        crs="EPSG:4326"
    )
    return merged


def save_as_shapefile(gdf: gpd.GeoDataFrame):
    ensure_dir(OUTPUT_DIR)
    gdf.to_file(OUTPUT_SHP, driver="ESRI Shapefile")


def run():
    print("Downloading coverage file...")
    coverage = load_coverage()

    print("Filtering tiles for Dubai...")
    dubai_tiles = filter_tiles_for_dubai(coverage)

    print(f"Found {len(dubai_tiles)} tiles for Dubai")

    tile_gdfs = []
    for _, row in dubai_tiles.iterrows():
        tile_name = row["properties"]["tile"]
        print(f"Downloading tile: {tile_name}")
        tile_gdf = download_tile(tile_name)
        tile_gdfs.append(tile_gdf)

    print("Merging tiles...")
    merged = merge_tiles(tile_gdfs)

    print("Saving shapefile...")
    save_as_shapefile(merged)

    print("Done.")


if __name__ == "__main__":
    run()

# python scrapers/building_footprints.py
# gpd.read_file("data/buildings/building_footprints.shp")
