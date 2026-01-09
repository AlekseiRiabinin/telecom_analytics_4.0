"""
Developer Building Footprints
(https://github.com/microsoft/GlobalMLBuildingFootprints)
"""

import os
import pandas as pd
import geopandas as gpd
import requests
from shapely.geometry import box


# -------------------------------------------------------------------
# Resolve project root and data directory
# -------------------------------------------------------------------

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "buildings")
OUTPUT_SHP = os.path.join(DATA_DIR, "building_footprints.shp")


# -------------------------------------------------------------------
# Microsoft Building Footprints URLs
# -------------------------------------------------------------------

COVERAGE_URL = "https://minedbuildings.z5.web.core.windows.net/global-buildings/buildings-coverage.geojson"
BASE_TILE_URL = "https://minedbuildings.z5.web.core.windows.net/global-buildings/"


# -------------------------------------------------------------------
# Utility functions
# -------------------------------------------------------------------

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def download_geojson(url: str) -> dict:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def load_coverage() -> gpd.GeoDataFrame:
    coverage_json = download_geojson(COVERAGE_URL)
    return gpd.GeoDataFrame.from_features(coverage_json["features"], crs="EPSG:4326")


def filter_tiles_for_dubai(coverage: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # Dubai bounding box (geodata.ucdavis.edu)
    dubai_bbox = box(54.8911, 24.6231, 56.2054, 25.3741)
    return coverage[coverage.intersects(dubai_bbox)]


def download_tile(tile_name: str) -> gpd.GeoDataFrame:
    url = BASE_TILE_URL + tile_name
    tile_json = download_geojson(url)
    return gpd.GeoDataFrame.from_features(tile_json["features"], crs="EPSG:4326")


def merge_tiles(tile_gdfs: list[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    merged = gpd.GeoDataFrame(pd.concat(tile_gdfs, ignore_index=True), crs="EPSG:4326")
    return merged


def save_as_shapefile(gdf: gpd.GeoDataFrame):
    ensure_dir(DATA_DIR)
    gdf.to_file(OUTPUT_SHP, driver="ESRI Shapefile")


# -------------------------------------------------------------------
# Main runner
# -------------------------------------------------------------------

def run():
    print("Downloading coverage file...")
    coverage = load_coverage()

    print("Filtering tiles for Dubai...")
    dubai_tiles = filter_tiles_for_dubai(coverage)
    print(f"Found {len(dubai_tiles)} tiles for Dubai")

    tile_gdfs = []
    for _, row in dubai_tiles.iterrows():

        props = row.to_dict()

        tile_name = (
            props.get("tile") or
            props.get("location") or
            props.get("quadkey") or
            props.get("path")
        )

        if not tile_name:
            raise ValueError(f"Could not find tile name in row: {props}")

        print(f"Downloading tile: {tile_name}")
        tile_gdf = download_tile(tile_name)
        tile_gdfs.append(tile_gdf)

    print("Merging tiles...")
    merged = merge_tiles(tile_gdfs)

    print(f"Saving shapefile to: {OUTPUT_SHP}")
    save_as_shapefile(merged)

    print("Done.")


if __name__ == "__main__":
    run()


# python3 ingestion/scrapers/building_footprints.py
# gpd.read_file("data/buildings/building_footprints.shp")
