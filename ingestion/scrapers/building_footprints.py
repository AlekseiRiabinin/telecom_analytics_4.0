"""
Microsoft Building Footprints for Dubai
via Planetary Computer STAC (ms-buildings)

Flow:
  1. Query STAC for tiles intersecting Dubai bbox
  2. For each tile:
       - take ABFS href
       - convert to HTTPS Planetary Computer data URL
       - sign with planetary_computer
       - download parquet locally (cached)
       - load with GeoPandas
  3. Merge all tiles
  4. Filter to Dubai boundary (GADM shapefile)
  5. Save as ESRI Shapefile for downstream use
"""

import os
import requests
import pandas as pd
import geopandas as gpd
import planetary_computer as pc
import pystac_client
from shapely.ops import unary_union


# -------------------------------------------------------------------
# Paths and constants
# -------------------------------------------------------------------

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))

BUILDINGS_DIR = os.path.join(PROJECT_ROOT, "data", "buildings")
TILE_CACHE_DIR = os.path.join(BUILDINGS_DIR, "tiles")
OUTPUT_SHP = os.path.join(BUILDINGS_DIR, "building_footprints_dubai.shp")

DUBAI_BOUNDARY_SHP = os.path.join(
    PROJECT_ROOT, "data", "boundaries", "gadm_dubai", "dubai_admin.shp"
)

# Dubai bbox (same as before)
DUBAI_BBOX = {
    "type": "Polygon",
    "coordinates": [[
        [54.8911, 24.6231],
        [56.2054, 24.6231],
        [56.2054, 25.3741],
        [54.8911, 25.3741],
        [54.8911, 24.6231]
    ]]
}


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def abfs_to_https(abfs_url: str) -> str:
    """
    Convert an ABFS URL from the ms-buildings STAC asset into
    the corresponding Planetary Computer HTTPS data endpoint.
    """
    # abfs://footprints/delta/...
    path = abfs_url.replace("abfs://", "")
    return f"https://planetarycomputer.microsoft.com/api/data/v1/{path}"


def download_to_local(url: str, local_path: str) -> str:
    """
    Download a (signed) HTTPS parquet URL to a local file, with simple caching.
    """
    if os.path.exists(local_path):
        print(f"Using cached tile: {local_path}")
        return local_path

    print(f"Downloading tile to {local_path}")
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    return local_path


def load_dubai_boundary(shp_path: str) -> gpd.GeoDataFrame:
    print(f"Loading Dubai boundary from {shp_path} ...")
    dubai = gpd.read_file(shp_path)
    if dubai.crs is None:
        dubai.set_crs(epsg=4326, inplace=True)
    else:
        dubai = dubai.to_crs(epsg=4326)
    return dubai


def filter_to_dubai(buildings: gpd.GeoDataFrame, dubai: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    print("Filtering buildings to Dubai boundary...")
    dubai_geom = unary_union(dubai.geometry)

    buildings = buildings[buildings.geometry.notnull()]
    sindex = buildings.sindex

    possible_idx = list(sindex.intersection(dubai_geom.bounds))
    candidates = buildings.iloc[possible_idx]

    mask = candidates.intersects(dubai_geom)
    dubai_buildings = candidates[mask]

    print(
        f"Total buildings: {len(buildings)}, "
        f"candidates: {len(candidates)}, "
        f"Dubai-only: {len(dubai_buildings)}"
    )

    return dubai_buildings


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def run():
    ensure_dir(BUILDINGS_DIR)
    ensure_dir(TILE_CACHE_DIR)

    print("Connecting to Planetary Computer STAC...")
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1"
    )

    print("Searching for building footprints intersecting Dubai...")
    search = catalog.search(
        collections=["ms-buildings"],
        intersects=DUBAI_BBOX
    )

    items = list(search.get_items())
    if not items:
        raise RuntimeError("No ms-buildings tiles found intersecting Dubai bbox.")

    print(f"Found {len(items)} tiles. Downloading and loading...")

    tile_gdfs = []

    for item in items:
        asset = item.assets["data"]
        abfs_url = asset.href

        https_url = abfs_to_https(abfs_url)
        signed_href = pc.sign(https_url)

        # Use quadkey part as local filename
        # e.g. .../quadkey=123023133 -> quadkey=123023133.parquet
        tile_id = abfs_url.split("/")[-1]
        local_tile_path = os.path.join(TILE_CACHE_DIR, f"{tile_id}.parquet")

        local_file = download_to_local(signed_href, local_tile_path)

        print(f"Reading parquet: {local_file}")
        gdf = gpd.read_parquet(local_file)

        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)
        else:
            gdf = gdf.to_crs(epsg=4326)

        tile_gdfs.append(gdf)

    print("Merging tiles...")
    buildings_all = gpd.GeoDataFrame(
        pd.concat(tile_gdfs, ignore_index=True),
        crs="EPSG:4326"
    )

    dubai = load_dubai_boundary(DUBAI_BOUNDARY_SHP)
    buildings_dubai = filter_to_dubai(buildings_all, dubai)

    ensure_dir(BUILDINGS_DIR)
    print(f"Saving shapefile to: {OUTPUT_SHP}")
    buildings_dubai.to_file(OUTPUT_SHP, driver="ESRI Shapefile")

    print("Done.")


if __name__ == "__main__":
    run()


# How to run:
#   python3 ingestion/scrapers/building_footprints.py
#
# How to load in Jupyter:
#   import geopandas as gpd
#   gdf = gpd.read_file("data/buildings/building_footprints_dubai.shp")
