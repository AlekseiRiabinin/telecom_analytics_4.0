import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry.base import BaseGeometry
from ingestion.pipelines.base_pipeline import BaseIngestionPipeline


def sanitize_value(v):
    try:
        if pd.isna(v):
            return None

    except Exception:
        pass

    if isinstance(v, (np.integer,)):
        return int(v)

    if isinstance(v, (np.floating,)):
        return float(v)

    return v


class DubaiBuildingsPipeline(BaseIngestionPipeline):
    source_system = "OSM_Dubai_Buildings"
    batch_size = 500

    def fetch_records(self):
        path = (
            "data/gcc-states-251227-free.shp/filtered/"
            "dubai_buildings.gpkg"
        )

        gdf = gpd.read_file(path).to_crs(4326)

        for _, row in gdf.iterrows():
            geom = row["geometry"]

            if geom is None:
                continue

            try:
                if not isinstance(geom, BaseGeometry):
                    continue

                if geom.area == 0:
                    continue

            except Exception:
                continue

            osm_id = row.get("osm_id")
            if osm_id is None:
                continue

            source_object = str(osm_id)

            props = {
                k: sanitize_value(v)
                for k, v in row.items()
                if k != "geometry"
            }

            if not isinstance(props, dict):
                continue

            yield source_object, props, geom


# python3 -m ingestion.scripts.run_osm_buildings_ingest
