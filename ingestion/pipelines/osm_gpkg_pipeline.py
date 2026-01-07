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

    if isinstance(v, np.integer):
        return int(v)

    if isinstance(v, np.floating):
        return float(v)

    return v


class DubaiGpkgPipeline(BaseIngestionPipeline):
    """
    Generic pipeline for any Dubai-filtered GPKG dataset.
    """

    # Class-level type hints
    path: str
    source_system: str
    min_area: float
    batch_size: int = 500

    def __init__(self, path: str, source_system: str, min_area: float = 0.0):
        super().__init__()

        # Instance attributes
        self.path = path
        self.source_system = source_system
        self.min_area = min_area

    def fetch_records(self):
        gdf = gpd.read_file(self.path).to_crs(4326)

        for _, row in gdf.iterrows():
            geom = row.get("geometry")

            if geom is None or not isinstance(geom, BaseGeometry):
                continue

            if self.min_area > 0 and geom.area < self.min_area:
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

            yield source_object, props, geom
