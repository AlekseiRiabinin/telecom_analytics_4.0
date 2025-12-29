from ingestion.pipelines.base_pipeline import BaseIngestionPipeline
import geopandas as gpd


class OSMBuildingsDubaiPipeline(BaseIngestionPipeline):
    source_system = "OSM_Free_Dubai"

    def fetch_records(self):
        gdf = gpd.read_file("path/to/dubai_buildings.gpkg")
        for _, row in gdf.iterrows():
            yield row.get("osm_id"), row.drop("geometry").to_dict(), row.geometry
