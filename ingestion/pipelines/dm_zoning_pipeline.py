from ingestion.pipelines.base_pipeline import BaseIngestionPipeline
from ingestion.sources.dm_zoning import load_dm_zoning


class DMZoningPipeline(BaseIngestionPipeline):
    source_system = "DubaiMunicipality"

    def fetch_records(self):
        gdf = load_dm_zoning()
        for _, row in gdf.iterrows():
            source_object = row.get("zone_id") or row.get("id")
            props = row.drop(labels=["geometry"]).to_dict()
            geom = row.geometry
            yield source_object, props, geom
