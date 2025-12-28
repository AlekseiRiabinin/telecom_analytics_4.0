from ingestion.pipelines.base_pipeline import BaseIngestionPipeline
from ingestion.sources.dld_parcels import load_dld_parcels


class DLDParcelsPipeline(BaseIngestionPipeline):
    source_system = "DLD"

    def fetch_records(self):
        gdf = load_dld_parcels()
        for _, row in gdf.iterrows():
            source_object = row.get("parcel_id") or row.get("id")
            props = row.drop(labels=["geometry"]).to_dict()
            geom = row.geometry
            yield source_object, props, geom
