from shapely.geometry import Point
from ingestion.pipelines.base_pipeline import BaseIngestionPipeline
from ingestion.sources.rta_gtfs import load_rta_stops


class RTAGTFSPipeline(BaseIngestionPipeline):
    source_system = "RTA"

    def fetch_records(self):
        df = load_rta_stops()
        for _, row in df.iterrows():
            source_object = row["stop_id"]
            props = row.to_dict()
            geom = Point(row["stop_lon"], row["stop_lat"])
            yield source_object, props, geom
