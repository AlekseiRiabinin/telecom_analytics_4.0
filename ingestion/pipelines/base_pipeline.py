from abc import ABC, abstractmethod
from ingestion.db.postgres import get_conn, insert_raw_feature, call_validate_feature


class BaseIngestionPipeline(ABC):
    source_system: str

    @abstractmethod
    def fetch_records(self):
        """Yield (source_object, properties_dict, shapely_geom)."""
        ...

    def run(self):
        with get_conn() as conn:
            for source_object, props, geom in self.fetch_records():
                geom_wkb = geom.wkb
                ingest_id = insert_raw_feature(
                    conn=conn,
                    source_system=self.source_system,
                    source_object=source_object,
                    payload=self.serialize_payload(props),
                    geom_wkb=geom_wkb,
                )
                self.after_insert(conn, ingest_id)

    def serialize_payload(self, props: dict) -> str:
        import json
        return json.dumps(props, default=str)

    def after_insert(self, conn, ingest_id: int):
        call_validate_feature(conn, ingest_id)
