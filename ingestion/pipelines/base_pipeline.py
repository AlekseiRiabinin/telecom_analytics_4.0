from abc import ABC, abstractmethod
import json
import logging
from shapely import to_wkb
from ingestion.db.postgres import (
    insert_raw_feature,
    insert_raw_features_batch,
    call_validate_feature,
    get_conn,
)
from ingestion.utils.geo import compute_checksum

logger = logging.getLogger(__name__)


class BaseIngestionPipeline(ABC):
    source_system: str
    batch_size: int = 500  # good default for OSM-scale data

    @abstractmethod
    def fetch_records(self):
        """Yield (source_object, properties_dict, shapely_geom)."""
        ...

    def preprocess(self, props, geom):
        return props, geom

    def after_validation(self, conn, ingest_id, validation_result):
        pass

    def serialize_payload(self, props: dict) -> str:
        return json.dumps(props, default=str)

    def run(self):
        logger.info(f"Starting ingestion for {self.source_system}")

        batch = []

        with get_conn() as conn:
            for i, (source_object, props, geom) in enumerate(self.fetch_records(), start=1):
                try:
                    props, geom = self.preprocess(props, geom)

                    payload = self.serialize_payload(props)
                    geom_wkb = to_wkb(geom) if geom else None
                    checksum = compute_checksum(payload, geom_wkb)

                    batch.append(
                        (
                            self.source_system,
                            source_object,
                            payload,
                            geom_wkb,
                            checksum,
                            False,  # is_processed
                        )
                    )

                    if len(batch) >= self.batch_size:
                        self._flush_batch(conn, batch)
                        batch.clear()

                except Exception as e:
                    logger.error(f"Error preparing record {source_object}: {e}")
                    continue

            # flush remaining
            if batch:
                self._flush_batch(conn, batch)

        logger.info(f"Completed ingestion for {self.source_system}")

    def _flush_batch(self, conn, batch):
        ingest_ids = insert_raw_features_batch(conn, batch)
        for ingest_id in ingest_ids:
            ok = call_validate_feature(conn, ingest_id)
            self.after_validation(conn, ingest_id, ok)
