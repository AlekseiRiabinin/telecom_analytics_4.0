import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from ingestion.config.settings import settings


@contextmanager
def get_conn():
    conn = psycopg2.connect(settings.db_dsn)
    try:
        yield conn
    finally:
        conn.close()

def insert_raw_feature(conn, source_system, source_object, payload, geom_wkb):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingest.raw_features (source_system, source_object, payload, geom_raw)
            VALUES (%s, %s, %s::jsonb, ST_GeomFromWKB(%s, 4326))
            RETURNING ingest_id;
            """,
            (source_system, source_object, payload, psycopg2.Binary(geom_wkb)),
        )
        ingest_id = cur.fetchone()[0]
    conn.commit()
    return ingest_id

def call_validate_feature(conn, ingest_id: int):
    with conn.cursor() as cur:
        cur.execute("SELECT ingest.validate_feature(%s);", (ingest_id,))
        (ok,) = cur.fetchone()
    conn.commit()
    return ok
