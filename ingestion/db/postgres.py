import logging
import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PgConnection
from contextlib import contextmanager
from typing import Generator, Optional
from ingestion.config.settings import settings

logger = logging.getLogger(__name__)


@contextmanager
def get_conn() -> Generator[PgConnection, None, None]:
    """
    Open a PostgreSQL connection using environment-driven DSN.
    Ensures clean open/close lifecycle.
    """
    conn: Optional[PgConnection] = None
    try:
        logger.debug(f"Connecting to Postgres at {settings.db_host}:{settings.db_port}/{settings.db_name}")
        conn = psycopg2.connect(settings.db_dsn)
        yield conn

    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

    finally:
        if conn:
            conn.close()


def insert_raw_feature(
    conn: PgConnection,
    source_system: str,
    source_object: Optional[str],
    payload: str,
    geom_wkb: Optional[bytes],
    checksum: Optional[str] = None,
    is_processed: bool = False,
) -> int:
    """
    Insert a raw feature into ingest.raw_features.
    Geometry is inserted as WKB (SRID 4326) if provided.
    Optional fields: checksum, is_processed.
    received_at is auto-populated by Postgres.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingest.raw_features 
                    (source_system, source_object, payload, geom_raw, checksum, is_processed)
                VALUES 
                    (
                        %s, 
                        %s, 
                        %s::jsonb,
                        CASE 
                            WHEN %s IS NOT NULL 
                            THEN ST_GeomFromWKB(%s, 4326) 
                            ELSE NULL 
                        END,
                        %s,
                        %s
                    )
                RETURNING ingest_id;
                """,
                (
                    source_system,
                    source_object,
                    payload,
                    geom_wkb,
                    psycopg2.Binary(geom_wkb) if geom_wkb else None,
                    checksum,
                    is_processed,
                ),
            )

            result = cur.fetchone()
            if result is None:
                raise ValueError("No ingest_id returned from INSERT")

            ingest_id = result[0]

        conn.commit()
        return ingest_id

    except Exception as e:
        logger.error(
            f"Failed to insert raw feature "
            f"(source_system={source_system}, source_object={source_object}): {e}"
        )
        conn.rollback()
        raise


def insert_raw_features_batch(
    conn: PgConnection,
    rows: list[tuple[str, str | None, str, bytes | None, str, bool]]
) -> list[int]:
    """
    Batch insert raw features.
    rows = [
        (source_system, source_object, payload, geom_wkb, checksum, is_processed),
        ...
    ]
    Returns list of ingest_ids.
    """
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO ingest.raw_features
                    (source_system, source_object, payload, geom_raw, checksum, is_processed)
                VALUES %s
                RETURNING ingest_id;
                """,
                [
                    (
                        r[0],  # source_system
                        r[1],  # source_object
                        r[2],  # payload
                        psycopg2.Binary(r[3]) if r[3] else None,
                        r[4],  # checksum
                        r[5],  # is_processed
                    )
                    for r in rows
                ],
                template="""
                    (%s, %s, %s::jsonb,
                     CASE WHEN %s IS NOT NULL THEN ST_GeomFromWKB(%s, 4326) ELSE NULL END,
                     %s, %s)
                """,
            )

            ingest_ids = [row[0] for row in cur.fetchall()]

        conn.commit()
        return ingest_ids

    except Exception as e:
        logger.error(f"Batch insert failed: {e}")
        conn.rollback()
        raise


def call_validate_feature(conn: PgConnection, ingest_id: int):
    """
    Call ingest.validate_feature(ingest_id) and return the result.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT ingest.validate_feature(%s);", (ingest_id,))
            result = cur.fetchone()
            if result is None:
                raise ValueError("validate_feature returned no result")
            (ok,) = result

        conn.commit()
        return ok

    except Exception as e:
        logger.error(f"Validation failed for ingest_id={ingest_id}: {e}")
        conn.rollback()
        raise
