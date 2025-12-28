import os
from dataclasses import dataclass


@dataclass
class Settings:
    db_dsn: str = os.getenv("DB_DSN", "postgresql://postgres:secret@localhost:5432/realestate")

    dld_parcels_url: str = os.getenv("DLD_PARCELS_URL", "")
    dm_zoning_url: str = os.getenv("DM_ZONING_URL", "")
    rta_gtfs_url: str = os.getenv("RTA_GTFS_URL", "")
    developer_footprints_dir: str = os.getenv("DEV_FOOTPRINTS_DIR", "./data/developer")
    listings_api_url: str = os.getenv("LISTINGS_API_URL", "")

settings = Settings()
