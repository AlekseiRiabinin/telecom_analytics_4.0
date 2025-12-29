import os
from dotenv import load_dotenv
from dataclasses import dataclass

load_dotenv()


@dataclass
class Settings:
    # --- Database ---
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: str = os.getenv("DB_PORT", "5435")
    db_name: str = os.getenv("DB_NAME", "cre_db")
    db_user: str = os.getenv("DB_USER", "cre_user")
    db_password: str = os.getenv("DB_PASSWORD", "cre_password")

    @property
    def db_dsn(self) -> str:
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    # --- Data source URLs / paths ---
    dld_parcels_url: str = os.getenv("DLD_PARCELS_URL", "")
    dm_zoning_url: str = os.getenv("DM_ZONING_URL", "")
    rta_gtfs_url: str = os.getenv("RTA_GTFS_URL", "")
    developer_footprints_dir: str = os.getenv("DEV_FOOTPRINTS_DIR", "./data/developer")
    listings_api_url: str = os.getenv("LISTINGS_API_URL", "")

settings = Settings()
