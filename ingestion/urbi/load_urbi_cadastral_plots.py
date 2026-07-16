"""
Loader for URBI cadastral plots.
Normalizes district folder names (underscores → spaces) and maps them to real URBI district IDs.
After loading raw data into ingest.raw_urbi_cadastral_plots,
it triggers normalization into core and spatial layers.
"""

from pathlib import Path
from typing import Any

import json
import psycopg2
from psycopg2.extensions import (
    cursor as PsyCursor,
    connection as PsyConnection,
)

DATA_ROOT: Path = Path("ingestion/scrapers/data/urbi_cadastral_plots")
DISTRICTS_FILE: Path = Path("ingestion/scrapers/data/urbi_districts.json")


def load_district(
    cursor: PsyCursor,
    district_name: str,
    district_id: str,
    plots: list[dict[str, Any]],
) -> None:
    """
    Calls the Postgres loader function to insert raw URBI cadastral plots.
    """
    cursor.execute(
        """
        SELECT ingest.load_urbi_cadastral_plots(%s, %s, %s::jsonb)
        """,
        (district_name, district_id, json.dumps(plots)),
    )


def load_district_map() -> dict[str, str]:
    """
    Loads urbi_districts.json and builds a mapping:
        district_name → district_id.
    """
    if not DISTRICTS_FILE.exists():
        raise FileNotFoundError(f"Missing districts file: {DISTRICTS_FILE}")

    districts: list[dict[str, Any]] = json.loads(DISTRICTS_FILE.read_text())

    mapping: dict[str, str] = {}

    for d in districts:
        name = d.get("name")
        district_id = d.get("id")

        if not name or not district_id:
                continue

        mapping[name] = str(district_id)

    return mapping


def normalize_folder_name(folder_name: str) -> str:
    """
    Converts folder names like 'Al_Awir_1' → 'Al Awir 1'.
    """
    return folder_name.replace("_", " ").strip()


def run_normalization(cursor: PsyCursor) -> None:
    """
    Calls SQL functions to populate core and spatial layers.
    """
    print("Running core.load_cadastral_plots()...")
    cursor.execute("SELECT core.load_cadastral_plots();")

    print("Running spatial.load_cadastral_plots_geom()...")
    cursor.execute("SELECT spatial.load_cadastral_plots_geom();")

    print("Normalization completed.")


def main() -> None:
    # Load district name → id mapping
    district_map: dict[str, str] = load_district_map()

    conn: PsyConnection = psycopg2.connect(
        dbname="cre_db",
        user="cre_user",
        password="cre_password",
        host="localhost",
        port=5435,
    )
    conn.autocommit = True
    cursor: PsyCursor = conn.cursor()

    cursor.execute("TRUNCATE ingest.raw_urbi_cadastral_plots;")

    print("=== Loading raw URBI cadastral plots into ingest schema ===")

    for district_dir in DATA_ROOT.iterdir():
        if not district_dir.is_dir():
            continue

        folder_name: str = district_dir.name
        district_name: str = normalize_folder_name(folder_name)

        district_id: str = district_map.get(district_name, district_name)

        plots_file: Path = district_dir / "plots.json"
        if not plots_file.exists():
            print(f"Skipping {folder_name}: no plots.json")
            continue

        plots: list[dict[str, Any]] = json.loads(plots_file.read_text())

        print(
            f"Loading {folder_name} → '{district_name}' "
            f"(district_id={district_id}) with {len(plots)} plots..."
        )

        load_district(cursor, district_name, district_id, plots)

    print("=== Raw loading complete ===")

    print("=== Starting normalization into core + spatial ===")
    run_normalization(cursor)

    cursor.close()
    conn.close()

    print("=== Pipeline complete ===")


if __name__ == "__main__":
    main()
