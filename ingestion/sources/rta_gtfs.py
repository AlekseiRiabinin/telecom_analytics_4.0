import zipfile
import pandas as pd
from pathlib import Path
from ingestion.config.settings import settings


def load_rta_stops():
    zpath = Path(settings.rta_gtfs_url)
    with zipfile.ZipFile(zpath, "r") as zf:
        with zf.open("stops.txt") as f:
            df = pd.read_csv(f)
    return df
