from ingestion.pipelines.osm_gpkg_pipeline import DubaiGpkgPipeline
from ingestion.config.datasets import DATASETS


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, help="Dataset key")
    args = parser.parse_args()

    args_dict = vars(args)
    dataset_key = args_dict["dataset"]

    if dataset_key not in DATASETS:
        raise ValueError(
            f"Unknown dataset '{dataset_key}'. Available: {', '.join(DATASETS.keys())}"
        )

    cfg = DATASETS[dataset_key]

    pipeline = DubaiGpkgPipeline(
        path=cfg["path"],
        source_system=cfg["source_system"],
        min_area=cfg["min_area"],
    )

    pipeline.run()


# python3 -m ingestion.scripts.run_gpkg_ingest --dataset buildings
# python3 -m ingestion.scripts.run_gpkg_ingest --dataset roads
# python3 -m ingestion.scripts.run_gpkg_ingest --dataset landuse
# python3 -m ingestion.scripts.run_gpkg_ingest --dataset pois
