from prefect import flow, task
from ingestion.pipelines.dld_pipeline import DLDParcelsPipeline
from ingestion.pipelines.dm_zoning_pipeline import DMZoningPipeline
from ingestion.pipelines.rta_pipeline import RTAGTFSPipeline
# later: developer_pipeline, listings_pipeline


@task
def run_dld():
    DLDParcelsPipeline().run()

@task
def run_dm_zoning():
    DMZoningPipeline().run()

@task
def run_rta():
    RTAGTFSPipeline().run()

@flow(name="dubai_ingestion_daily")
def dubai_ingestion_daily():
    dld = run_dld()
    dm = run_dm_zoning(wait_for=[dld])
    rta = run_rta(wait_for=[dm])
    # you can add developer + listings here, with dependencies if needed

if __name__ == "__main__":
    dubai_ingestion_daily()
