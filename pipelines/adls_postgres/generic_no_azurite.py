import os
import dlt
from dlt.sources.filesystem import filesystem, read_parquet

# Variables d'environnement transmises par la Factory Airflow
CONTAINER_NAME = os.environ["DLT_AZURE_CONTAINER"]
FILE_GLOB = os.environ.get("DLT_FILE_GLOB", "*.parquet")
PIPELINE_ID = os.environ.get("DLT_PIPELINE_ID", "adls_to_postgres")
TARGET_SCHEMA = os.environ.get("DLT_TARGET_SCHEMA", "dlt")
TARGET_TABLE = os.environ.get("DLT_TARGET_TABLE", "raw_data")
WRITE_STRATEGY = os.environ.get("DLT_WRITE_STRATEGY", "replace").lower()


@dlt.source
def adls_parquet_source():
    """Lit les fichiers Parquet directement depuis le conteneur ADLS Gen2."""
    files = filesystem(
        bucket_url=f"az://{CONTAINER_NAME}",
        file_glob=FILE_GLOB,
    )
    return files | read_parquet()


def run_pipeline():
    print(f"🚀 Ingestion DLT Native : az://{CONTAINER_NAME}/{FILE_GLOB} -> Postgres ({TARGET_SCHEMA}.{TARGET_TABLE})")

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_ID,
        destination="postgres_dest",
        dataset_name=TARGET_SCHEMA,
    )

    load_info = pipeline.run(
        adls_parquet_source(),
        table_name=TARGET_TABLE,
        write_disposition=WRITE_STRATEGY,
    )

    print("✅ Pipeline exécuté avec succès !")
    print(load_info)


if __name__ == "__main__":
    run_pipeline()