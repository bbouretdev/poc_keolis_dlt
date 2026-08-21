import os
import dlt
from dlt.sources.sql_database import sql_table

# Variables d'environnement transmises par la Factory Airflow
PIPELINE_ID = os.environ.get("DLT_PIPELINE_ID", "postgres_to_adls")
SOURCE_SCHEMA = os.environ["DLT_SOURCE_SCHEMA"]
SOURCE_TABLE = os.environ["DLT_SOURCE_TABLE"]
TARGET_CONTAINER = os.environ["DLT_TARGET_PATH"]
TARGET_FILENAME = os.environ["DLT_TARGET_FILENAME"]
BACKEND = os.environ.get("DLT_BACKEND", "connectorx")
CHUNK_SIZE = int(os.environ.get("DLT_CHUNK_SIZE", "100000"))


def run_export_pipeline():
    print(f"🚀 Export DLT Native : Postgres ({SOURCE_SCHEMA}.{SOURCE_TABLE}) -> az://{TARGET_CONTAINER}/{TARGET_FILENAME}.parquet")

    # 1. Source SQL
    source = sql_table(
        table=SOURCE_TABLE,
        schema=SOURCE_SCHEMA,
        backend=BACKEND,
        chunk_size=CHUNK_SIZE,
    )

    # 2. Pipeline DLT vers la destination 'filesystem' (Azure Blob/ADLS Gen2)
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_ID,
        destination="filesystem",
        dataset_name=TARGET_CONTAINER,
    )

    load_info = pipeline.run(
        source,
        table_name=TARGET_FILENAME,
        write_disposition="replace",
        loader_file_format="parquet",
    )

    print("✅ Export terminé avec succès !")
    print(load_info)


if __name__ == "__main__":
    run_export_pipeline()