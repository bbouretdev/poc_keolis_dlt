import os
import dlt
from dlt.sources.sql_database import sql_database

PIPELINE_ID = os.environ.get("DLT_PIPELINE_ID", "postgres_multi_to_adls")
SOURCE_SCHEMA = os.environ["DLT_SOURCE_SCHEMA"]
# Liste de tables séparées par des virgules (ex: "orders,items,customers")
SOURCE_TABLES_RAW = os.environ.get("DLT_SOURCE_TABLES", "*")
TARGET_CONTAINER = os.environ["DLT_TARGET_PATH"]
BACKEND = os.environ.get("DLT_BACKEND", "connectorx")
CHUNK_SIZE = int(os.environ.get("DLT_CHUNK_SIZE", "100000"))


def run_export_pipeline():
    print(f"🚀 Export Multi-Tables : Postgres ({SOURCE_SCHEMA}.{SOURCE_TABLES_RAW}) -> az://{TARGET_CONTAINER}/")

    # Parsing de la liste des tables
    if SOURCE_TABLES_RAW == "*":
        table_names = None  # None = dlt exporte TOUTES les tables du schéma
    else:
        table_names = [t.strip() for t in SOURCE_TABLES_RAW.split(",") if t.strip()]

    # 1. Source SQL Multi-Tables (Création de la source globale)
    source = sql_database(
        schema=SOURCE_SCHEMA,
        table_names=table_names,
        backend=BACKEND,
        chunk_size=CHUNK_SIZE,
    )

    # 2. Pipeline DLT vers Filesystem (ADLS Gen2)
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_ID,
        destination="filesystem",
        dataset_name=TARGET_CONTAINER,
    )

    # DLT va extraire chaque table et écrire un dossier/fichier Parquet par table !
    load_info = pipeline.run(
        source,
        write_disposition="replace",
        loader_file_format="parquet",
    )

    print("✅ Export multi-tables terminé avec succès !")
    print(load_info)


if __name__ == "__main__":
    run_export_pipeline()