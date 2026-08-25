import os
import sys
import dlt
from dlt.sources.sql_database import sql_table

# -----------------------------------------------------------------------------
# 1. LECTURE DES VARIABLES D'ENVIRONNEMENT
# -----------------------------------------------------------------------------
try:
    pipeline_id = os.environ["DLT_PIPELINE_ID"]
    source_schema = os.environ["DLT_SOURCE_SCHEMA"]
    dataset_name = os.environ["DLT_DATASET_NAME"]
    source_table = os.environ["DLT_SOURCE_TABLE"]
    target_name = os.environ["DLT_TARGET_NAME"]
    partition_col = os.environ.get("DLT_PARTITION_COL", "").strip()
    backend = os.environ["DLT_BACKEND"].lower()
    chunk_size = int(os.environ["DLT_CHUNK_SIZE"])
    write_strategy = os.environ["DLT_WRITE_STRATEGY"].lower()
except KeyError as e:
    print(f"❌ ERREUR CRITIQUE : Variable {e} absente.")
    sys.exit(1)


def run_export_pipeline():
    print(f"🚀 Export DLT - Strategy: {write_strategy}")
    print(f"📦 Source : {source_schema}.{source_table} -> Cible : {dataset_name}/{target_name}")
    if partition_col:
        print(f"📅 Partitionnement activé sur la colonne : {partition_col}")

    # -------------------------------------------------------------------------
    # 2. PRÉPARATION DE LA RESSOURCE SOURCE
    # -------------------------------------------------------------------------
    dlt_kwargs = {
        "table": source_table,
        "schema": source_schema,
        "backend": backend,
        "chunk_size": chunk_size,
    }

    if backend == "pyarrow":
        dlt_kwargs["reflection_level"] = "full_with_precision"

    resource = sql_table(**dlt_kwargs)

    # Renommage de la ressource si un chemin cible spécifique est fourni
    if target_name != source_table:
        resource = resource.with_name(target_name)

    resource.apply_hints(
        write_disposition=write_strategy,
        file_format="parquet",
    )

    # -------------------------------------------------------------------------
    # 3. EXECUTION DU PIPELINE
    # DLT lit destination="filesystem" et résout automatiquement
    # DESTINATION__FILESYSTEM__BUCKET_URL et DESTINATION__FILESYSTEM__LAYOUT
    # -------------------------------------------------------------------------
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_id,
        destination="filesystem",
        dataset_name=dataset_name,
    )

    load_info = pipeline.run(resource)

    print("\n✅ Export terminé avec succès !")
    print(load_info)


if __name__ == "__main__":
    run_export_pipeline()