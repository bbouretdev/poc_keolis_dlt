import os
import sys
import dlt
from dlt.sources.sql_database import sql_table

# -----------------------------------------------------------------------------
# 1. PARAMÈTRES DU JOB (INJECTÉS PAR LE POD KUBERNETES)
# -----------------------------------------------------------------------------
pipeline_id = os.environ.get("DLT_PIPELINE_ID", "postgres_to_adls_pipeline")
source_schema = os.environ.get("DLT_SOURCE_SCHEMA", "public")
source_table = os.environ.get("DLT_SOURCE_TABLE")

target_name = os.environ.get("DLT_TARGET_NAME", source_table)
target_path = os.environ.get("DLT_TARGET_PATH", "target-data")
backend = os.environ.get("DLT_BACKEND", "connectorx").lower()
chunk_size = int(os.environ.get("DLT_CHUNK_SIZE", "100000"))
write_strategy = os.getenv("DLT_WRITE_STRATEGY", "replace").lower()


def run_export_pipeline():
    if not source_table:
        print("❌ La variable DLT_SOURCE_TABLE est obligatoire pour ce Pod.")
        sys.exit(1)

    print(f"🚀 Démarrage de l'export DLT Native - Engine: {backend} - Strategy: {write_strategy}")
    print(f"📦 Source : {source_schema}.{source_table} -> Cible : {target_path}/{target_name}")

    # -------------------------------------------------------------------------
    # 2. PRÉPARATION DE LA RESSOURCE SOURCE POSTGRESQL
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

    # Renommage de la ressource/fichier cible si spécifié
    if target_name and target_name != source_table:
        print(f"✏️ Renommage de la ressource DLT : '{source_table}' -> '{target_name}'")
        resource = resource.with_name(target_name)

    # -------------------------------------------------------------------------
    # 3. EXÉCUTION DU PIPELINE DLT (DESTINATION FILESYSTEM)
    # -------------------------------------------------------------------------
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_id,
        destination="filesystem",
        dataset_name=target_path,
    )

    load_info = pipeline.run(
        resource,
        write_disposition=write_strategy,
        loader_file_format="parquet",
    )

    print("\n✅ Export DLT terminé avec succès !")
    print(pipeline.last_trace)
    print(load_info)


if __name__ == "__main__":
    run_export_pipeline()