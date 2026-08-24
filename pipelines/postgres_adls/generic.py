import os
import sys
import dlt
from dlt.sources.sql_database import sql_table

# -----------------------------------------------------------------------------
# 1. RÉCUPÉRATION STRICTE DES VARIABLES D'ENVIRONNEMENT (SANS DEFAULT)
# -----------------------------------------------------------------------------
try:
    pipeline_id = os.environ["DLT_PIPELINE_ID"]
    source_schema = os.environ["DLT_SOURCE_SCHEMA"]
    source_table = os.environ["DLT_SOURCE_TABLE"]
    target_name = os.environ["DLT_TARGET_NAME"]  # ex: "ventes/2026/commandes_export"
    target_path = os.environ["DLT_TARGET_PATH"]
    backend = os.environ["DLT_BACKEND"].lower()
    chunk_size = int(os.environ["DLT_CHUNK_SIZE"])
    write_strategy = os.environ["DLT_WRITE_STRATEGY"].lower()
except KeyError as e:
    print(f"❌ ERREUR CRITIQUE : La variable d'environnement {e} est absente.")
    sys.exit(1)


def run_export_pipeline():
    print(f"🚀 Démarrage de l'export DLT Native - Engine: {backend} - Strategy: {write_strategy}")
    print(f"📦 Source : {source_schema}.{source_table} -> Cible : {target_path}/{target_name}.parquet")

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

    # DLT gère les slashes dans with_name() en créant l'arborescence de sous-dossiers dans ADLS
    if target_name != source_table:
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