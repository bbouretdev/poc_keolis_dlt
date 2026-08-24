import os
import sys
import dlt
from dlt.sources.sql_database import sql_table

# -----------------------------------------------------------------------------
# 1. LECTURE STRICTE DES VARIABLES D'ENVIRONNEMENT
# -----------------------------------------------------------------------------
try:
    pipeline_id = os.environ["DLT_PIPELINE_ID"]
    source_schema = os.environ["DLT_SOURCE_SCHEMA"]
    dataset_name = os.environ["DLT_DATASET_NAME"]
    source_table = os.environ["DLT_SOURCE_TABLE"]
    target_name = os.environ["DLT_TARGET_NAME"]  # ex: "ventes/commandes_export"
    backend = os.environ["DLT_BACKEND"].lower()
    chunk_size = int(os.environ["DLT_CHUNK_SIZE"])
    write_strategy = os.environ["DLT_WRITE_STRATEGY"].lower()
except KeyError as e:
    print(f"❌ ERREUR CRITIQUE : La variable d'environnement {e} est absente.")
    sys.exit(1)


def run_export_pipeline():
    print(f"🚀 Démarrage export DLT Native - Engine: {backend} - Strategy: {write_strategy}")
    print(f"📦 Source : {source_schema}.{source_table} -> Cible : {dataset_name}/{target_name}")

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

    if target_name != source_table:
        print(f"✏️ Renommage de la ressource DLT : '{source_table}' -> '{target_name}'")
        resource = resource.with_name(target_name)

    # -------------------------------------------------------------------------
    # 3. EXÉCUTION DU PIPELINE DLT
    # -------------------------------------------------------------------------
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_id,
        destination="filesystem",
        dataset_name=dataset_name,  # Fixe le domaine racine (ex: "billetique")
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