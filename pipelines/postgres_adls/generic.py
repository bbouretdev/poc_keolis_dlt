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
    source_table = os.environ["DLT_SOURCE_TABLE"]
    target_name = os.environ["DLT_TARGET_NAME"]  # ex: "referentiel/articles_export"
    backend = os.environ["DLT_BACKEND"].lower()
    chunk_size = int(os.environ["DLT_CHUNK_SIZE"])
    write_strategy = os.environ["DLT_WRITE_STRATEGY"].lower()
except KeyError as e:
    print(f"❌ ERREUR CRITIQUE : La variable d'environnement {e} est absente.")
    sys.exit(1)


def run_export_pipeline():
    print(f"🚀 Démarrage export DLT Native - Engine: {backend} - Strategy: {write_strategy}")
    print(f"📦 Source : {source_schema}.{source_table} -> Cible : {target_name}")

    # -------------------------------------------------------------------------
    # 2. DÉPARATION DU CHEMIN (DATASET) ET DU NOM DE TABLE
    # -------------------------------------------------------------------------
    # Si target_name contient des slashes (ex: "referentiel/articles_export"),
    # dataset_folder devient "referentiel" et final_table_name devient "articles_export".
    if "/" in target_name:
        dataset_folder, final_table_name = target_name.rsplit("/", 1)
    else:
        dataset_folder = None
        final_table_name = target_name

    # -------------------------------------------------------------------------
    # 3. PRÉPARATION DE LA RESSOURCE SOURCE POSTGRESQL
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

    if final_table_name != source_table:
        print(f"✏️ Nom de la ressource DLT : '{source_table}' -> '{final_table_name}'")
        resource = resource.with_name(final_table_name)

    # -------------------------------------------------------------------------
    # 4. EXÉCUTION DU PIPELINE DLT
    # -------------------------------------------------------------------------
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_id,
        destination="filesystem",
        dataset_name=dataset_folder,  # Fixe le sous-dossier exact (ex: "referentiel" ou "billetique/ventes")
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