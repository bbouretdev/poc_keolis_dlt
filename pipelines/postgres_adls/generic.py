import os
import sys
from datetime import datetime
import dlt
from dlt.sources.sql_database import sql_table

# -----------------------------------------------------------------------------
# 1. LECTURE STRICTE DES VARIABLES D'ENVIRONNEMENT
# -----------------------------------------------------------------------------
try:
    pipeline_id = os.environ["DLT_PIPELINE_ID"]
    source_schema = os.environ["DLT_SOURCE_SCHEMA"]
    source_table = os.environ["DLT_SOURCE_TABLE"]
    target_name = os.environ["DLT_TARGET_NAME"]
    target_path = os.environ["DLT_TARGET_PATH"]
    backend = os.environ["DLT_BACKEND"].lower()
    chunk_size = int(os.environ["DLT_CHUNK_SIZE"])
    write_strategy = os.environ["DLT_WRITE_STRATEGY"].lower()
    
    cursor_column = os.environ.get("DLT_CURSOR_COLUMN", "").strip()
    primary_key = os.environ.get("DLT_PRIMARY_KEY", "").strip()
except KeyError as e:
    print(f"❌ ERREUR CRITIQUE : La variable d'environnement {e} est absente.")
    sys.exit(1)


def run_export_pipeline():
    print(f"🚀 Démarrage export DLT Native - Engine: {backend} - Strategy: {write_strategy}")
    print(f"📦 Source : {source_schema}.{source_table} -> Cible : {target_path}/{target_name}.parquet")

    # -------------------------------------------------------------------------
    # 2. CONFIGURATION DU WATERMARK / INCRÉMENTALITÉ DLT
    # -------------------------------------------------------------------------
    incremental_param = None
    
    if write_strategy in ["append", "merge"] and cursor_column:
        # Watermark Minuit : bloque la borne haute à 00:00:00 du jour J
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        print(f"⏰ Activation Watermark DLT sur '{cursor_column}' (Borne haute : {end_date.isoformat()})")
        incremental_param = dlt.sources.incremental(
            cursor_path=cursor_column,
            initial_value=datetime(1970, 1, 1),
            end_value=end_date,
        )

    # -------------------------------------------------------------------------
    # 3. PRÉPARATION DE LA RESSOURCE SOURCE POSTGRESQL
    # -------------------------------------------------------------------------
    dlt_kwargs = {
        "table": source_table,
        "schema": source_schema,
        "backend": backend,
        "chunk_size": chunk_size,
        "incremental": incremental_param,
    }

    if backend == "pyarrow":
        dlt_kwargs["reflection_level"] = "full_with_precision"

    resource = sql_table(**dlt_kwargs)

    if target_name != source_table:
        print(f"✏️ Renommage de la ressource DLT : '{source_table}' -> '{target_name}'")
        resource = resource.with_name(target_name)

    if write_strategy == "merge":
        if not primary_key:
            print("❌ ERREUR : 'primary_key' est obligatoire pour la stratégie 'merge'.")
            sys.exit(1)
        print(f"🔑 Application de la clé primaire pour le Merge : {primary_key}")
        resource.apply_hints(primary_key=primary_key)

    # -------------------------------------------------------------------------
    # 4. EXÉCUTION DU PIPELINE DLT
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