import io
import os
import sys
import dlt
import pyarrow as pa
import pyarrow.parquet as pq
from dlt.sources.sql_database import sql_table, sql_database

# -----------------------------------------------------------------------------
# 1. RÉCUPÉRATION DES VARIABLES D'ENVIRONNEMENT (TRANSMISES PAR AIRFLOW)
# -----------------------------------------------------------------------------
pipeline_id = os.environ.get("DLT_PIPELINE_ID", "postgres_to_adls_pipeline")
source_schema = os.environ.get("DLT_SOURCE_SCHEMA", "public")
source_table = os.environ.get("DLT_SOURCE_TABLE")

# Nom de fichier/table cible personnalisé (Option 2)
target_name = os.environ.get("DLT_TARGET_NAME", source_table)

target_path = os.environ.get("DLT_TARGET_PATH", "target-data")  # Nom du conteneur Azure
backend = os.environ.get("DLT_BACKEND", "connectorx").lower()
chunk_size = int(os.environ.get("DLT_CHUNK_SIZE", "100000"))

IS_LOCAL_AZURITE = os.getenv("USE_AZURITE", "true").lower() == "true"


def run_export_pipeline():
    if not source_table:
        print("❌ La variable DLT_SOURCE_TABLE est obligatoire pour ce Pod.")
        sys.exit(1)

    mode_label = "Azurite Dev (SDK Direct)" if IS_LOCAL_AZURITE else "ADLS Gen2 Prod (DLT Native)"
    print(f"🚀 Démarrage de l'export DLT [{mode_label}] - Engine: {backend}...")
    print(f"📦 Source : {source_schema}.{source_table} -> Cible : {target_path}/{target_name}.parquet")

    # -------------------------------------------------------------------------
    # CONFIGURATION DES OPTIONS DE RÉFLEXION DU MOTEUR (CONNECTORX vs PYARROW)
    # -------------------------------------------------------------------------
    table_kwargs = {
        "table": source_table,
        "schema": source_schema,
        "backend": backend,
        "chunk_size": chunk_size,
    }
    database_kwargs = {
        "schema": source_schema,
        "table_names": [source_table],
        "backend": backend,
        "chunk_size": chunk_size,
    }

    # Si on demande PyArrow, on injecte la précision stricte PostgreSQL
    if backend == "pyarrow":
        table_kwargs["reflection_level"] = "full_with_precision"
        database_kwargs["reflection_level"] = "full_with_precision"

    # -------------------------------------------------------------------------
    # 2. MODE DEV LOCAL (AZURITE)
    # -------------------------------------------------------------------------
    if IS_LOCAL_AZURITE:
        from azure.storage.blob import BlobServiceClient

        conn_str = os.getenv(
            "AZURE_STORAGE_CONNECTION_STRING",
            (
                "DefaultEndpointsProtocol=http;"
                "AccountName=devstoreaccount1;"
                "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
                "BlobEndpoint=http://azurite:10000/devstoreaccount1;"
            ),
        )

        blob_service_client = BlobServiceClient.from_connection_string(
            conn_str, api_version="2020-08-04"
        )
        container_client = blob_service_client.get_container_client(target_path)
        try:
            container_client.create_container()
        except Exception:
            pass

        print(f"🔄 Export de la table '{source_schema}.{source_table}'...")

        # Utilisation de nos kwargs dynamiques (avec/sans full_with_precision)
        src_table = sql_table(**table_kwargs)

        arrow_tables = []
        for chunk in src_table:
            if chunk is not None:
                if isinstance(chunk, pa.Table):
                    arrow_tables.append(chunk)
                elif isinstance(chunk, pa.RecordBatch):
                    arrow_tables.append(pa.Table.from_batches([chunk]))
                elif isinstance(chunk, list) and len(chunk) > 0:
                    arrow_tables.append(pa.Table.from_pylist(chunk))
                else:
                    try:
                        arrow_tables.append(pa.Table.from_pandas(chunk))
                    except Exception:
                        pass

        if not arrow_tables:
            print(f"⚠️ Aucune donnée dans la table '{source_table}'. Export sauté.")
            return

        consolidated_table = pa.concat_tables(arrow_tables)
        parquet_buffer = io.BytesIO()
        pq.write_table(consolidated_table, parquet_buffer)
        parquet_buffer.seek(0)

        blob_name = f"{target_name}.parquet"
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(parquet_buffer.getvalue(), overwrite=True)
        print(f"✅ Table '{source_table}' ({consolidated_table.num_rows} lignes) -> Blob '{blob_name}' dans '{target_path}'")

    # -------------------------------------------------------------------------
    # 3. MODE PROD NATIVE (ADLS GEN2)
    # -------------------------------------------------------------------------
    else:
        postgres_source = sql_database(**database_kwargs)

        resource = postgres_source.resources[source_table]

        if target_name and target_name != source_table:
            print(f"✏️ Renommage de la ressource DLT : '{source_table}' -> '{target_name}'")
            resource = resource.with_name(target_name)

        pipeline = dlt.pipeline(
            pipeline_name=pipeline_id,
            destination="filesystem",
            dataset_name=target_path,
        )

        load_info = pipeline.run(
            resource,
            write_disposition="replace",
            loader_file_format="parquet",
        )

        print(pipeline.last_trace)
        print(load_info)


if __name__ == "__main__":
    run_export_pipeline()