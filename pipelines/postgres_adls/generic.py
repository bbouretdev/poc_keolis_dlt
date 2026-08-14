import io
import os
import dlt
from dlt.sources.sql_database import sql_table

# Paramètres du pipeline via variables d'environnement transmis par le Pod
pipeline_id = os.environ.get("DLT_PIPELINE_ID")
source_schema = os.environ.get("DLT_SOURCE_SCHEMA")
source_table = os.environ.get("DLT_SOURCE_TABLE")
target_path = os.environ.get("DLT_TARGET_PATH")  # Nom du conteneur/dataset
target_filename = os.environ.get("DLT_TARGET_FILENAME")
backend = os.environ.get("DLT_BACKEND")
chunk_size = int(os.environ.get("DLT_CHUNK_SIZE"))

# Détection de l'environnement (par défaut Azurite)
IS_LOCAL_AZURITE = os.getenv("USE_AZURITE").lower() == "true"


def run_export_pipeline():
    mode_label = "Azurite Dev (SDK Direct)" if IS_LOCAL_AZURITE else "ADLS Gen2 Prod (DLT Native)"
    print(f"🚀 Démarrage du pipeline DLT [{mode_label}]...")

    # 1. Définition de la source SQL (commune aux deux modes)
    # DLT lira la connexion Postgres depuis SOURCES__SQL_DATABASE__CREDENTIALS
    source = sql_table(
        table=source_table,
        schema=source_schema,
        backend=backend,
        chunk_size=chunk_size,
    )

    if IS_LOCAL_AZURITE:
        # ------------------------------------------------------------------
        # WORKAROUND DEV LOCAL (AZURITE)
        # ------------------------------------------------------------------
        from azure.storage.blob import BlobServiceClient
        import pyarrow as pa
        import pyarrow.parquet as pq

        print(f"📦 Extraction de la table SQL '{source_schema}.{source_table}'...")
        
        batches = []
        for chunk in source():
            if chunk:
                batches.append(pa.RecordBatch.from_pylist(chunk))

        if not batches:
            print("⚠️ Aucune donnée trouvée dans la table source.")
            return

        arrow_table = pa.Table.from_batches(batches)
        parquet_buffer = io.BytesIO()
        pq.write_table(arrow_table, parquet_buffer)
        parquet_buffer.seek(0)

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

        blob_name = f"{target_filename}.parquet" if not target_filename.endswith(".parquet") else target_filename
        blob_client = container_client.get_blob_client(blob_name)

        blob_client.upload_blob(parquet_buffer.getvalue(), overwrite=True)
        print(f"✅ Fichier '{blob_name}' téléversé avec succès dans le conteneur Azurite '{target_path}' ({arrow_table.num_rows} lignes) !")

    else:
        # ------------------------------------------------------------------
        # PIPELINE DLT NATIVE (PROD / ADLS GEN2)
        # ------------------------------------------------------------------
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_id,
            destination="filesystem",
            dataset_name=target_path,
        )

        load_info = pipeline.run(
            source,
            table_name=target_filename,
            write_disposition="replace",
            loader_file_format="parquet",
        )

        print(pipeline.last_trace)
        print(load_info)


if __name__ == "__main__":
    run_export_pipeline()