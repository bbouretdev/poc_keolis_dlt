import io
import os
import sys
import dlt
from dlt.sources.filesystem import filesystem, read_parquet

# 1. Variables d'environnement pour le mode local / prod
USE_AZURITE = os.getenv("USE_AZURITE", "false").lower() == "true"

# 2. Paramètres DLT transmis par le Pod
CONTAINER_NAME = os.getenv("DLT_AZURE_CONTAINER", "source-data")
FILE_GLOB = os.getenv("DLT_FILE_GLOB", "*.parquet")
PIPELINE_ID = os.getenv("DLT_PIPELINE_ID", "adls_to_postgres_pipeline")
TARGET_SCHEMA = os.getenv("DLT_TARGET_SCHEMA", "dlt")
TARGET_TABLE = os.getenv("DLT_TARGET_TABLE", "raw_data")
WRITE_STRATEGY = os.getenv("DLT_WRITE_STRATEGY", "replace").lower()


if USE_AZURITE:
    # ------------------------------------------------------------------
    # 1. MODE DEV LOCAL (AZURITE)
    # Contourne le bug de signature MAC HMAC de la lib adlfs/fsspec
    # ------------------------------------------------------------------
    from azure.storage.blob import BlobServiceClient
    import pyarrow.parquet as pq

    @dlt.resource(name=TARGET_TABLE, selected=True)
    def source_parquet_data():
        conn_str = os.getenv(
            "AZURE_STORAGE_CONNECTION_STRING",
            (
                "DefaultEndpointsProtocol=http;"
                "AccountName=devstoreaccount1;"
                "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
                "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
            ),
        )

        # Force api_version="2020-08-04" pour compatibilité stricte Azurite
        blob_service_client = BlobServiceClient.from_connection_string(
            conn_str, api_version="2020-08-04"
        )
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        found_blobs = False
        for blob in container_client.list_blobs():
            if blob.name.endswith(".parquet"):
                found_blobs = True
                blob_client = container_client.get_blob_client(blob.name)
                stream = blob_client.download_blob()
                table = pq.read_table(io.BytesIO(stream.readall()))
                yield table.to_pylist()
        
        if not found_blobs:
            print(f"⚠️ Aucun fichier parquet trouvé dans le conteneur Azurite '{CONTAINER_NAME}'")

else:
    # ------------------------------------------------------------------
    # 2. MODE PROD (ADLS GEN2 100% DLT-NATIVE)
    # ------------------------------------------------------------------
    @dlt.source
    def source_parquet_data():
        files = filesystem(
            bucket_url=f"az://{CONTAINER_NAME}",
            file_glob=FILE_GLOB,
        )
        return files | read_parquet()


def run_pipeline():
    mode_label = f"Azurite Local ({CONTAINER_NAME})" if USE_AZURITE else f"ADLS Gen2 Prod ({CONTAINER_NAME})"
    print(f"🚀 Démarrage du pipeline DLT [{mode_label}] -> Table Cible: {TARGET_SCHEMA}.{TARGET_TABLE}")

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_ID,
        destination="postgres_dest",
        dataset_name=TARGET_SCHEMA,
    )

    load_info = pipeline.run(
        source_parquet_data(),
        table_name=TARGET_TABLE,
        write_disposition=WRITE_STRATEGY,
    )

    print("\n✅ Pipeline DLT exécuté avec succès !")
    print(load_info)


if __name__ == "__main__":
    run_pipeline()