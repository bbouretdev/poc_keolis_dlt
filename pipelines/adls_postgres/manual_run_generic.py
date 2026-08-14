import io
import os
import dlt
from dlt.sources.filesystem import filesystem, read_parquet

# Détection de l'environnement (par défaut Azurite pour tes tests locaux)
IS_LOCAL_AZURITE = os.getenv("USE_AZURITE", "true").lower() == "true"


if IS_LOCAL_AZURITE:
    # ------------------------------------------------------------------
    # 1. MODE DEV LOCAL (AZURITE)
    # Contourne le bug de signature MAC HMAC de la lib adlfs/fsspec
    # ------------------------------------------------------------------
    from azure.storage.blob import BlobServiceClient
    import pyarrow.parquet as pq

    @dlt.resource(selected=True)
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
        container_client = blob_service_client.get_container_client("source-data")

        for blob in container_client.list_blobs():
            if blob.name.endswith(".parquet"):
                blob_client = container_client.get_blob_client(blob.name)
                stream = blob_client.download_blob()
                table = pq.read_table(io.BytesIO(stream.readall()))
                yield table.to_pylist()

else:
    # ------------------------------------------------------------------
    # 2. MODE PROD (ADLS GEN2 100% DLT-NATIVE)
    # Utilise le pipe natif dlt qui tourne parfaitement sur Azure Cloud
    # ------------------------------------------------------------------
    @dlt.source
    def source_parquet_data():
        files = filesystem(
            bucket_url="az://source-data",
            file_glob="*.parquet",
        )
        return files | read_parquet()


def run_pipeline():
    mode_label = "Azurite Dev (SDK Direct)" if IS_LOCAL_AZURITE else "ADLS Gen2 Prod (DLT Native)"
    print(f"🚀 Démarrage du pipeline DLT [{mode_label}]...")

    pipeline = dlt.pipeline(
        pipeline_name="azurite_to_postgres",
        destination="postgres_dest",
        dataset_name="dlt",
    )

    load_info = pipeline.run(
        source_parquet_data(),
        table_name="users_test",
        write_disposition="replace",
    )

    print("\n✅ Pipeline exécuté avec succès !")
    print(load_info)


if __name__ == "__main__":
    run_pipeline()