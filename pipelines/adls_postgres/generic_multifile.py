import io
import os
import dlt
from dlt.sources.filesystem import filesystem, read_parquet

USE_AZURITE = os.getenv("USE_AZURITE", "true").lower() == "true"

CONTAINER_NAME = os.getenv("DLT_AZURE_CONTAINER", "source-data")
# Support de plusieurs patterns séparés par des virgules
FILE_GLOBS_RAW = os.getenv("DLT_FILE_GLOB", "*.parquet")
PIPELINE_ID = os.getenv("DLT_PIPELINE_ID", "adls_multi_to_postgres")
TARGET_SCHEMA = os.getenv("DLT_TARGET_SCHEMA", "dlt")
WRITE_STRATEGY = os.getenv("DLT_WRITE_STRATEGY", "replace").lower()

FILE_GLOBS = [g.strip() for g in FILE_GLOBS_RAW.split(",") if g.strip()]


def get_table_name_from_blob(blob_name: str) -> str:
    """Dérive un nom de table Postgres propre à partir du nom du fichier/chemin."""
    filename = os.path.basename(blob_name)
    base_name = os.path.splitext(filename)[0]
    return base_name.replace("-", "_").replace(" ", "_").lower()


if USE_AZURITE:
    # ------------------------------------------------------------------
    # 1. MODE DEV LOCAL (AZURITE - MULTI FICHIERS)
    # ------------------------------------------------------------------
    from azure.storage.blob import BlobServiceClient
    import pyarrow.parquet as pq

    @dlt.source
    def source_parquet_data():
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
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        resources = []
        try:
            all_blobs = list(container_client.list_blobs())
        except Exception as e:
            print(f"⚠️ Erreur d'accès au conteneur '{CONTAINER_NAME}': {e}")
            return []

        # Pour chaque blob correspondant à un des patterns
        for blob in all_blobs:
            if blob.name.endswith(".parquet"):
                blob_name = blob.name
                table_name = get_table_name_from_blob(blob_name)

                # Fonction génératrice isolée par fichier
                def make_generator(b_name=blob_name):
                    def item_generator():
                        blob_client = container_client.get_blob_client(b_name)
                        stream = blob_client.download_blob()
                        table = pq.read_table(io.BytesIO(stream.readall()))
                        yield table.to_pylist()
                    return item_generator

                # Déclaration d'une ressource DLT nommée d'après le fichier
                res = dlt.resource(
                    make_generator(),
                    name=table_name,
                    selected=True,
                )
                resources.append(res)

        if not resources:
            print(f"⚠️ Aucun fichier parquet trouvé dans Azurite '{CONTAINER_NAME}'")

        return resources

else:
    # ------------------------------------------------------------------
    # 2. MODE PROD (ADLS GEN2 100% DLT-NATIVE MULTI-PATTERNS)
    # ------------------------------------------------------------------
    @dlt.source
    def source_parquet_data():
        resources = []
        for glob_pattern in FILE_GLOBS:
            table_name = get_table_name_from_blob(glob_pattern)
            files = filesystem(
                bucket_url=f"az://{CONTAINER_NAME}",
                file_glob=glob_pattern,
            )
            resources.append((files | read_parquet()).with_name(table_name))
        return resources


def run_pipeline():
    mode_label = f"Azurite Local ({CONTAINER_NAME})" if USE_AZURITE else f"ADLS Gen2 Prod ({CONTAINER_NAME})"
    print(f"🚀 Démarrage du pipeline Multi-Ingestion [{mode_label}] -> Schema: {TARGET_SCHEMA}")

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_ID,
        destination="postgres_dest",
        dataset_name=TARGET_SCHEMA,
    )

    load_info = pipeline.run(
        source_parquet_data(),
        write_disposition=WRITE_STRATEGY,
    )

    print("\n✅ Multi-Ingestion exécutée avec succès !")
    print(load_info)


if __name__ == "__main__":
    run_pipeline()