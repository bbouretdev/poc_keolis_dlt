import io
import os
import dlt
from dlt.sources.filesystem import filesystem, read_parquet

USE_AZURITE = os.getenv("USE_AZURITE", "true").lower() == "true"

CONTAINER_NAME = os.getenv("DLT_AZURE_CONTAINER", "source-data")
FILE_MAPPING_RAW = os.getenv("DLT_FILE_GLOB", "*.parquet")
PIPELINE_ID = os.getenv("DLT_PIPELINE_ID", "adls_to_postgres_pipeline")
TARGET_SCHEMA = os.getenv("DLT_TARGET_SCHEMA", "dlt")
WRITE_STRATEGY = os.getenv("DLT_WRITE_STRATEGY", "replace").lower()

# Taille des lots pour le streaming (par défaut 10 000 lignes)
CHUNK_SIZE = int(os.getenv("DLT_CHUNK_SIZE", "10000"))


def parse_file_mapping(raw_input: str) -> dict:
    """
    Parse la chaîne transmise et retourne un dict {glob_pattern: target_table_name_or_None}.
    Exexmples acceptés :
      - "items.parquet:items, orders.parquet:client_orders"
      - "*.parquet"
    """
    mapping = {}
    tokens = [t.strip() for t in raw_input.split(",") if t.strip()]
    
    for token in tokens:
        if ":" in token:
            file_pattern, table_alias = token.split(":", 1)
            mapping[file_pattern.strip()] = table_alias.strip().lower()
        else:
            mapping[token.strip()] = None
            
    return mapping


def derive_table_name(path: str, alias: str = None) -> str:
    """Si un alias est fourni, l'utilise, sinon déduit le nom depuis le fichier."""
    if alias:
        return alias
    filename = os.path.basename(path)
    base_name = os.path.splitext(filename)[0]
    return base_name.replace("-", "_").replace(" ", "_").lower()


MAPPING = parse_file_mapping(FILE_MAPPING_RAW)


if USE_AZURITE:
    # ------------------------------------------------------------------
    # 1. MODE DEV LOCAL (AZURITE) AVEC STREAMING / CHUNKING PAR BATCHS
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
            print(f"⚠️ Erreur d'accès au conteneur Azurite '{CONTAINER_NAME}': {e}")
            return []

        for file_pattern, custom_table in MAPPING.items():
            for blob in all_blobs:
                if file_pattern == "*.parquet" or blob.name == file_pattern or blob.name.endswith(file_pattern):
                    blob_name = blob.name
                    target_table_name = derive_table_name(blob_name, custom_table)

                    # Générateur découpant la table Arrow par lots (chunk_size)
                    def make_generator(b_name=blob_name):
                        def item_generator():
                            blob_client = container_client.get_blob_client(b_name)
                            stream = blob_client.download_blob()
                            table = pq.read_table(io.BytesIO(stream.readall()))
                            
                            # REPRODUIT LE COMPORTEMENT DLT NATIVE (CHUNK_SIZE) :
                            # Au lieu de yield toute la table en 1 bloc, on yield par paquets de N lignes
                            for batch in table.to_batches(max_chunksize=CHUNK_SIZE):
                                yield batch.to_pylist()

                        return item_generator

                    res = dlt.resource(
                        make_generator(),
                        name=target_table_name,
                        selected=True,
                    )
                    resources.append(res)

        if not resources:
            print(f"⚠️ Aucun fichier correspondant trouvé dans Azurite '{CONTAINER_NAME}' pour {MAPPING}")

        return resources

else:
    # ------------------------------------------------------------------
    # 2. MODE PROD NATIVE (ADLS GEN2) - STREAMING STREAMÉ PAR DEFAULT
    # ------------------------------------------------------------------
    @dlt.source
    def source_parquet_data():
        resources = []
        
        for file_pattern, custom_table in MAPPING.items():
            files = filesystem(
                bucket_url=f"az://{CONTAINER_NAME}",
                file_glob=file_pattern,
            )
            
            for file_item in files:
                file_path = file_item["file_name"]
                target_table_name = derive_table_name(file_path, custom_table)
                
                single_file = filesystem(
                    bucket_url=f"az://{CONTAINER_NAME}",
                    file_glob=file_path,
                )
                
                res = (single_file | read_parquet()).with_name(target_table_name)
                resources.append(res)

        return resources


def run_pipeline():
    mode_label = f"Azurite Local ({CONTAINER_NAME})" if USE_AZURITE else f"ADLS Gen2 Prod ({CONTAINER_NAME})"
    print(f"🚀 Démarrage du pipeline DLT Ingestion [{mode_label}] -> Schema: {TARGET_SCHEMA}")
    print(f"📋 Mapping : {MAPPING} | Lot (chunk_size): {CHUNK_SIZE} lignes")

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_ID,
        destination="postgres_dest",
        dataset_name=TARGET_SCHEMA,
    )

    load_info = pipeline.run(
        source_parquet_data(),
        write_disposition=WRITE_STRATEGY,
    )

    print("\n✅ Ingestion terminée avec succès !")
    print(load_info)


if __name__ == "__main__":
    run_pipeline()