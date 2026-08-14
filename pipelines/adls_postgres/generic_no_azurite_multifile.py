import os
import dlt
from dlt.sources.filesystem import filesystem, read_parquet

CONTAINER_NAME = os.environ["DLT_AZURE_CONTAINER"]
# Liste de patterns séparés par des virgules
FILE_GLOBS = os.environ.get("DLT_FILE_GLOBS", "*.parquet").split(",")
PIPELINE_ID = os.environ.get("DLT_PIPELINE_ID", "adls_multi_to_postgres")
TARGET_SCHEMA = os.environ.get("DLT_TARGET_SCHEMA", "dlt")
WRITE_STRATEGY = os.environ.get("DLT_WRITE_STRATEGY", "replace").lower()


@dlt.source
def adls_multi_source():
    """Source DLT qui scannera plusieurs fichiers/patterns dans ADLS."""
    resources = []
    for glob_pattern in FILE_GLOBS:
        glob_pattern = glob_pattern.strip()
        
        # Nom de la table dérivé du dossier/pattern (ou nom fixe)
        # Ex: "users/*.parquet" -> table_name = "users"
        clean_name = glob_pattern.split("/")[0].replace("*", "").replace(".", "") or "raw_data"
        
        files = filesystem(
            bucket_url=f"az://{CONTAINER_NAME}",
            file_glob=glob_pattern,
        )
        
        # On attache la lecture parquet à chaque pattern de fichiers
        resources.append(
            (files | read_parquet()).with_name(clean_name)
        )
        
    return resources


def run_pipeline():
    print(f"🚀 Ingestion Multi-Fichiers : az://{CONTAINER_NAME} ({FILE_GLOBS}) -> Postgres ({TARGET_SCHEMA})")

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_ID,
        destination="postgres_dest",
        dataset_name=TARGET_SCHEMA,
    )

    # DLT gère nativement une liste ou un générateur de ressources
    load_info = pipeline.run(
        adls_multi_source(),
        write_disposition=WRITE_STRATEGY,
    )

    print("✅ Ingestion multi-fichiers terminée !")
    print(load_info)


if __name__ == "__main__":
    run_pipeline()