import os
import sys
import dlt
from dlt.destinations import filesystem
from dlt.sources.sql_database import sql_table
import pyarrow as pa
import pyarrow.compute as pc

# -----------------------------------------------------------------------------
# 1. LECTURE DES VARIABLES D'ENVIRONNEMENT
# -----------------------------------------------------------------------------
try:
    pipeline_id = os.environ["DLT_PIPELINE_ID"]
    source_schema = os.environ["DLT_SOURCE_SCHEMA"]
    dataset_name = os.environ["DLT_DATASET_NAME"]
    source_table = os.environ["DLT_SOURCE_TABLE"]
    target_name = os.environ["DLT_TARGET_NAME"]
    partition_col = os.environ.get("DLT_PARTITION_COL", "").strip()
    backend = os.environ["DLT_BACKEND"].lower()
    chunk_size = int(os.environ["DLT_CHUNK_SIZE"])
    write_strategy = os.environ["DLT_WRITE_STRATEGY"].lower()
    use_azurite = os.environ.get("USE_AZURITE", "false").lower() in ("true", "1", "yes")
    storage_format = os.environ.get("DLT_STORAGE_FORMAT", "delta").lower()
except KeyError as e:
    print(f"❌ ERREUR CRITIQUE : Variable {e} absente.")
    sys.exit(1)


# -----------------------------------------------------------------------------
# 2. TRANSFORMER DLT SUR DATE MÉTIER
# -----------------------------------------------------------------------------
def add_date_partitions(table: pa.Table) -> pa.Table:
    if not partition_col or partition_col not in table.column_names:
        return table
    col = table.column(partition_col)
    return (
        table.append_column("Year", pc.year(col))
        .append_column("Month", pc.month(col))
        .append_column("Day", pc.day(col))
    )


def run_export_pipeline():
    print(f"🚀 Export DLT (Format : {storage_format.upper()}) - Strategy : {write_strategy}")
    print(f"📦 Source : {source_schema}.{source_table} -> Cible : {dataset_name}/{target_name}")

    # -------------------------------------------------------------------------
    # 3. CRÉATION DE LA RESSOURCE SOURCE
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
        resource = resource.with_name(target_name)

    # -------------------------------------------------------------------------
    # 4. PARTITIONNEMENT
    # -------------------------------------------------------------------------
    columns_hints = {}

    if partition_col:
        print(f"📅 Partitionnement activé sur la colonne : {partition_col}")
        resource.add_map(add_date_partitions)

        columns_hints = {
            "Year": {"partition": True, "data_type": "bigint"},
            "Month": {"partition": True, "data_type": "bigint"},
            "Day": {"partition": True, "data_type": "bigint"},
        }

    # "parquet" n'est pas un table_format valide pour dlt (seuls delta, iceberg, etc. le sont)
    table_format_value = storage_format if storage_format == "delta" else None

    resource.apply_hints(
        write_disposition=write_strategy,
        table_format=table_format_value,
        columns=columns_hints if partition_col else None,
    )

    # -------------------------------------------------------------------------
    # 5. RESOLUTION DE LA DESTINATION
    # -------------------------------------------------------------------------
    bucket_url = os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"]

    if storage_format == "delta":
        if use_azurite:
            azurite_endpoint = "http://azurite:10000/devstoreaccount1"
            destination_obj = filesystem(
                bucket_url=bucket_url,
                deltalake_storage_options={
                    "azure_storage_allow_http": "true",
                    "azure_storage_use_http": "true",
                    "allow_http": "true",
                    "azure_storage_account_name": "devstoreaccount1",
                    "azure_storage_account_key": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
                    "azure_endpoint_url": azurite_endpoint,
                    "azure_endpoint": azurite_endpoint,
                },
            )
        else:
            destination_obj = filesystem(
                bucket_url=bucket_url,
                deltalake_storage_options={
                    "timeout": "60s",
                    "max_retries": "3",
                },
            )
    else:
        # Format Parquet standard (utilise fsspec/adlfs nativement)
        destination_obj = filesystem(bucket_url=bucket_url)

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_id,
        destination=destination_obj,
        dataset_name=dataset_name,
    )

    load_info = pipeline.run(resource)

    print(f"\n✅ Export {storage_format.upper()} terminé avec succès !")
    print(load_info)


if __name__ == "__main__":
    run_export_pipeline()