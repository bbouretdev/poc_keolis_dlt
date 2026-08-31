import datetime
import os
import sys
import dlt
from dlt.destinations import filesystem
from dlt.sources.sql_database import sql_table
import pyarrow as pa
import pyarrow.compute as pc
import sqlalchemy as sa

# -----------------------------------------------------------------------------
# 1. LECTURE ET CONTÔLE DES VARIABLES D'ENVIRONNEMENT
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
    use_partition = os.environ.get("DLT_USE_PARTITION", "true").lower() in ("true", "1", "yes")

    # Mode Fenêtré
    enable_windowing = os.environ.get("DLT_ENABLE_WINDOWING", "false").lower() in ("true", "1", "yes")
    incremental_cursor = os.environ.get("DLT_INCREMENTAL_CURSOR", "").strip()
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


# -----------------------------------------------------------------------------
# 3. BUILDER DE CALLBACK : BORNE SUPÉRIEURE (MINUIT CE MATIN)
# -----------------------------------------------------------------------------
def build_upper_bound_callback(cursor_col_name: str):
    """
    Injecte la condition SQL : WHERE cursor_col < minuit_aujourd'hui.
    Supporte la signature dlt multi-arguments (*args, **kwargs).
    """
    midnight_today = datetime.datetime.now(datetime.timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    def query_adapter(query, table_schema, *args, **kwargs):
        if hasattr(table_schema, "columns") and cursor_col_name in table_schema.columns:
            col_obj = table_schema.columns[cursor_col_name]
            col_name = col_obj["name"] if isinstance(col_obj, dict) else getattr(col_obj, "name", cursor_col_name)
        else:
            col_name = cursor_col_name

        sql_col = sa.column(col_name)
        print(f"🔒 Borne supérieure SQL injectée : {col_name} < {midnight_today.isoformat()}")
        return query.where(sql_col < midnight_today)

    return query_adapter


def run_export_pipeline():
    print(f"🚀 Export DLT (Format : {storage_format.upper()}) - Strategy : {write_strategy}")
    print(f"⚙️ Backend : {backend} | Chunk size : {chunk_size}")
    print(f"📦 Source : {source_schema}.{source_table} -> Cible : {dataset_name}/{target_name}")

    # -------------------------------------------------------------------------
    # 4. CONFIGURATION DE LA RESSOURCE DLT SOURCE
    # -------------------------------------------------------------------------
    dlt_kwargs = {
        "table": source_table,
        "schema": source_schema,
        "backend": backend,
        "chunk_size": chunk_size,
    }

    if backend == "pyarrow":
        dlt_kwargs["reflection_level"] = "full_with_precision"

    # Mode Fenêtré
    if enable_windowing and incremental_cursor:
        print(f"🪟 Mode fenêtré activé sur le curseur : '{incremental_cursor}'")
        
        # Borne inférieure dynamique (WHERE cursor > watermark)
        dlt_kwargs["incremental"] = dlt.sources.incremental(
            incremental_cursor,
            initial_value=None,
            range_start="open",
        )

        # Borne supérieure fixe (WHERE cursor < minuit_aujourd'hui)
        dlt_kwargs["query_adapter_callback"] = build_upper_bound_callback(incremental_cursor)
    else:
        print("ℹ️ Mode Full / Non-fenêtré activé.")

    resource = sql_table(**dlt_kwargs)

    if target_name != source_table:
        resource = resource.with_name(target_name)

    # -------------------------------------------------------------------------
    # 5. PARTITIONNEMENT
    # -------------------------------------------------------------------------
    columns_hints = {}

    if use_partition and partition_col:
        print(f"📅 Partitionnement activé sur la colonne : {partition_col}")
        resource.add_map(add_date_partitions)

        columns_hints = {
            "Year": {"partition": True, "data_type": "bigint"},
            "Month": {"partition": True, "data_type": "bigint"},
            "Day": {"partition": True, "data_type": "bigint"},
        }
    else:
        print("ℹ️ Mode non-partitionné activé.")

    table_format_value = storage_format if storage_format == "delta" else None

    resource.apply_hints(
        write_disposition=write_strategy,
        table_format=table_format_value,
        columns=columns_hints if (use_partition and partition_col) else None,
    )

    # -------------------------------------------------------------------------
    # 6. INSTANCIATION DESTINATION ET EXECUTION
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