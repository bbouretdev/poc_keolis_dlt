import os
import sys
from typing import Any
import dlt
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
        table.append_column("year", pc.year(col))
        .append_column("month", pc.month(col))
        .append_column("day", pc.day(col))
    )


def run_export_pipeline():
    print(f"🚀 Export DLT - Strategy: {write_strategy}")
    print(f"📦 Source : {source_schema}.{source_table} -> Cible : {dataset_name}/{target_name}")

    # -------------------------------------------------------------------------
    # 3. PRÉPARATION DE LA RESSOURCE SOURCE
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
    # 4. CALCUL ET INJECTION DES COLONNES MÉTIERS
    # -------------------------------------------------------------------------
    columns_hints = {}
    if partition_col:
        print(f"📅 Partitionnement activé sur la colonne : {partition_col}")
        
        # Injection de la transformation vectorielle Arrow
        resource.add_map(add_date_partitions)
        
        # Hints de partitionnement
        columns_hints = {
            "year": {"partition": True, "data_type": "bigint"},
            "month": {"partition": True, "data_type": "bigint"},
            "day": {"partition": True, "data_type": "bigint"},
        }

    resource.apply_hints(
        write_disposition=write_strategy,
        file_format="parquet",
        columns=columns_hints if partition_col else None,
    )

    # -------------------------------------------------------------------------
    # 5. EXECUTION DU PIPELINE ET CONFIGURATION DYNAMIQUE DU LAYOUT
    # -------------------------------------------------------------------------
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_id,
        destination="filesystem",
        dataset_name=dataset_name,
    )

    # Application dynamique du layout une fois les hints enregistrés sur la ressource
    if partition_col:
        layout_pattern = "{table_name}/Year={year}/Month={month}/Day={day}/{file_id}.{ext}"
        pipeline.destination_client().config.layout = layout_pattern

    load_info = pipeline.run(resource)

    print("\n✅ Export terminé avec succès !")
    print(load_info)


if __name__ == "__main__":
    run_export_pipeline()