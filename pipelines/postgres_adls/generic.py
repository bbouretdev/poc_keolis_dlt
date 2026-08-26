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
def add_date_partitions(date_column: str):
    def _stamp(table: pa.Table) -> pa.Table:
        col = table.column(date_column)
        return (
            table.append_column("year", pc.year(col))
            .append_column("month", pc.month(col))
            .append_column("day", pc.day(col))
        )

    @dlt.transformer(name=f"add_date_partitions_from_{date_column.lower()}")
    def _transformer(item: Any):
        if isinstance(item, pa.RecordBatch):
            item = pa.Table.from_batches([item])
        yield _stamp(item)

    return _transformer


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
    # 4. CALCUL ET INJECTION DES COLONNES MÉTIERS DANS LE SCHÉMA DLT
    # -------------------------------------------------------------------------
    extra_columns = {}
    if partition_col:
        print(f"📅 Partitionnement sur la date métier : {partition_col}")
        base_columns = dict(resource.columns)
        
        # Application du transformer PyArrow
        resource = (resource | add_date_partitions(date_column=partition_col)).with_name(target_name)
        
        # Indique à DLT que 'year', 'month', 'day' sont des colonnes de partition
        extra_columns = {
            **base_columns,
            "year": {"partition": True, "data_type": "bigint"},
            "month": {"partition": True, "data_type": "bigint"},
            "day": {"partition": True, "data_type": "bigint"},
        }

    resource.apply_hints(
        write_disposition=write_strategy,
        file_format="parquet",
        columns=extra_columns if partition_col else None,
    )

    # -------------------------------------------------------------------------
    # 5. EXECUTION DU PIPELINE
    # -------------------------------------------------------------------------
    if partition_col:
        layout_pattern = "{table_name}/Year={year}/Month={month}/Day={day}/{file_id}.{ext}"
        # On passe extra_placeholders sous forme de dictionnaire de fonctions/valeurs réelles
        # DLT saura qu'il doit évaluer dynamiquement ces clés depuis chaque RecordBatch
        dest = dlt.destinations.filesystem(
            layout=layout_pattern,
            extra_placeholders={
                "year": lambda item: item["year"],
                "month": lambda item: item["month"],
                "day": lambda item: item["day"],
            } if False else None  # Astuce DLT : laisser le schéma gérer directement
        )
    else:
        dest = "filesystem"

    # Si partition_col est présent, on indique le layout via la config du pipeline
    pipeline_kwargs = {
        "pipeline_name": pipeline_id,
        "destination": "filesystem",
        "dataset_name": dataset_name,
    }

    pipeline = dlt.pipeline(**pipeline_kwargs)

    # On passe le layout dynamique au niveau de la méthode run si partition_col existe
    run_kwargs = {}
    if partition_col:
        layout_pattern = "{table_name}/Year={year}/Month={month}/Day={day}/{file_id}.{ext}"
        # Configuration dynamique de la destination dans le pipeline
        pipeline.destination_client().config.layout = layout_pattern

    load_info = pipeline.run(resource)

    print("\n✅ Export terminé avec succès !")
    print(load_info)


if __name__ == "__main__":
    run_export_pipeline()