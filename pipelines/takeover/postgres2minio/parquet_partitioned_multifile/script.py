from datetime import datetime, timedelta
import logging
import dlt
from dlt.sources.sql_database import sql_database
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ==============================================================================
# SECTION 1 : GESTION DES BORNES DE DATES ET ADAPTATEURS SQL
# ==============================================================================
def resolve_date_bounds(start_date: str, end_date: str):
    """Normalise les bornes temporelles pour le filtrage SQL semi-ouvert."""
    if start_date == end_date and len(start_date) == 10:
        dt_end = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        return start_date, dt_end.strftime("%Y-%m-%d")
    return start_date, end_date


def build_query_adapter(tables_config: dict):
    """Filtre la requête SQL côté PostgreSQL si des dates sont spécifiées."""
    def query_adapter(query, table):
        table_config = tables_config.get(table.name, {})
        date_column_name = table_config.get("date_column")
        start_date = table_config.get("start_date")
        end_date = table_config.get("end_date")

        if date_column_name and start_date and end_date and date_column_name in table.c:
            actual_start, actual_end = resolve_date_bounds(start_date, end_date)
            sqlalchemy_date_column = table.c[date_column_name]
            return query.where(sqlalchemy_date_column >= actual_start).where(
                sqlalchemy_date_column < actual_end
            )
        return query

    return query_adapter


def get_pyarrow_s3_filesystem():
    """Crée l'objet S3FileSystem PyArrow à partir des secrets dlt."""
    aws_access_key_id = dlt.secrets.get("destination.filesystem.credentials.aws_access_key_id")
    aws_secret_access_key = dlt.secrets.get("destination.filesystem.credentials.aws_secret_access_key")
    endpoint_url = dlt.secrets.get("destination.filesystem.credentials.endpoint_url") or ""
    region_name = dlt.secrets.get("destination.filesystem.credentials.region_name") or "us-east-1"

    endpoint_override = endpoint_url.replace("http://", "").replace("https://", "") if endpoint_url else None

    return pafs.S3FileSystem(
        access_key=aws_access_key_id,
        secret_key=aws_secret_access_key,
        endpoint_override=endpoint_override,
        region=region_name,
        scheme="http" if endpoint_url.startswith("http://") else "https",
    )


# ==============================================================================
# SECTION 2 : EXPORT PARQUET SIMPLE VIA PYARROW (HIVE PARTITIONING)
# ==============================================================================
def process_table_export(table_name: str, table_config: dict):
    postgres_schema = table_config.get("schema", getattr(config, "DEFAULT_SCHEMA", "public"))
    backend = table_config.get("backend", "connectorx")
    date_column_name = table_config.get("date_column")
    start_date = table_config.get("start_date")
    end_date = table_config.get("end_date")

    fs = get_pyarrow_s3_filesystem()
    
    bucket_name = config.BUCKET_URL.replace("s3://", "").rstrip("/")
    table_base_path = f"{bucket_name}/{config.DATASET_NAME}/{table_name}"

    is_windowed = bool(date_column_name and start_date and end_date)

    # 1. Extraction SQL via dlt / ConnectorX -> PyArrow
    query_adapter = build_query_adapter({table_name: table_config})
    table_source = sql_database(
        schema=postgres_schema,
        backend=backend,
        reflection_level="full_with_precision",
        chunk_size=100000,
        query_adapter_callback=query_adapter if is_windowed else None,
    ).with_resources(table_name)

    resource = getattr(table_source, table_name)
    extracted_data = list(resource)

    if not extracted_data:
        logging.warning(f"⚠️ Aucune donnée extraite pour la table '{table_name}'.")
        return

    # 2. Consolidation PyArrow
    batches = []
    for item in extracted_data:
        if isinstance(item, pa.Table):
            batches.extend(item.to_batches())
        elif isinstance(item, pa.RecordBatch):
            batches.append(item)

    arrow_table = pa.Table.from_batches(batches)

    # Configuration du partitionnement style Hive : "colonne=valeur"
    hive_partitioning = (
        ds.partitioning(
            schema=pa.schema([(date_column_name, arrow_table.schema.field(date_column_name).type)]), 
            flavor="hive"
        )
        if date_column_name and date_column_name in arrow_table.column_names
        else None
    )

    # 3. Écriture Parquet native sur le stockage objet
    if is_windowed:
        actual_start, _ = resolve_date_bounds(start_date, end_date)
        target_partition_dir = f"{table_base_path}/{date_column_name}={actual_start}"
        
        logging.info(f"--- Remplacement ciblé (Parquet Hive) pour '{table_name}' sur {date_column_name}={actual_start} ---")

        # PURGE CIBLÉE DE LA PARTITION
        try:
            fs.delete_dir(target_partition_dir)
        except Exception:
            pass

        # Écriture directe avec nom de fichier explicite pour éviter le sous-dossier parasite
        ds.write_dataset(
            data=arrow_table,
            base_dir=target_partition_dir,
            format="parquet",
            filesystem=fs,
            basename_template="data_{i}.parquet",
            existing_data_behavior="overwrite_or_ignore",
        )

    else:
        logging.info(f"--- Remplacement COMPLET (Parquet Full Overwrite) pour '{table_name}' ---")
        
        # PURGE TOTALE DU DOSSIER DE LA TABLE
        try:
            fs.delete_dir(table_base_path)
        except Exception:
            pass

        # Écriture racine avec partitionnement Hive automatique
        ds.write_dataset(
            data=arrow_table,
            base_dir=table_base_path,
            format="parquet",
            filesystem=fs,
            partitioning=hive_partitioning,
            basename_template="data_{i}.parquet",
            existing_data_behavior="overwrite_or_ignore",
        )

    logging.info(f"✅ Export Parquet réussi pour '{table_name}'.")


def export_tables_to_minio():
    logging.info("🚀 Démarrage du traitement Parquet simple...")
    for table_name, table_config in config.TABLES_CONFIG.items():
        try:
            process_table_export(table_name, table_config)
        except Exception as error:
            logging.error(f"❌ Échec lors de l'export de '{table_name}': {error}")
            continue
    logging.info("🏁 Traitement terminé.")


if __name__ == "__main__":
    export_tables_to_minio()