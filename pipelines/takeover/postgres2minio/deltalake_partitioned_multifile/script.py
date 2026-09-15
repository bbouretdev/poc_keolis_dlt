from datetime import datetime, timedelta
import logging
import sys
import dlt
from dlt.sources.sql_database import sql_database
from deltalake import write_deltalake
import pyarrow as pa

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ==============================================================================
# SECTION 1 : GESTION DES BORNES DE DATES ET ADAPTATEUR SQL
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


def get_s3_storage_options():
    """Lit les accès MinIO/S3 depuis secrets.toml pour la librairie deltalake."""
    aws_access_key_id = dlt.secrets.get("destination.filesystem.credentials.aws_access_key_id")
    aws_secret_access_key = dlt.secrets.get("destination.filesystem.credentials.aws_secret_access_key")
    endpoint_url = dlt.secrets.get("destination.filesystem.credentials.endpoint_url") or ""
    region_name = dlt.secrets.get("destination.filesystem.credentials.region_name") or "us-east-1"

    return {
        "AWS_ACCESS_KEY_ID": aws_access_key_id,
        "AWS_SECRET_ACCESS_KEY": aws_secret_access_key,
        "AWS_ENDPOINT_URL": endpoint_url,
        "AWS_REGION": region_name,
        "AWS_ALLOW_HTTP": "true" if endpoint_url.startswith("http://") else "false",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


# ==============================================================================
# SECTION 2 : EXPORT UNIFIÉ DELTA LAKE (FULL ET FENÊTRÉ)
# ==============================================================================
def process_table_export(table_name: str, table_config: dict):
    postgres_schema = table_config.get("schema", getattr(config, "DEFAULT_SCHEMA", "public"))
    backend = table_config.get("backend", "connectorx")
    date_column_name = table_config.get("date_column")
    start_date = table_config.get("start_date")
    end_date = table_config.get("end_date")
    stream_to_delta = table_config.get("stream_to_delta", getattr(config, "STREAM_TO_DELTA", False))

    table_uri = f"{config.BUCKET_URL.rstrip('/')}/{config.DATASET_NAME}/{table_name}"
    is_windowed = bool(date_column_name and start_date and end_date)

    # 1. Extraction SQL via dlt (ConnectorX) -> PyArrow
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

    # 2. Consolidation des RecordBatches PyArrow
    batches = []
    for item in extracted_data:
        if isinstance(item, pa.Table):
            batches.extend(item.to_batches())
        elif isinstance(item, pa.RecordBatch):
            batches.append(item)

    # Choix du conteneur selon l'option de Streaming
    if stream_to_delta:
        data_to_write = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    else:
        data_to_write = pa.Table.from_batches(batches)

    # 3. Écriture UNIFIÉE dans Delta Lake via `write_deltalake`
    storage_options = get_s3_storage_options()

    if is_windowed:
        actual_start, actual_end = resolve_date_bounds(start_date, end_date)
        predicate = f"{date_column_name} >= '{actual_start}' AND {date_column_name} < '{actual_end}'"
        logging.info(f"--- Remplacement CIBLÉ pour '{table_name}' sur [{actual_start} -> {actual_end}[ ---")
        
        write_deltalake(
            table_or_uri=table_uri,
            data=data_to_write,
            mode="overwrite",
            partition_by=[date_column_name],
            predicate=predicate,
            storage_options=storage_options,
        )
    else:
        logging.info(f"--- Remplacement COMPLET (Full Overwrite) pour '{table_name}' ---")
        
        write_deltalake(
            table_or_uri=table_uri,
            data=data_to_write,
            mode="overwrite",
            partition_by=[date_column_name] if date_column_name else None,
            storage_options=storage_options,
        )

    logging.info(f"✅ Export réussi pour '{table_name}'.")


def export_tables_to_minio():
    logging.info("🚀 Démarrage du traitement d'exportation unifié...")
    for table_name, table_config in config.TABLES_CONFIG.items():
        try:
            process_table_export(table_name, table_config)
        except Exception as error:
            logging.error(f"❌ Échec lors de l'export de '{table_name}': {error}")
            continue
    logging.info("🏁 Traitement terminé.")


if __name__ == "__main__":
    export_tables_to_minio()