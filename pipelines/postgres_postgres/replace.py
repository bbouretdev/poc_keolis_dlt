import dlt
from dlt.sources.sql_database import sql_table

import os
import json

from datetime import datetime, timezone

start_time = datetime.now(timezone.utc)
print(f"[START] Pipeline started at {start_time.isoformat()}")

pipeline_id = os.environ["DLT_PIPELINE_ID"]
source_schema = os.environ["DLT_SOURCE_SCHEMA"]
source_table = os.environ["DLT_SOURCE_TABLE"]
target_schema = os.environ["DLT_TARGET_SCHEMA"]
target_table = os.environ["DLT_TARGET_TABLE"]
backend = os.environ["DLT_BACKEND"]
chunk_size = int(os.environ["DLT_CHUNK_SIZE"])

# --- Infos source (pour les logs) ---
source_host = os.environ.get("SOURCES__SQL_DATABASE__CREDENTIALS__HOST")
source_db = os.environ.get("SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE")
source_port = os.environ.get("SOURCES__SQL_DATABASE__CREDENTIALS__PORT")

# --- Infos destination (pour les logs) ---
dest_host = os.environ.get("DESTINATION__POSTGRES_DEST__CREDENTIALS__HOST")
dest_db = os.environ.get("DESTINATION__POSTGRES_DEST__CREDENTIALS__DATABASE")
dest_port = os.environ.get("DESTINATION__POSTGRES_DEST__CREDENTIALS__PORT")

source = sql_table(
    table=source_table,
    schema=source_schema,
    backend=backend,
    chunk_size=chunk_size,
)

pipeline = dlt.pipeline(
    pipeline_name=pipeline_id,
    destination="postgres_dest",
    dataset_name=target_schema
)

load_info = pipeline.run(
    source,
    table_name=target_table,
    write_disposition="replace"
)

end_time = datetime.now(timezone.utc)
duration = (end_time - start_time).total_seconds()

normalize_info = pipeline.last_trace.last_normalize_info
rows_processed = normalize_info.row_counts.get(DEST_TABLE, 0)

print("=" * 60)
print("PIPELINE EXECUTION SUMMARY")
print("=" * 60)
print(f"Started at         : {start_time.isoformat()}")
print(f"Finished at        : {end_time.isoformat()}")
print(f"Duration           : {duration:.2f}s")
print(f"Backend            : {BACKEND or 'default (sqlalchemy)'}")
print("-" * 60)
print("SOURCE")
print(f"  Host             : {source_host}")
print(f"  Database         : {source_db}")
print(f"  Port             : {source_port}")
print(f"  Schema           : {SOURCE_SCHEMA}")
print(f"  Table            : {SOURCE_TABLE}")
print("-" * 60)
print("DESTINATION")
print(f"  Host             : {dest_host}")
print(f"  Database         : {dest_db}")
print(f"  Port             : {dest_port}")
print(f"  Dataset (schema) : {DEST_DATASET}")
print(f"  Table            : {DEST_TABLE}")
print("-" * 60)
print("ROWS")
print(f"  Write disposition : {WRITE_DISPOSITION}")
print(f"  Primary key        : {primary_key or 'n/a'}")
print(f"  Rows processed      : {rows_processed}")
print("=" * 60)

print(pipeline.last_trace)
print(load_info)