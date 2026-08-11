import os
import json
from datetime import datetime, timezone
import dlt
from dlt.sources.sql_database import sql_table

start_time = datetime.now(timezone.utc)

pipeline_id = os.environ["DLT_PIPELINE_ID"]
source_schema = os.environ["DLT_SOURCE_SCHEMA"]
source_table = os.environ["DLT_SOURCE_TABLE"]
target_schema = os.environ["DLT_TARGET_SCHEMA"]
target_table = os.environ["DLT_TARGET_TABLE"]
backend = os.environ.get("DLT_BACKEND", "connectorx")
chunk_size = int(os.environ.get("DLT_CHUNK_SIZE", "50000"))

write_disp_map = {
    "REPLACE": "replace",
    "APPEND": "append",
    "UPDATE": "merge"
}
raw_strategy = os.environ.get("DLT_WRITE_STRATEGY", "REPLACE")
write_disposition = write_disp_map.get(raw_strategy, "replace")

primary_key = None
if os.environ.get("DLT_PRIMARY_KEY"):
    primary_key = json.loads(os.environ["DLT_PRIMARY_KEY"])

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
    write_disposition=write_disposition,
    primary_key=primary_key
)

end_time = datetime.now(timezone.utc)
duration = (end_time - start_time).total_seconds()

print("=" * 60)
print("PIPELINE EXECUTION SUMMARY")
print("=" * 60)
print(f"Started at         : {start_time.isoformat()}")
print(f"Finished at        : {end_time.isoformat()}")
print(f"Duration           : {duration:.2f}s")
print(f"Backend            : {backend}")
print("-" * 60)
print("SOURCE")
print(f"  Host             : {source_host}")
print(f"  Database         : {source_db}")
print(f"  Port             : {source_port}")
print(f"  Schema           : {source_schema}")
print(f"  Table            : {source_table}")
print("-" * 60)
print("DESTINATION")
print(f"  Host             : {dest_host}")
print(f"  Database         : {dest_db}")
print(f"  Port             : {dest_port}")
print(f"  Dataset (schema) : {target_schema}")
print(f"  Table            : {target_table}")
print("-" * 60)
print("ROWS")
print(f"  Write disposition : REPLACE")
print(f"  Rows processed      : {rows_processed}")
print("=" * 60)

print(pipeline.last_trace)
print(load_info)