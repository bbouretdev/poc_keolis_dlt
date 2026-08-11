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
    "ECRASER": "replace",
    "AJOUTER": "append",
    "METTRE_A_JOUR": "merge"
}
raw_strategy = os.environ.get("DLT_WRITE_STRATEGY", "ECRASER")
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
print(f"PIPELINE COMPLETED: {write_disposition} on {target_table} in {(end_time - start_time).total_seconds():.2f}s")
print(load_info)