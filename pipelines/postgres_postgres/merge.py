import dlt
from dlt.sources.sql_database import sql_table

import os
import json

pipeline_id = os.environ["DLT_PIPELINE_ID"]
source_schema = os.environ["DLT_SOURCE_SCHEMA"]
source_table = os.environ["DLT_SOURCE_TABLE"]
target_schema = os.environ["DLT_TARGET_SCHEMA"]
target_table = os.environ["DLT_TARGET_TABLE"]
backend = os.environ["DLT_BACKEND"]
chunk_size = int(os.environ["DLT_CHUNK_SIZE"])

primary_key = None
if "DLT_PRIMARY_KEY" in os.environ:
    primary_key = json.loads(os.environ["DLT_PRIMARY_KEY"])

source = sql_table(
    table=source_table,
    schema=source_schema,
    backend=backend,
    chunk_size=chunk_size,
)
if primary_key:
    source = source.apply_hints(primary_key=primary_key)

pipeline = dlt.pipeline(
    pipeline_name=pipeline_id,
    destination="postgres_dest",
    dataset_name=target_schema
)

load_info = pipeline.run(
    source,
    table_name=target_table,
    write_disposition="merge"
)

print(pipeline.last_trace)
print(load_info)