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

print(pipeline.last_trace)
print(load_info)