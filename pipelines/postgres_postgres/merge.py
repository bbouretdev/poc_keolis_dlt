import dlt
from dlt.sources.sql_database import sql_table

import os
import json

schema = os.environ["DLT_SOURCE_SCHEMA"]
table = os.environ["DLT_SOURCE_TABLE"]
backend = os.environ["DLT_BACKEND"]
chunk_size = int(os.environ["DLT_CHUNK_SIZE"])
primary_key = None
if "DLT_PRIMARY_KEY" in os.environ:
    primary_key = json.loads(os.environ["DLT_PRIMARY_KEY"])

source = sql_table(
    table=table,
    schema=schema,
    backend=backend,
    chunk_size=chunk_size,
)
if primary_key:
    source = source.apply_hints(primary_key=primary_key)

pipeline = dlt.pipeline(
    pipeline_name="postgres_postgres_merge",
    destination="postgres_dest",
    dataset_name="dlt"
)

load_info = pipeline.run(
    source,
    table_name="orders",
    write_disposition="merge"
)

print(pipeline.last_trace)
print(load_info)