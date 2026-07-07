import dlt
from dlt.sources.sql_database import sql_table

import os
import json

primary_key = None
if "DLT_PRIMARY_KEY" in os.environ:
    primary_key = json.loads(os.environ["DLT_PRIMARY_KEY"])

source = sql_table(
    table="orders",
    schema="dlt",
    backend="connectorx",
    chunk_size=50000,
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