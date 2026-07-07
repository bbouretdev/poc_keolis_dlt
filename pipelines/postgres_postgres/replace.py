import dlt
from dlt.sources.sql_database import sql_table

import os
import json

schema = os.environ["DLT_SOURCE_SCHEMA"]
table = os.environ["DLT_SOURCE_TABLE"]
backend = os.environ["DLT_BACKEND"]
chunk_size = int(os.environ["DLT_CHUNK_SIZE"])

source = sql_table(
    table=table,
    schema=schema,
    backend=backend,
    chunk_size=chunk_size,
)

pipeline = dlt.pipeline(
    pipeline_name="postgres_postgres_replace",
    destination="postgres_dest",
    dataset_name="dlt"
)

load_info = pipeline.run(
    source,
    table_name="orders",
    write_disposition="replace"
)

print(pipeline.last_trace)
print(load_info)