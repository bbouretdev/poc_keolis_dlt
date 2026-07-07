import dlt
from dlt.sources.sql_database import sql_table

source = sql_table(
    table="orders",
    schema="dlt",
    backend="connectorx",
    chunk_size=50000,
).apply_hints(
    primary_key="order_number"
)

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