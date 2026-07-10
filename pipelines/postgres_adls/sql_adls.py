import dlt
import connectorx as cx
import os
from datetime import datetime, timezone

start_time = datetime.now(timezone.utc)
print(f"[START] Pipeline started at {start_time.isoformat()}")

pipeline_id = os.environ["DLT_PIPELINE_ID"]
target_schema = os.environ["DLT_TARGET_SCHEMA"]
target_table = os.environ["DLT_TARGET_TABLE"]
sql_query = os.environ["DLT_SOURCE_QUERY"]

# --- Connexion source (via connectorx, format URI) ---
source_user = os.environ["SOURCES__SQL_DATABASE__CREDENTIALS__USERNAME"]
source_password = os.environ["SOURCES__SQL_DATABASE__CREDENTIALS__PASSWORD"]
source_host = os.environ["SOURCES__SQL_DATABASE__CREDENTIALS__HOST"]
source_port = os.environ["SOURCES__SQL_DATABASE__CREDENTIALS__PORT"]
source_db = os.environ["SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE"]

conn_str = f"postgresql://{source_user}:{source_password}@{source_host}:{source_port}/{source_db}"

@dlt.resource(name=target_table, write_disposition="replace")
def custom_query_resource():
    table = cx.read_sql(conn_str, sql_query, return_type="arrow")
    yield table

pipeline = dlt.pipeline(
    pipeline_name=pipeline_id,
    destination="postgres_dest",
    dataset_name=target_schema,
)

load_info = pipeline.run(custom_query_resource(), table_name=target_table)

end_time = datetime.now(timezone.utc)
duration = (end_time - start_time).total_seconds()

normalize_info = pipeline.last_trace.last_normalize_info
rows_processed = normalize_info.row_counts.get(target_table, 0)

print("=" * 60)
print("PIPELINE EXECUTION SUMMARY")
print("=" * 60)
print(f"Started at         : {start_time.isoformat()}")
print(f"Finished at        : {end_time.isoformat()}")
print(f"Duration           : {duration:.2f}s")
print(f"Backend            : connectorx")
print(f"Query              : {sql_query}")
print(f"Rows processed     : {rows_processed}")
print("=" * 60)

print(pipeline.last_trace)
print(load_info)