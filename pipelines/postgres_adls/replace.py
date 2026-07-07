import dlt
from dlt.sources.sql_database import sql_table
from dlt.destinations import filesystem

import os

pipeline_id = os.environ["DLT_PIPELINE_ID"]
source_schema = os.environ["DLT_SOURCE_SCHEMA"]
source_table = os.environ["DLT_SOURCE_TABLE"]
target_path = os.environ["DLT_TARGET_PATH"]
target_filename = os.environ["DLT_TARGET_FILENAME"]
backend = os.environ["DLT_BACKEND"]
chunk_size = int(os.environ["DLT_CHUNK_SIZE"])

source = sql_table(
    table=source_table,
    schema=source_schema,
    backend=backend,
    chunk_size=chunk_size,
)

# Destination : Azure Blob Storage (via Azurite en local/POC)
blob_destination = filesystem(
    bucket_url="az://moncontainer",
    destination_name="azurite_blob",
)

pipeline = dlt.pipeline(
    pipeline_name=pipeline_id,
    destination=blob_destination,
    dataset_name=target_path,
)

load_info = pipeline.run(
    source,
    table_name=target_filename,
    write_disposition="replace",
    loader_file_format="parquet",
)

print(pipeline.last_trace)
print(load_info)