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

# destination = filesystem(
#     bucket_url="az://keolis",
#     credentials={
#         "azure_storage_account_name": "devstoreaccount1",
#         "azure_storage_account_key": "Eby8vdM02xNOcqFeqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
#         "azure_account_host": "azurite:10000/devstoreaccount1",
#     },
# )

pipeline = dlt.pipeline(
    pipeline_name="pif",
    destination="filesystem",
    dataset_name="paf",
)

# pipeline = dlt.pipeline(
#     pipeline_name=pipeline_id,
#     destination=destination,
#     dataset_name=target_path,
# )

print(destination.configuration)
load_info = pipeline.run(
    source,
    table_name=target_filename,
    write_disposition="replace",
    loader_file_format="parquet",
)

print(pipeline.last_trace)
print(load_info)