import dlt
from dlt.sources.rest_api import rest_api_source

import os
import json

from datetime import datetime, timezone

start_time = datetime.now(timezone.utc)
print(f"[START] Pipeline started at {start_time.isoformat()}")

pipeline_id = os.environ["DLT_PIPELINE_ID"]

api_base_url = os.environ["DLT_SOURCE_API_BASE_URL"]
source_endpoint = os.environ["DLT_SOURCE_ENDPOINT"]
target_path = os.environ["DLT_TARGET_PATH"]
target_filename = os.environ["DLT_TARGET_FILENAME"]
file_format = os.environ.get("DLT_FILE_FORMAT", "jsonl")
chunk_size = int(os.environ["DLT_CHUNK_SIZE"])


source = rest_api_source(
    {
        "client": {
            "base_url": api_base_url,
        },
        "resource_defaults": {
            "endpoint": {
                "params": {
                    "limit": chunk_size,
                },
            },
        },
        "resources": [
            {
                "name": target_path,
                "endpoint": {
                    "path": source_endpoint,
                    "paginator": "json_link",  # PokeAPI expose "next" dans la réponse JSON
                },
            },
        ],
    }
)

pipeline = dlt.pipeline(
    pipeline_name=pipeline_id,
    destination="filesystem",
    dataset_name=target_path,
)

load_info = pipeline.run(
    source,
    table_name=target_filename,
    write_disposition="replace",
    loader_file_format=file_format,
)

end_time = datetime.now(timezone.utc)
duration = (end_time - start_time).total_seconds()

normalize_info = pipeline.last_trace.last_normalize_info

if normalize_info is not None:
    rows_processed = normalize_info.row_counts.get(target_path, 0)
else:
    print("Aucune extraction effectuée — chargement d'un package en attente uniquement")
    rows_processed = 0

print("=" * 60)
print("PIPELINE EXECUTION SUMMARY")
print("=" * 60)
print(f"Started at         : {start_time.isoformat()}")
print(f"Finished at        : {end_time.isoformat()}")
print(f"Duration           : {duration:.2f}s")
print("-" * 60)
print("SOURCE")
print(f"  API base URL     : {api_base_url}")
print(f"  Endpoint         : {source_endpoint}")
print(f"  Page size        : {chunk_size}")
print("-" * 60)
print("DESTINATION")
print(f"  Bucket URL       : {target_bucket_url}")
print(f"  File format      : {file_format}")
print(f"  Dataset (table)  : {target_path}")
print("-" * 60)
print("ROWS")
print(f"  Write disposition : REPLACE")
print(f"  Rows processed      : {rows_processed}")
print("=" * 60)

print(pipeline.last_trace)
print(load_info)