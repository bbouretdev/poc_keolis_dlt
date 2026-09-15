DEFAULT_SCHEMA = "dlt"
STREAM_TO_DELTA = True

TABLES_CONFIG = {
    "items": {
        "schema": "dlt",
        "date_column": "expedition_date",
        "start_date": None,
        "end_date": None,
    },
    "orders": {
        "schema": "dlt",
        "date_column": "expedition_date",
        "start_date": "2026-08-25",
        "end_date": "2026-08-25",
        "stream_to_delta": True,
    },
    "test_precision_moteurs": {
        "schema": "public",
        "date_column": "date_passage",
        "start_date": None,
        "end_date": None,
        "backend": "pyarrow",
    },
}

DATASET_NAME = "billetique"
BUCKET_URL = "s3://target-data"