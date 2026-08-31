from __future__ import annotations

import logging
import os
import random
import time
from typing import Any

import dlt
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources.filesystem import filesystem, read_parquet
from dlt.sources.helpers.requests import Client as DltHttpClient
from dlt.sources.helpers.requests import RequestException as DltRequestException
from dlt.sources.helpers.requests import Session as DltSession
from dlt.sources.rest_api import check_connection, rest_api_source

logger = logging.getLogger("api_to_adls_bis")

USE_AZURITE = os.getenv("USE_AZURITE", "false").strip().lower() == "true"

CONTAINER_NAME = os.getenv("DLT_AZURE_CONTAINER", "target-data")
FILE_GLOB = os.getenv("DLT_FILE_GLOB", "*.parquet")
PIPELINE_ID = os.getenv("DLT_PIPELINE_ID", "api_to_adls_pipeline")
TARGET_SCHEMA = os.getenv("DLT_TARGET_SCHEMA", "dlt")
WRITE_STRATEGY = os.getenv("DLT_WRITE_STRATEGY", "replace").lower()
CHUNK_SIZE = int(os.getenv("DLT_CHUNK_SIZE", "10000"))

DEFAULT_AZURITE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://azurite:10000/devstoreaccount1;"
)

_RETRYABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (
    ConnectionError,
    TimeoutError,
    OSError,
    DltRequestException,
    PipelineStepFailed,
)


def _first_error_message(exc: BaseException) -> str:
    text = str(exc).strip()
    if not text:
        return exc.__class__.__name__
    return text.splitlines()[0]


def resolve_storage_bucket_url(bucket_url: str | None = None) -> str:
    """Normalise the destination URL for Azurite or ADLS Gen2."""
    return bucket_url or f"az://{CONTAINER_NAME}"


def resolve_azurite_connection_string() -> str:
    """Return the local Azurite connection string used by DLT and the SDK."""
    return os.getenv("AZURE_STORAGE_CONNECTION_STRING", DEFAULT_AZURITE_CONNECTION_STRING)


def ensure_azurite_runtime_settings() -> None:
    """Populate the required runtime env vars when Azurite is enabled."""
    if not USE_AZURITE:
        return
    os.environ.setdefault("AZURE_STORAGE_CONNECTION_STRING", resolve_azurite_connection_string())
    os.environ.setdefault("AZURE_STORAGE_ACCOUNT_NAME", "devstoreaccount1")
    os.environ.setdefault(
        "AZURE_STORAGE_ACCOUNT_KEY",
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
    )


def parse_file_mapping(raw_input: str) -> dict[str, str | None]:
    """Parse patterns like '*.parquet' or 'items.parquet:items' ."""
    mapping: dict[str, str | None] = {}
    for token in (part.strip() for part in raw_input.split(",") if part.strip()):
        if ":" in token:
            pattern, alias = token.split(":", 1)
            mapping[pattern.strip()] = alias.strip().lower() or None
        else:
            mapping[token] = None
    return mapping


MAPPING = parse_file_mapping(FILE_GLOB)


def derive_table_name(path: str, alias: str | None = None) -> str:
    """Use an explicit alias when provided, otherwise derive from the file name."""
    if alias:
        return alias
    filename = os.path.basename(path)
    stem = os.path.splitext(filename)[0]
    return stem.replace("-", "_").replace(" ", "_").lower()


if USE_AZURITE:
    from azure.storage.blob import BlobServiceClient
    import pyarrow.parquet as pq

    @dlt.source
    def source_parquet_data():
        conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", DEFAULT_AZURITE_CONNECTION_STRING)
        blob_service_client = BlobServiceClient.from_connection_string(conn_str, api_version="2020-08-04")
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        resources = []
        try:
            blobs = list(container_client.list_blobs())
        except Exception as exc:  # pragma: no cover - defensive branch
            logger.error("AZURITE_LIST_FAILED | container=%s | error=%s", CONTAINER_NAME, exc)
            return []

        for file_pattern, custom_table in MAPPING.items():
            for blob in blobs:
                if file_pattern == "*.parquet" or blob.name == file_pattern or blob.name.endswith(file_pattern):
                    blob_name = blob.name
                    target_table_name = derive_table_name(blob_name, custom_table)

                    def make_generator(blob_path: str = blob_name):
                        def item_generator():
                            blob_client = container_client.get_blob_client(blob_path)
                            stream = blob_client.download_blob()
                            table = pq.read_table(stream.readall())
                            for batch in table.to_batches(max_chunksize=CHUNK_SIZE):
                                yield batch.to_pylist()

                        return item_generator

                    resources.append(dlt.resource(make_generator(), name=target_table_name, selected=True))

        if not resources:
            logger.warning("AZURITE_NO_MATCH | container=%s | patterns=%s", CONTAINER_NAME, MAPPING)

        return resources

else:
    @dlt.source
    def source_parquet_data():
        resources = []
        for file_pattern, custom_table in MAPPING.items():
            files = filesystem(bucket_url=f"az://{CONTAINER_NAME}", file_glob=file_pattern)
            for file_item in files:
                file_path = file_item["file_name"]
                target_table_name = derive_table_name(file_path, custom_table)
                single_file = filesystem(bucket_url=f"az://{CONTAINER_NAME}", file_glob=file_path)
                resources.append((single_file | read_parquet()).with_name(target_table_name))
        return resources


def build_adls_destination(
    bucket_url: str,
    layout: str = "{table_name}",
    deltalake_storage_options: dict[str, str] | None = None,
):
    """Create the DLT destination for either Azurite or ADLS Gen2."""
    resolved_bucket_url = resolve_storage_bucket_url(bucket_url)

    if USE_AZURITE:
        logger.warning(
            "DESTINATION_MODE=AZURITE | bucket=%s | account=devstoreaccount1",
            resolved_bucket_url,
        )
        ensure_azurite_runtime_settings()
        storage_options = {
            **(deltalake_storage_options or {"timeout": "60s", "max_retries": "3"}),
            "connection_string": resolve_azurite_connection_string(),
        }
        return dlt.destinations.filesystem(
            bucket_url=resolved_bucket_url,
            layout=layout,
            file_format="parquet",
            deltalake_storage_options=storage_options,
        )

    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    if not account_name or not account_key:
        raise ValueError(
            "AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY must both be set when USE_AZURITE=false."
        )

    storage_options = {
        **(deltalake_storage_options or {"timeout": "60s", "max_retries": "3"}),
        "account_name": account_name,
        "account_key": account_key,
    }
    return dlt.destinations.filesystem(
        bucket_url=resolved_bucket_url,
        layout=layout,
        deltalake_storage_options=storage_options,
    )


def _build_retrying_session(
    total_retries: int = 5,
    backoff_factor: float = 1.0,
    max_retry_delay: float = 60.0,
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
) -> DltSession:
    return DltHttpClient(
        request_max_attempts=total_retries,
        request_backoff_factor=backoff_factor,
        request_max_retry_delay=max_retry_delay,
        status_codes=status_forcelist,
        respect_retry_after_header=True,
    ).session


def _run_pipeline_with_retry(
    pipeline: dlt.Pipeline,
    source,
    *,
    max_attempts: int = 3,
    base_delay: float = 5.0,
    max_delay: float = 60.0,
    retryable_exceptions: tuple[type[BaseException], ...] = _RETRYABLE_EXCEPTIONS,
    **run_kwargs: Any,
):
    """Run pipeline.run with short, readable retry logs."""
    attempt = 1
    while True:
        try:
            return pipeline.run(source, **run_kwargs)
        except retryable_exceptions as exc:
            error_msg = _first_error_message(exc)
            if attempt >= max_attempts:
                logger.error(
                    "PIPELINE_RETRY_EXHAUSTED | pipeline=%s | attempts=%s | type=%s | error=%s",
                    pipeline.pipeline_name,
                    attempt,
                    type(exc).__name__,
                    error_msg,
                )
                raise

            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            delay += random.uniform(0, delay * 0.1)
            logger.warning(
                "PIPELINE_RETRY | pipeline=%s | attempt=%s/%s | retry_in=%.1fs | type=%s | error=%s",
                pipeline.pipeline_name,
                attempt,
                max_attempts,
                delay,
                type(exc).__name__,
                error_msg,
            )
            time.sleep(delay)
            attempt += 1


def rest_api_to_adls_source(
    base_url: str,
    resources: list[dict | str],
    default_params: dict | None = None,
    session: DltSession | None = None,
):
    normalized_resources = []
    for resource in resources:
        if isinstance(resource, str):
            normalized_resources.append(resource)
        elif isinstance(resource, dict):
            normalized_resources.append(resource)
        else:
            raise TypeError(f"Unsupported resource type: {type(resource)!r}")

    return rest_api_source(
        {
            "client": {
                "base_url": base_url,
                "session": session or _build_retrying_session(),
            },
            "resource_defaults": {"endpoint": {"params": default_params or {}}},
            "resources": normalized_resources,
        }
    )


def load_rest_api_to_adls(
    bucket_url: str,
    dataset_name: str,
    pipeline_name: str,
    base_url: str,
    resources: list[dict | str],
    load_mode: str = "full",
    default_params: dict | None = None,
    primary_key: str | None = None,
    layout: str = "{table_name}",
    max_retry_attempts: int = 3,
    retry_base_delay: float = 5.0,
):
    """Load one or many REST resources into ADLS / Azurite."""
    normalized_mode = load_mode.lower()
    if normalized_mode not in {"full", "delta"}:
        raise ValueError("load_mode must be 'full' or 'delta'")
    if not resources:
        raise ValueError("resources must contain at least one element")

    resolved_bucket_url = resolve_storage_bucket_url(bucket_url)
    destination = build_adls_destination(bucket_url=resolved_bucket_url, layout=layout)
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
    )

    source = rest_api_to_adls_source(
        base_url=base_url,
        resources=resources,
        default_params=default_params,
    )

    resource_name = resources[0]["name"] if isinstance(resources[0], dict) and "name" in resources[0] else str(resources[0])

    try:
        can_connect, error_msg = check_connection(source, resource_name)
    except _RETRYABLE_EXCEPTIONS as exc:
        raise ConnectionError(f"Unable to reach API {base_url}: {_first_error_message(exc)}") from exc

    if not can_connect:
        raise ConnectionError(error_msg or f"Connection check failed for API {base_url}")

    hints: dict[str, Any] = {}
    if primary_key:
        hints["primary_key"] = primary_key
    if normalized_mode == "delta":
        hints.update({"table_format": "delta", "write_disposition": "append"})
    else:
        hints.update({"file_format": "parquet", "write_disposition": "replace"})

    for resource in source.resources.values():
        resource.apply_hints(**hints)

    logger.info(
        "API_TO_ADLS_START | pipeline=%s | base_url=%s | dataset=%s | mode=%s | destination=%s",
        pipeline_name,
        base_url,
        dataset_name,
        load_mode,
        "AZURITE" if USE_AZURITE else "ADLS",
    )

    try:
        result = _run_pipeline_with_retry(
            pipeline,
            source,
            max_attempts=max_retry_attempts,
            base_delay=retry_base_delay,
        )
    except Exception:
        logger.exception(
            "API_TO_ADLS_FAILED | pipeline=%s | base_url=%s | dataset=%s",
            pipeline_name,
            base_url,
            dataset_name,
        )
        raise

    logger.info("API_TO_ADLS_DONE | pipeline=%s | rows=%s", pipeline_name, result)
    return result


def run_rest_api_to_adls_pipeline(
    bucket_url: str,
    dataset_name: str,
    pipeline_name: str,
    base_url: str,
    resources: list[dict | str],
    load_mode: str = "full",
    default_params: dict | None = None,
    primary_key: str | None = None,
    layout: str = "{table_name}",
    max_retry_attempts: int = 3,
    retry_base_delay: float = 5.0,
):
    return load_rest_api_to_adls(
        bucket_url=bucket_url,
        dataset_name=dataset_name,
        pipeline_name=pipeline_name,
        base_url=base_url,
        resources=resources,
        load_mode=load_mode,
        default_params=default_params,
        primary_key=primary_key,
        layout=layout,
        max_retry_attempts=max_retry_attempts,
        retry_base_delay=retry_base_delay,
    )
