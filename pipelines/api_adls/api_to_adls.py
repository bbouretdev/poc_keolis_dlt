from __future__ import annotations

import io
import logging
import random
import time
import os
from typing import Any

import dlt
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources.helpers.requests import Client as DltHttpClient
from dlt.sources.helpers.requests import RequestException as DltRequestException
from dlt.sources.helpers.requests import Session as DltSession
from dlt.sources.rest_api import check_connection, rest_api_source
from dlt.sources.filesystem import filesystem, read_parquet

USE_AZURITE = os.getenv("USE_AZURITE", "true").lower() == "true"

logger = logging.getLogger(__name__)

# Exceptions considered transient: retrying has a chance of succeeding.
# Anything else (schema errors, bad credentials, bad config, 4xx client
# errors other than 429) is raised immediately since a retry would not help.
# DltRequestException covers connection errors, timeouts and HTTP errors
# raised by DLT's own HTTP client (dlt.sources.helpers.requests).
_RETRYABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (
    ConnectionError,
    TimeoutError,
    OSError,
    DltRequestException,
    PipelineStepFailed,
)

CONTAINER_NAME = os.getenv("DLT_AZURE_CONTAINER", "target-data")
FILE_MAPPING_RAW = os.getenv("DLT_FILE_GLOB", "*.parquet")
PIPELINE_ID = os.getenv("DLT_PIPELINE_ID", "api_to_adls_pipeline")
TARGET_SCHEMA = os.getenv("DLT_TARGET_SCHEMA", "dlt")
WRITE_STRATEGY = os.getenv("DLT_WRITE_STRATEGY", "replace").lower()
CHUNK_SIZE = int(os.getenv("DLT_CHUNK_SIZE", "10000"))

DEFAULT_AZURITE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)


def resolve_storage_bucket_url(bucket_url: str | None = None) -> str:
    """Normalise l’URL de destination pour Azurite ou ADLS Gen2."""
    if bucket_url:
        return bucket_url
    return f"az://{CONTAINER_NAME}"


def resolve_azurite_connection_string() -> str:
    """Retourne la chaîne de connexion Azurite locale utilisée par le SDK et DLT."""
    return os.getenv("AZURE_STORAGE_CONNECTION_STRING", DEFAULT_AZURITE_CONNECTION_STRING)


def ensure_azurite_runtime_settings() -> None:
    """Injecte les variables nécessaires au runtime local Azurite."""
    if USE_AZURITE:
        os.environ.setdefault("AZURE_STORAGE_CONNECTION_STRING", resolve_azurite_connection_string())
        os.environ.setdefault("AZURE_STORAGE_ACCOUNT_NAME", "devstoreaccount1")
        os.environ.setdefault("AZURE_STORAGE_ACCOUNT_KEY", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")

def parse_file_mapping(raw_input: str) -> dict:
    """
    Parse la chaîne transmise et retourne un dict {glob_pattern: target_table_name_or_None}.
    Exexmples acceptés :
      - "items.parquet:items, orders.parquet:client_orders"
      - "*.parquet"
    """
    mapping = {}
    tokens = [t.strip() for t in raw_input.split(",") if t.strip()]
    
    for token in tokens:
        if ":" in token:
            file_pattern, table_alias = token.split(":", 1)
            mapping[file_pattern.strip()] = table_alias.strip().lower()
        else:
            mapping[token.strip()] = None
            
    return mapping

MAPPING = parse_file_mapping(FILE_MAPPING_RAW)

def derive_table_name(path: str, alias: str = None) -> str:
    """Si un alias est fourni, l'utilise, sinon déduit le nom depuis le fichier."""
    if alias:
        return alias
    filename = os.path.basename(path)
    base_name = os.path.splitext(filename)[0]
    return base_name.replace("-", "_").replace(" ", "_").lower()

if USE_AZURITE:
    # ------------------------------------------------------------------
    # 1. MODE DEV LOCAL (AZURITE) AVEC STREAMING / CHUNKING PAR BATCHS
    # ------------------------------------------------------------------
    from azure.storage.blob import BlobServiceClient
    import pyarrow.parquet as pq

    @dlt.source
    def source_parquet_data():
        conn_str = os.getenv(
            "AZURE_STORAGE_CONNECTION_STRING",
            (
                "DefaultEndpointsProtocol=http;"
                "AccountName=devstoreaccount1;"
                "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
                "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
            ),
        )

        blob_service_client = BlobServiceClient.from_connection_string(
            conn_str, api_version="2020-08-04"
        )
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        resources = []
        try:
            all_blobs = list(container_client.list_blobs())
        except Exception as e:
            print(f"⚠️ Erreur d'accès au conteneur Azurite '{CONTAINER_NAME}': {e}")
            return []

        for file_pattern, custom_table in MAPPING.items():
            for blob in all_blobs:
                if file_pattern == "*.parquet" or blob.name == file_pattern or blob.name.endswith(file_pattern):
                    blob_name = blob.name
                    target_table_name = derive_table_name(blob_name, custom_table)

                    # Générateur découpant la table Arrow par lots (chunk_size)
                    def make_generator(b_name=blob_name):
                        def item_generator():
                            blob_client = container_client.get_blob_client(b_name)
                            stream = blob_client.download_blob()
                            table = pq.read_table(io.BytesIO(stream.readall()))
                            
                            # REPRODUIT LE COMPORTEMENT DLT NATIVE (CHUNK_SIZE) :
                            # Au lieu de yield toute la table en 1 bloc, on yield par paquets de N lignes
                            for batch in table.to_batches(max_chunksize=CHUNK_SIZE):
                                yield batch.to_pylist()

                        return item_generator

                    res = dlt.resource(
                        make_generator(),
                        name=target_table_name,
                        selected=True,
                    )
                    resources.append(res)

        if not resources:
            print(f"⚠️ Aucun fichier correspondant trouvé dans Azurite '{CONTAINER_NAME}' pour {MAPPING}")

        return resources

else:
    # ------------------------------------------------------------------
    # 2. MODE PROD NATIVE (ADLS GEN2) - STREAMING STREAMÉ PAR DEFAULT
    # ------------------------------------------------------------------
    @dlt.source
    def source_parquet_data():
        resources = []
        
        for file_pattern, custom_table in MAPPING.items():
            files = filesystem(
                bucket_url=f"az://{CONTAINER_NAME}",
                file_glob=file_pattern,
            )
            
            for file_item in files:
                file_path = file_item["file_name"]
                target_table_name = derive_table_name(file_path, custom_table)
                
                single_file = filesystem(
                    bucket_url=f"az://{CONTAINER_NAME}",
                    file_glob=file_path,
                )
                
                res = (single_file | read_parquet()).with_name(target_table_name)
                resources.append(res)

        return resources


def build_adls_destination(
    bucket_url: str,
    layout: str = "{table_name}",
    deltalake_storage_options: dict[str, str] | None = None,
):
    """Crée la destination DLT compatible ADLS et Azurite local.

    En local, on active la chaîne de connexion Azurite avant la création du
    pipeline pour que DLT écrive bien vers le conteneur local sans passer par
    le endpoint ADLS.
    """
    resolved_bucket_url = resolve_storage_bucket_url(bucket_url)

    if USE_AZURITE:
        ensure_azurite_runtime_settings()
        return dlt.destinations.filesystem(
            bucket_url=resolved_bucket_url,
            layout=layout,
            file_format="parquet",
            deltalake_storage_options={
                **(deltalake_storage_options or {"timeout": "60s", "max_retries": "3"}),
                "connection_string": resolve_azurite_connection_string(),
            },
        )

    return dlt.destinations.filesystem(
        bucket_url=resolved_bucket_url,
        layout=layout,
        deltalake_storage_options=deltalake_storage_options
        or {"timeout": "60s", "max_retries": "3"},
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
    """Run ``pipeline.run(source, **run_kwargs)`` with exponential backoff.

    Only ``retryable_exceptions`` trigger a retry; every other exception
    propagates immediately. Delay grows as ``base_delay * 2**(attempt-1)``,
    capped at ``max_delay``, with a small random jitter to avoid retry storms
    across concurrent Airflow tasks.
    """
    attempt = 1
    while True:
        try:
            return pipeline.run(source, **run_kwargs)
        except retryable_exceptions as exception:
            if attempt >= max_attempts:
                logger.error(
                    "PIPELINE RETRY EXHAUSTED | pipeline=%s | attempts=%s | error=%s",
                    pipeline.pipeline_name,
                    attempt,
                    exception,
                )
                raise
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            delay += random.uniform(0, delay * 0.1)
            logger.warning(
                "PIPELINE RETRY | pipeline=%s | attempt=%s/%s | retrying_in=%.1fs | error=%s",
                pipeline.pipeline_name,
                attempt,
                max_attempts,
                delay,
                exception,
            )
            time.sleep(delay)
            attempt += 1


def rest_api_to_adls_source(
    base_url: str,
    resources: list[dict | str],
    default_params: dict | None = None,
    session: DltSession | None = None,
):
    return rest_api_source(
        {
            "client": {
                "base_url": base_url,
                "session": session or _build_retrying_session(),
            },
            "resource_defaults": {"endpoint": {"params": default_params or {}}},
            "resources": resources,
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
    """Load one or more REST API resources into ADLS (or an Azurite-compatible URL).

    ``full`` replaces a Parquet table per resource. ``delta`` appends rows
    into a Delta table, relying on each resource's own incremental/cursor
    configuration (set on ``resources`` before calling this function, the
    same way it is done for ``rest_api_source`` elsewhere in this module).

    Error handling:
      - Connection is verified with ``check_connection`` before any load is
        attempted; failures are raised as ``ConnectionError`` so Airflow can
        distinguish "source unreachable" from "load failed".
      - Transient HTTP errors (429/5xx, timeouts, dropped connections) are
        retried with exponential backoff at two levels: within each HTTP
        request (via the retrying session) and around the whole
        ``pipeline.run`` call (via ``_run_pipeline_with_retry``), in case a
        retry-exhausted request still bubbles up as a pipeline failure.
      - Non-transient errors (bad schema, bad config, auth failure) are not
        retried and propagate immediately.
    """
    normalized_mode = load_mode.lower()
    if normalized_mode not in {"full", "delta"}:
        raise ValueError("load_mode must be 'full' or 'delta'")
    if not resources:
        raise ValueError("resources must contain at least one resource")

    resolved_bucket_url = resolve_storage_bucket_url(bucket_url)
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=build_adls_destination(bucket_url=resolved_bucket_url, layout=layout),
        dataset_name=dataset_name,
    )
    source = rest_api_to_adls_source(
        base_url=base_url,
        resources=resources,
        default_params=default_params,
    )

    resource_name = resources[0]["name"] if isinstance(resources[0], dict) else resources[0]
    try:
        can_connect, error_msg = check_connection(source, resource_name)
    except _RETRYABLE_EXCEPTIONS as exception:
        raise ConnectionError(f"Unable to reach {base_url}: {exception}") from exception
    if not can_connect:
        raise ConnectionError(error_msg)

    hints: dict[str, object] = {}
    if primary_key:
        hints["primary_key"] = primary_key
    if normalized_mode == "delta":
        hints.update({"table_format": "delta", "write_disposition": "append"})
    else:
        hints.update({"file_format": "parquet", "write_disposition": "replace"})
    for resource in source.resources.values():
        resource.apply_hints(**hints)

    logger.info(
        "API TO ADLS START | pipeline=%s | base_url=%s | dataset=%s | mode=%s",
        pipeline_name,
        base_url,
        dataset_name,
        load_mode,
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
            "API TO ADLS FAILED | pipeline=%s | base_url=%s | dataset=%s",
            pipeline_name,
            base_url,
            dataset_name,
        )
        raise
    logger.info("API TO ADLS DONE | pipeline=%s", pipeline_name)
    logger.info(result.asstr(verbosity=2))
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