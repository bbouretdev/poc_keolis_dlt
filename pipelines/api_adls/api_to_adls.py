from __future__ import annotations

import logging
import os

import dlt
from dlt.sources.helpers.requests import Client as DltHttpClient
from dlt.sources.helpers.requests import Session as DltSession
from dlt.sources.rest_api import rest_api_source


logger = logging.getLogger(__name__)


# Config
DEFAULT_HTTP_RETRY_ATTEMPTS = 5
DEFAULT_HTTP_BACKOFF_FACTOR = 1.0
DEFAULT_HTTP_MAX_RETRY_DELAY = 60.0

DEFAULT_MAX_TABLE_NESTING = 2
DEFAULT_LAYOUT = "{table_name}"

AZURE_ACCOUNT_NAME_ENV = "AZURE_STORAGE_ACCOUNT_NAME"
AZURE_ACCOUNT_KEY_ENV = "AZURE_STORAGE_ACCOUNT_KEY"


def _get_azure_credentials() -> dict[str, str]:
    account_name = os.getenv(AZURE_ACCOUNT_NAME_ENV)
    account_key = os.getenv(AZURE_ACCOUNT_KEY_ENV)

    missing = [
        env_name
        for env_name, value in (
            (AZURE_ACCOUNT_NAME_ENV, account_name),
            (AZURE_ACCOUNT_KEY_ENV, account_key),
        )
        if not value
    ]

    if missing:
        raise ValueError(
            "Missing required Azure environment variables: "
            + ", ".join(missing)
        )

    return {
        "azure_storage_account_name": account_name,
        "azure_storage_account_key": account_key,
    }


def http_session(
    *,
    retry_attempts: int = DEFAULT_HTTP_RETRY_ATTEMPTS,
    backoff_factor: float = DEFAULT_HTTP_BACKOFF_FACTOR,
    max_retry_delay: float = DEFAULT_HTTP_MAX_RETRY_DELAY,
) -> DltSession:
    if retry_attempts < 0:
        raise ValueError("retry_attempts must be >= 0")

    if backoff_factor < 0:
        raise ValueError("backoff_factor must be >= 0")

    if max_retry_delay < 0:
        raise ValueError("max_retry_delay must be >= 0")

    return DltHttpClient(
        request_max_attempts=retry_attempts,
        request_backoff_factor=backoff_factor,
        request_max_retry_delay=max_retry_delay,
        status_codes=(429, 500, 502, 503, 504),
        respect_retry_after_header=True,
    ).session


def rest_api_source(
    *,
    base_url: str,
    resources: list[dict | str],
    default_params: dict | None,
    session: DltSession,
    max_table_nesting: int,
):
    if not base_url:
        raise ValueError("base_url must not be empty")

    if not resources:
        raise ValueError("resources must contain at least one resource")

    if max_table_nesting < 0:
        raise ValueError("max_table_nesting must be >= 0")

    return rest_api_source(
        {
            "client": {
                "base_url": base_url,
                "session": session,
            },
            "resource_defaults": {
                "endpoint": {
                    "params": default_params or {},
                }
            },
            "resources": resources,
        },
        max_table_nesting=max_table_nesting,
    )


def _build_adls_destination(
    *,
    container_name: str,
    layout: str,
):
    if not container_name:
        raise ValueError("container_name must not be empty")

    if not layout:
        raise ValueError("layout must not be empty")

    credentials = _get_azure_credentials()

    return dlt.destinations.filesystem(
        bucket_url=f"az://{container_name}",
        layout=layout,
        file_format="parquet",
        credentials=credentials,
    )


def _apply_resource_hints(
    source,
    resources: list[dict | str],
) -> None:

    for resource_config in resources:
        if isinstance(resource_config, str):
            resource_name = resource_config
            config = {}
        else:
            config = resource_config
            resource_name = config.get("name")

        if not resource_name:
            raise ValueError(
                "Each resource must define a 'name'"
            )

        resource = source.resources.get(resource_name)

        if resource is None:
            raise ValueError(
                f"Resource '{resource_name}' was not found in dlt source"
            )

        hints: dict[str, object] = {}

        primary_key = config.get("primary_key")
        if primary_key:
            hints["primary_key"] = primary_key

        write_disposition = config.get("write_disposition")
        if write_disposition:
            hints["write_disposition"] = write_disposition

        table_format = config.get("table_format")
        if table_format:
            hints["table_format"] = table_format

        file_format = config.get("file_format")
        if file_format:
            hints["file_format"] = file_format

        if hints:
            resource.apply_hints(**hints)

        logger.debug(
            "Resource configured | resource=%s | hints=%s",
            resource_name,
            hints,
        )


def run_rest_api_to_adls_pipeline(
    *,
    container_name: str,
    dataset_name: str,
    pipeline_name: str,
    base_url: str,
    resources: list[dict | str],
    load_mode: str = "full",
    default_params: dict | None = None,
    primary_key: str | None = None,
    layout: str = DEFAULT_LAYOUT,
    retry_attempts: int = DEFAULT_HTTP_RETRY_ATTEMPTS,
    retry_backoff: float = DEFAULT_HTTP_BACKOFF_FACTOR,
    retry_max_delay: float = DEFAULT_HTTP_MAX_RETRY_DELAY,
    max_table_nesting: int = DEFAULT_MAX_TABLE_NESTING,
):
    if not resources:
        raise ValueError(
            "resources must contain at least one resource"
        )

    logger.info(
        "API -> ADLS START | "
        "pipeline=%s | dataset=%s | base_url=%s | "
        "container=%s | mode=%s | resources=%d",
        pipeline_name,
        dataset_name,
        base_url,
        container_name,
        len(resources),
    )

    session = http_session(
        retry_attempts=retry_attempts,
        backoff_factor=retry_backoff,
        max_retry_delay=retry_max_delay,
    )

    source = rest_api_source(
        base_url=base_url,
        resources=resources,
        default_params=default_params,
        session=session,
        max_table_nesting=max_table_nesting,
    )

    _apply_resource_hints(
        source=source,
        resources=resources,
    )

    destination = _build_adls_destination(
        container_name=container_name,
        layout=layout,
    )

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
    )

    try:
        result = pipeline.run(source)

    except Exception:
        logger.exception(
            "API -> ADLS FAILED | "
            "pipeline=%s | dataset=%s | base_url=%s | mode=%s",
            pipeline_name,
            dataset_name,
            base_url,
        )
        raise

    logger.info(
        "API -> ADLS SUCCESS | "
        "pipeline=%s | dataset=%s | mode=%s",
        pipeline_name,
        dataset_name,
    )

    logger.debug(
        "DLT load info | pipeline=%s | load_info=%s",
        pipeline_name,
        result.asstr(verbosity=1),
    )

    return result