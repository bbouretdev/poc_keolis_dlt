import io
import os
import dlt
import pyarrow as pa
import pyarrow.parquet as pq
from dlt.sources.sql_database import sql_table, sql_database

pipeline_id = os.environ.get("DLT_PIPELINE_ID", "postgres_to_adls_pipeline")
source_schema = os.environ.get("DLT_SOURCE_SCHEMA", "public")
# Multi-tables séparées par des virgules (ex: "orders,items" ou "*")
source_tables_raw = os.environ.get("DLT_SOURCE_TABLE", "*")
target_path = os.environ.get("DLT_TARGET_PATH", "target-data")  # Nom du conteneur
backend = os.environ.get("DLT_BACKEND", "connectorx")
chunk_size = int(os.environ.get("DLT_CHUNK_SIZE", "100000"))

IS_LOCAL_AZURITE = os.getenv("USE_AZURITE", "true").lower() == "true"


def run_export_pipeline():
    mode_label = "Azurite Dev (SDK Direct Multi-Tables)" if IS_LOCAL_AZURITE else "ADLS Gen2 Prod (DLT Native Multi-Tables)"
    print(f"🚀 Démarrage du pipeline DLT Export [{mode_label}]...")

    # Parsing des noms de tables
    if source_tables_raw.strip() == "*":
        tables_list = None
    else:
        tables_list = [t.strip() for t in source_tables_raw.split(",") if t.strip()]

    if IS_LOCAL_AZURITE:
        # ------------------------------------------------------------------
        # WORKAROUND DEV LOCAL (AZURITE MULTI-TABLES)
        # ------------------------------------------------------------------
        from azure.storage.blob import BlobServiceClient

        conn_str = os.getenv(
            "AZURE_STORAGE_CONNECTION_STRING",
            (
                "DefaultEndpointsProtocol=http;"
                "AccountName=devstoreaccount1;"
                "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
                "BlobEndpoint=http://azurite:10000/devstoreaccount1;"
            ),
        )

        blob_service_client = BlobServiceClient.from_connection_string(
            conn_str, api_version="2020-08-04"
        )
        container_client = blob_service_client.get_container_client(target_path)
        try:
            container_client.create_container()
        except Exception:
            pass

        # Si tables_list est None (*), dlt ne peut pas deviner sans introspection.
        # On va créer une source sql_database temporaire pour lister les tables.
        if tables_list is None:
            full_db_source = sql_database(schema=source_schema, backend=backend)
            tables_list = list(full_db_source.resources.keys())

        print(f"📦 Tables à exporter vers Azurite: {tables_list}")

        for t_name in tables_list:
            print(f"🔄 Export de la table '{source_schema}.{t_name}'...")
            
            src_table = sql_table(
                table=t_name,
                schema=source_schema,
                backend=backend,
                chunk_size=chunk_size,
            )

            arrow_tables = []
            for chunk in src_table:
                if chunk is not None:
                    if isinstance(chunk, pa.Table):
                        arrow_tables.append(chunk)
                    elif isinstance(chunk, pa.RecordBatch):
                        arrow_tables.append(pa.Table.from_batches([chunk]))
                    elif isinstance(chunk, list) and len(chunk) > 0:
                        arrow_tables.append(pa.Table.from_pylist(chunk))
                    else:
                        try:
                            arrow_tables.append(pa.Table.from_pandas(chunk))
                        except Exception:
                            pass

            if not arrow_tables:
                print(f"⚠️ Aucune donnée dans la table '{t_name}'. Export sauté.")
                continue

            consolidated_table = pa.concat_tables(arrow_tables)
            parquet_buffer = io.BytesIO()
            pq.write_table(consolidated_table, parquet_buffer)
            parquet_buffer.seek(0)

            blob_name = f"{t_name}.parquet"
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(parquet_buffer.getvalue(), overwrite=True)
            print(f"✅ Table '{t_name}' ({consolidated_table.num_rows} lignes) -> Blob '{blob_name}' dans '{target_path}'")

    else:
        # ------------------------------------------------------------------
        # PIPELINE DLT NATIVE (PROD / ADLS GEN2 MULTI-TABLES)
        # ------------------------------------------------------------------
        source = sql_database(
            schema=source_schema,
            table_names=tables_list,
            backend=backend,
            chunk_size=chunk_size,
        )

        pipeline = dlt.pipeline(
            pipeline_name=pipeline_id,
            destination="filesystem",
            dataset_name=target_path,
        )

        load_info = pipeline.run(
            source,
            write_disposition="replace",
            loader_file_format="parquet",
        )

        print(pipeline.last_trace)
        print(load_info)


if __name__ == "__main__":
    run_export_pipeline()