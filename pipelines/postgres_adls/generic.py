import os
import sys
import socket
import threading
import dlt
from dlt.destinations import filesystem
from dlt.sources.sql_database import sql_table
import pyarrow as pa
import pyarrow.compute as pc

# -----------------------------------------------------------------------------
# 0. PROXY TCP PYTHON (Contournement de 127.0.0.1:10000 -> azurite:10000)
# -----------------------------------------------------------------------------
def start_tcp_proxy(local_port=10000, remote_host="azurite", remote_port=10000):
    """
    Proxy TCP purement Python (standard library) pour intercepter
    127.0.0.1:10000 (force par delta-rs avec use_emulator) et rediriger vers azurite:10000.
    """
    def handle_client(client_socket):
        try:
            remote_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            remote_socket.connect((remote_host, remote_port))
        except Exception as err:
            client_socket.close()
            return

        def forward(src, dst):
            try:
                while True:
                    data = src.recv(8192)
                    if not data:
                        break
                    dst.sendall(data)
            except Exception:
                pass
            finally:
                try:
                    src.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    dst.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass

        t1 = threading.Thread(target=forward, args=(client_socket, remote_socket), daemon=True)
        t2 = threading.Thread(target=forward, args=(remote_socket, client_socket), daemon=True)
        t1.start()
        t2.start()

    def server_loop():
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server_socket.bind(("127.0.0.1", local_port))
            server_socket.listen(128)
            print(f"🔌 Proxy TCP local actif : 127.0.0.1:{local_port} ➔ {remote_host}:{remote_port}")
            while True:
                client_sock, _ = server_socket.accept()
                threading.Thread(target=handle_client, args=(client_sock,), daemon=True).start()
        except Exception as e:
            print(f"⚠️ Avertissement Proxy TCP : {e}")

    threading.Thread(target=server_loop, daemon=True).start()


# -----------------------------------------------------------------------------
# 1. LECTURE DES VARIABLES D'ENVIRONNEMENT
# -----------------------------------------------------------------------------
try:
    pipeline_id = os.environ["DLT_PIPELINE_ID"]
    source_schema = os.environ["DLT_SOURCE_SCHEMA"]
    dataset_name = os.environ["DLT_DATASET_NAME"]
    source_table = os.environ["DLT_SOURCE_TABLE"]
    target_name = os.environ["DLT_TARGET_NAME"]
    partition_col = os.environ.get("DLT_PARTITION_COL", "").strip()
    backend = os.environ["DLT_BACKEND"].lower()
    chunk_size = int(os.environ["DLT_CHUNK_SIZE"])
    write_strategy = os.environ["DLT_WRITE_STRATEGY"].lower()
    use_azurite = os.environ.get("USE_AZURITE", "false").lower() in ("true", "1", "yes")
except KeyError as e:
    print(f"❌ ERREUR CRITIQUE : Variable {e} absente.")
    sys.exit(1)


# Démarrage du proxy local uniquement en mode Azurite
if use_azurite:
    start_tcp_proxy(local_port=10000, remote_host="azurite", remote_port=10000)


# -----------------------------------------------------------------------------
# 2. TRANSFORMER DLT SUR DATE MÉTIER
# -----------------------------------------------------------------------------
def add_date_partitions(table: pa.Table) -> pa.Table:
    if not partition_col or partition_col not in table.column_names:
        return table
    col = table.column(partition_col)
    return (
        table.append_column("Year", pc.year(col))
        .append_column("Month", pc.month(col))
        .append_column("Day", pc.day(col))
    )


def run_export_pipeline():
    print(f"🚀 Export DLT Delta Lake - Strategy: {write_strategy}")
    print(f"📦 Source : {source_schema}.{source_table} -> Cible : {dataset_name}/{target_name}")

    # -------------------------------------------------------------------------
    # 3. CRÉATION DE LA RESSOURCE SOURCE
    # -------------------------------------------------------------------------
    dlt_kwargs = {
        "table": source_table,
        "schema": source_schema,
        "backend": backend,
        "chunk_size": chunk_size,
    }

    if backend == "pyarrow":
        dlt_kwargs["reflection_level"] = "full_with_precision"

    resource = sql_table(**dlt_kwargs)

    if target_name != source_table:
        resource = resource.with_name(target_name)

    # -------------------------------------------------------------------------
    # 4. PARTITIONNEMENT DELTA LAKE
    # -------------------------------------------------------------------------
    columns_hints = {}
    if partition_col:
        print(f"📅 Partitionnement Delta Lake activé sur la colonne : {partition_col}")
        
        resource.add_map(add_date_partitions)
        
        columns_hints = {
            "Year": {"partition": True, "data_type": "bigint"},
            "Month": {"partition": True, "data_type": "bigint"},
            "Day": {"partition": True, "data_type": "bigint"},
        }

    resource.apply_hints(
        write_disposition=write_strategy,
        table_format="delta",
        columns=columns_hints if partition_col else None,
    )

    # -------------------------------------------------------------------------
    # 5. RESOLUTION DE LA DESTINATION (MODE EMULATEUR LOCAL)
    # -------------------------------------------------------------------------
    bucket_url = os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"]

    if use_azurite:
        destination_obj = filesystem(
            bucket_url=bucket_url,
            deltalake_storage_options={
                "use_emulator": "true",
                "azure_storage_allow_http": "true",
                "azure_storage_account_name": "devstoreaccount1",
                "azure_storage_account_key": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
            },
        )
    else:
        destination_obj = filesystem(
            bucket_url=bucket_url,
            deltalake_storage_options={
                "timeout": "60s",
                "max_retries": "3",
            },
        )

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_id,
        destination=destination_obj,
        dataset_name=dataset_name,
    )

    load_info = pipeline.run(resource)

    print("\n✅ Export Delta Lake terminé avec succès !")
    print(load_info)


if __name__ == "__main__":
    run_export_pipeline()