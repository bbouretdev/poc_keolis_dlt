import io
import os
import dlt
import pandas as pd

# Détection de l'environnement (par défaut Azurite pour tes tests locaux)
IS_LOCAL_AZURITE = os.getenv("USE_AZURITE", "true").lower() == "true"


def export_postgres_to_azurite():
    mode_label = "Azurite Dev (SDK Direct)" if IS_LOCAL_AZURITE else "ADLS Gen2 Prod (DLT Native)"
    print(f"🚀 Démarrage de l'export Postgres -> Azure [{mode_label}]...")

    # ------------------------------------------------------------------
    # 1. ÉTAPE DLT : Extraire les données de Postgres
    # ------------------------------------------------------------------
    # On utilise la source SQL native de dlt (ou sql_database)
    # Ici on configure une pipeline de lecture
    pipeline = dlt.pipeline(
        pipeline_name="postgres_to_azurite_export",
        destination="postgres_dest",  # Connexion à la source DB
        dataset_name="raw_data",
    )

    # Requête SQL pour récupérer les données de la table créée précédemment
    with pipeline.sql_client() as client:
        with client.execute_query("SELECT * FROM raw_data.users_test") as cursor:
            # Récupération des données sous forme de dictionnaires Python
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            data = [dict(zip(columns, row)) for row in rows]

    print(f"📊 {len(data)} lignes récupérées depuis Postgres (raw_data.users_test).")

    if not data:
        print("⚠️ Aucune donnée à exporter.")
        return

    # ------------------------------------------------------------------
    # 2. ÉTAPE D'ÉCRITURE : Dev (SDK Direct) vs Prod (DLT Native)
    # ------------------------------------------------------------------
    if IS_LOCAL_AZURITE:
        # --- MODE DEV LOCAL (AZURITE) ---
        from azure.storage.blob import BlobServiceClient

        # 1. Conversion des dictionnaires en fichier Parquet en mémoire
        df = pd.DataFrame(data)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow", index=False)
        parquet_buffer.seek(0)

        # 2. Upload vers Azurite via le SDK natif
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
        
        # On s'assure que le conteneur de destination existe
        container_name = "target-data"
        try:
            blob_service_client.create_container(container_name)
        except Exception:
            pass  # Déjà existant

        blob_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob="exported_users.parquet"
        )
        
        blob_client.upload_blob(parquet_buffer, overwrite=True)
        print(f"✅ Fichier 'exported_users.parquet' écrit avec succès dans le conteneur Azurite '{container_name}' !")

    else:
        # --- MODE PROD (ADLS GEN2 100% DLT-NATIVE) ---
        # En prod, dlt gère l'écriture Parquet sur ADLS Gen2 nativement via destination="filesystem"
        export_pipeline = dlt.pipeline(
            pipeline_name="postgres_to_adls_native",
            destination=dlt.destinations.filesystem(
                bucket_url="az://target-data",
                file_format="parquet",
            ),
            dataset_name="exports",
        )

        @dlt.resource(name="users_exported", write_disposition="replace")
        def get_data():
            yield data

        load_info = export_pipeline.run(get_data())
        print("✅ Export DLT-native vers ADLS Gen2 terminé !")
        print(load_info)


if __name__ == "__main__":
    export_postgres_to_azurite()