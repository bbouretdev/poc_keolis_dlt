import dlt
from dlt.sources.sql_database import sql_database

SOURCE_CONN = "postgresql://source_user:source_pwd@localhost:5433/source_db"
DEST_CONN = "postgresql://dest_user:dest_pwd@localhost:5434/dest_db"


def load_full_pipeline():

    # 1. Déclarer source SQL
    source = sql_database(
        credentials=SOURCE_CONN,
        chunk_size=50000,  # ton choix archi validé
    )

    # 2. Sélectionner tables (full load)
    pipeline = dlt.pipeline(
        pipeline_name="postgres_full_copy",
        destination=dlt.destinations.postgres(credentials=DEST_CONN),
        dataset_name="public",
    )

    # 3. Charger une table
    load_info = pipeline.run(
        source.with_resources("orders"),
        write_disposition="replace",  # FULL LOAD
    )

    print(load_info)


if __name__ == "__main__":
    load_full_pipeline()