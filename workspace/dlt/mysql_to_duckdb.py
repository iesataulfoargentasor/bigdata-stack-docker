"""EL con dlt: copia retail_db (mysql-retail) en DuckDB, en el esquema retail_raw.

dlt no transforma nada: trae las tablas tal cual están en origen y añade dos
columnas de control (_dlt_load_id y _dlt_id). Las transformaciones son cosa de dbt.

    docker compose -f docker-compose.stack.yml exec lab python /workspace/dlt/mysql_to_duckdb.py
"""

import os

import dlt
from dlt.sources.sql_database import sql_database

MYSQL_URL = os.getenv("RETAIL_MYSQL_URL", "mysql+pymysql://retail:retail@mysql-retail:3306/retail_db")
DUCKDB_PATH = os.getenv("RETAIL_DUCKDB_PATH", "/workspace/data/retail.duckdb")


def main():
    origen = sql_database(MYSQL_URL)

    pipeline = dlt.pipeline(
        pipeline_name="retail_mysql_to_duckdb",
        destination=dlt.destinations.duckdb(DUCKDB_PATH),
        dataset_name="retail_raw",
    )

    info = pipeline.run(origen, write_disposition="replace")
    print(info)


if __name__ == "__main__":
    main()
