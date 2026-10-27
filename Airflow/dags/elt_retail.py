"""ELT de retail_db: el mismo recorrido que se hace a mano en el nodo lab.

    dlt (EL): mysql-retail -> /workspace/data/retail.duckdb (esquema retail_raw)
    dbt (T):  retail_raw -> main_staging -> main_marts

DuckDB admite un solo proceso escribiendo: si un cuaderno del lab tiene el
fichero abierto, la tarea falla hasta que se cierre la conexión.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

ELT_BIN = "/opt/elt-venv/bin"
DBT_DIR = "/workspace/dbt/retail"

with DAG(
    dag_id="elt_retail",
    description="dlt (MySQL -> DuckDB) y dbt (staging -> marts)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,  # DuckDB: un único escritor para las ejecuciones del DAG.
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["elt", "retail", "dlt", "dbt"],
) as dag:
    extraer_cargar = BashOperator(
        task_id="dlt_mysql_a_duckdb",
        bash_command=f"{ELT_BIN}/python /workspace/dlt/mysql_to_duckdb.py",
    )

    transformar = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && {ELT_BIN}/dbt run --profiles-dir {DBT_DIR}",
    )

    probar = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && {ELT_BIN}/dbt test --profiles-dir {DBT_DIR}",
    )

    extraer_cargar >> transformar >> probar
