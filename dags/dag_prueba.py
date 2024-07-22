from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scripts.main import load_measurements_data, load_stations_data

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

def test_postgres_conn():
    from scripts.conexion import conect_to_db
    engine = conect_to_db("config/config.ini", "postgresql")
    if engine:
        print("PostgreSQL connection successful")
    else:
        raise Exception("Failed to connect to PostgreSQL")

with DAG(
    dag_id="dag_luchmeetnet_v2",
    start_date=datetime(2023, 11, 28),
    catchup=False,
    schedule_interval="0 * * * *",
    default_args=default_args
) as dag:
    
    test_conn_task = PythonOperator(
        task_id="test_conn",
        python_callable=test_postgres_conn
    )

    creacion_tablas_task = PostgresOperator(
        task_id='crear_tablas',
        postgres_conn_id="Postgres_conection",
        sql="sql/create_table.sql"
    )

    load_stations_data_task = PythonOperator(
        task_id="load_stations_data",
        python_callable=load_stations_data,
        op_kwargs={
            "config_file": "config/config.ini"
        }
    )

    load_measurements_data_task = PythonOperator(
        task_id="load_measurements_data",
        python_callable=load_measurements_data,
        op_kwargs={
            "config_file": "config/config.ini",
            "start": "{{ data_interval_start }}",
            "end": "{{ data_interval_end }}"
        }
    )

    dummy_end_task = DummyOperator(
        task_id="dummy_end"
    )

    test_conn_task >> creacion_tablas_task >> load_stations_data_task
    load_stations_data_task >> load_measurements_data_task
    load_measurements_data_task >> dummy_end_task
