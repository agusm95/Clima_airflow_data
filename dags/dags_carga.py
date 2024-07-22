from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from scripts.conexion import conect_to_db, initialize_tables
from scripts.etl_prueba import cargar_configuracion, load_measurements_data

default_args = {
    'start_date': datetime(2024, 7, 18),
    'retries': 1,
    'retry_delay': timedelta(days=1)
}

def verificar_conexion(config_file, section):
    try:
        engine = conect_to_db(config_file, section)
        if engine:
            engine.dispose()
            return True
        return False
    except Exception as e:
        raise ValueError(f"No se puede conectar a la base de datos: {e}")

def ejecutar_initialize_tables(config_file, section, table_name):
    engine = conect_to_db(config_file, section)
    if engine:
        initialize_tables(engine, table_name)

def ejecutar_load_measuments_data(config_file, start, end):
    return load_measurements_data(config_file, start, end)

with DAG(
    'dag_carga',
    default_args=default_args,
    description='DAG para la carga de datos de temperatura',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    verificar_conexion_db = PythonOperator(
        task_id="verificar_conexion_db",
        python_callable=verificar_conexion,
        op_args=['config/config.ini', 'postgresql']
    )

    ejecutar_script_creacion_tabla_final = PythonOperator(
        task_id="ejecutar_script_creacionTablaFinal",
        python_callable=ejecutar_initialize_tables,
        op_args=['config/config.ini', 'postgresql', 'weather_data']
    )

    ejecutar_script_etl_task = PythonOperator(
        task_id="ejecutar_script_etl",
        python_callable=ejecutar_load_measuments_data,
        op_args=['config/config.ini', 'start_time', 'end_time']
    )

    verificar_conexion_db >> ejecutar_script_creacion_tabla_final >> ejecutar_script_etl_task
