from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys, os

# Agregar src al PYTHONPATH
sys.path.append("/opt/airflow/src")
from etl.extract import extract
from etl.transform import transform_data
from etl.load import save_raw_local, save_processed_local, save_processed_postgres


# Variables base
BASE_DIR = "/opt/airflow"
DATA_RAW = os.path.join(BASE_DIR, "data/raw/shows_raw.csv")
DATA_PROCESSED = os.path.join(BASE_DIR, "data/processed/shows_clean.csv")

DB_URL = "postgresql://airflow:airflow@postgres:5432/airflow"  # mismo que tu docker-compose


# === Funciones para cada task ===
def run_extract(**context):
    df_raw = extract()
    context["ti"].xcom_push(key="raw", value=df_raw.to_json())
    return "Raw extracted"

def run_transform(**context):
    import pandas as pd
    raw_json = context["ti"].xcom_pull(key="raw")
    df_raw = pd.read_json(raw_json)
    df_clean = transform_data(df_raw)
    context["ti"].xcom_push(key="clean", value=df_clean.to_json())
    return "Data transformed"

def run_load(**context):
    import pandas as pd
    raw_json = context["ti"].xcom_pull(key="raw")
    clean_json = context["ti"].xcom_pull(key="clean")

    df_raw = pd.read_json(raw_json)
    df_clean = pd.read_json(clean_json)

    # Guardar local
    save_raw_local(df_raw, DATA_RAW)
    save_processed_local(df_clean, DATA_PROCESSED)

    # Guardar en Postgres
    save_processed_postgres(df_clean, db_url=DB_URL, table_name="shows")
    return "Data loaded"


# === Definición del DAG ===
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 9, 19),
    "retries": 1,
}

with DAG(
    dag_id="movies_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=run_extract,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=run_load,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task
