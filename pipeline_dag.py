from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Importo le funzioni
from my_functions import extract_and_filter_data,clean_data,upload_to_gcs_and_bigquery

# Configurazione DAG
default_args = {
    'owner': 'Alberto',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Creo il DAG
with DAG(
    'pipeline_dag',
    default_args=default_args,
    description='DAG per eseguire la pipeline di estrazione, pulizia e caricamento dati',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Definisco il task per l'estrazione dei dati
    extract_data_task = PythonOperator(
        task_id='extract_and_filter_data',
        python_callable=extract_and_filter_data,
        op_kwargs={'start_date': '2024-08-01', 'end_date': '2024-08-10'},
    )

    # Definisco il task per la pulizia dei dati
    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
    )

    # Definisco il task per il caricamento su GCS e BigQuery
    upload_task = PythonOperator(
        task_id='upload_to_gcs_and_bigquery',
        python_callable=upload_to_gcs_and_bigquery,
        op_kwargs={
            'bucket_name': 'innovationengineer',
            'project_id': 'innovation-engineer',
            'dataset_id': 'marketing_data',
        },
    )

    # Imposto la sequenza dei task
    extract_data_task >> clean_data_task >> upload_task
