import os
import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import tempfile

# Adding the path to the system path for importing the transformation script
sys.path.insert(0, '/Users/dmitryloschinin/Desktop/big_data_systems_2')
from transform_script import transform

# DAG settings
default_args = {
    'owner': 'dmitryloschinin',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='user_activity_dag',
    default_args=default_args,
    schedule_interval='0 0 5 * *', # Schedule to run on the 5th of each month
    catchup=False,
    max_active_runs=1,
    description='DAG for calculating client activity by products.',
)

# Setting paths directly
AIRFLOW_HOME = "/Users/dmitryloschinin/Desktop/big_data_systems_2"
DATA_PATH = os.path.join(AIRFLOW_HOME, 'data')
INPUT_PATH = os.path.join(DATA_PATH, 'source')
OUTPUT_PATH = os.path.join(DATA_PATH, 'result')

def extract_data(**context):
    """Data extraction function"""
    profit_data = pd.read_csv(os.path.join(INPUT_PATH, 'profit_table.csv'))

    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp_file:
        profit_data.to_csv(tmp_file.name, index=False)
        file_path = tmp_file.name

    context['task_instance'].xcom_push(key="profit_data_path", value=file_path)

def transform_data(**context):
    """Data transformation function"""
    file_path = context['task_instance'].xcom_pull(task_ids='extract', key='profit_data_path')
    profit_data = pd.read_csv(file_path)
    date = context['ds']  # Using Airflow's `ds` macro
    transformed_data = transform(profit_data, date)

    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp_file:
        transformed_data.to_csv(tmp_file.name, index=False)
        transformed_data_path = tmp_file.name

    context['task_instance'].xcom_push(key="transformed_data_path", value=transformed_data_path)
    os.remove(file_path)  # Removing the temporary file after processing

def load_data(**context):
    """Data loading function"""
    transformed_data_path = context['task_instance'].xcom_pull(task_ids='transform', key='transformed_data_path')
    transformed_data = pd.read_csv(transformed_data_path)
    transformed_data.to_csv(os.path.join(OUTPUT_PATH, 'flags_activity.csv'), mode='a', header=False, index=False)
    os.remove(transformed_data_path)  # Removing the temporary file after loading

# Defining the DAG and tasks
with dag:
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task
