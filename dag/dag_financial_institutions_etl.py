from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Default arguments that apply to all tasks in the DAG.
default_args = {
    'owner': 'airflow',                 
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)  # Time to wait between retries.
}

# Instantiate the DAG.
dag = DAG(
    dag_id='financial_institution_etl_dag',
    default_args=default_args,
    schedule_interval='0 0 1 1,4,7,10 *',  # Run on the day after the last day of each quarter

    catchup=False,
)

get_bank_data = PythonOperator(
    task_id='get_bank_data',
    python_callable='/Users/imranmahmood/Projects/alpha-rank-ai/scripts/fetch_data/get_bank_data.py',
    dag=dag,
)

get_credit_union_data = PythonOperator(
    task_id='get_credit_union_data',
    python_callable='/Users/imranmahmood/Projects/alpha-rank-ai/scripts/fetch_data/get_credit_union_data.py',
    dag=dag,
)

transform_credit_union_data = PythonOperator(
    task_id='transform_credit_union_data',
    python_callable='/Users/imranmahmood/Projects/alpha-rank-ai/scripts/transform_data/transform_credit_union_data.py',
    dag=dag,
)

transform_bank_data = PythonOperator(
    task_id='transform_bank_data',
    python_callable='/Users/imranmahmood/Projects/alpha-rank-ai/scripts/transform_data/transform_bank_data.py',
    dag=dag,
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable='/Users/imranmahmood/Projects/alpha-rank-ai/scripts/load_data/write_to_table.py',
    dag=dag,
)
