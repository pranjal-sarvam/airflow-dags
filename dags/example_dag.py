from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

# Set up logging
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_kubernetes_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['example'],
)


def print_hello():
    logger.info("Starting hello_python task")
    print("Hello from Airflow on Kubernetes!")
    logger.info("Hello message printed successfully")
    return "Task completed successfully"


def print_date():
    logger.info("Starting print_date task")
    current_time = datetime.now()
    print(f"Current date: {current_time}")
    logger.info(f"Date printed: {current_time}")
    return "Date printed successfully"


# Tasks using PythonOperator (works great with LocalExecutor)
hello_task = PythonOperator(
    task_id='hello_python',
    python_callable=print_hello,
    dag=dag,
)

date_task = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag,
)

# Set task dependencies
hello_task >> date_task
