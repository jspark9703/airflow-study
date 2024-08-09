from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Python 작업 함수 정의
def process_data():
    print("Processing data...")

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
    'example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # 시작 작업
    start_task = EmptyOperator(
        task_id='start_task'
    )

    
    bash_t1 = BashOperator(
        task_id = "bash t1",
        bash_command="echo whoami"
    )
    
    bash_t2 = BashOperator(
        task_id = "bash t2",
        bash_command="echo $HOSTNAME"
    )
    # 종료 작업
    end_task = EmptyOperator(
        task_id='end_task'
    )

    # 작업 순서 정의
    start_task >> bash_t1 >> bash_t2 >> end_task