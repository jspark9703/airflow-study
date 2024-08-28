import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import random

with DAG(
    dag_id = 'dag_trigger_dag_run_operator',
    schedule = '30 9 * * *', # 매일 6시 30분
    start_date = pendulum.datetime(2024, 8, 27, tz = 'Asia/Seoul'),
    catchup = False
) as dag:
    start_task= BashOperator(
        task_id ="start_task",
        bash_command='echo "start"'
    )
    trigger_dag_task = TriggerDagRunOperator(
        task_id = "trigger_dag_task",
        trigger_dag_id="dag_python_operator",
        trigger_run_id=None,
        execution_date="{{data_interval_start}}",
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=None
        
        
    )
  
    start_task >> trigger_dag_task