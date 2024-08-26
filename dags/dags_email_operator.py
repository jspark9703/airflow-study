from __future__ import annotations
import datetime
import pendulum
from airflow.models.dag import DAG
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2024, 8, 26, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
    params={"example_key": "example_value"},
) as dag:
    send_email_task = EmailOperator(
         task_id = "send_email_task",
         to="jspark9703@gmail.com",
         subject="airflow  성공메일",
         html_content="airflow 작업완료"
    )