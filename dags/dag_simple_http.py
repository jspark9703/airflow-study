from airflow.providers.http.operators.http import SimpleHttpOperator
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task

with DAG (
    dag_id = 'dag_simple_http',
    schedule = '30 9 * * *', # 매일 6시 30분
    start_date = pendulum.datetime(2024, 8, 27, tz = 'Asia/Seoul'),
    catchup = False
) as dag:
    
    tb_forgeted_things = SimpleHttpOperator(
        task_id = "tb_forgeted_things",
        http_conn_id = "data_forgeted_thing",
        endpoint = "{{var.value.api_key_openapi_seoul_go.kr}}/json/lostArticleBizInfo/1/5/",
        method = "GET",
        headers = {"Content-Type":"application/json",
                   "charset":"utf-8",
                   "Accept": "*/*"
                   }
    )
    
    @task(task_id = "python_2")
    def python_2(**kwargs):
        ti = kwargs["ti"]
        rslt = ti.xcom_pull(task_id = "tb_forgeted_things")
        import json
        from pprint import pprint
        
        pprint(json.loads(rslt))
        
    tb_forgeted_things >> python_2()
    