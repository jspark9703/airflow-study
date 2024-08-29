import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


with DAG(
    dag_id= "dag_python_with_postgres",
    schedule=None,
    start_date=pendulum.datetime(2024,8,27, tz="Asia/Seoul"),
    catchup=False
) as dag :
    
    def insrt_postgres(postgres_conn_id, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import psycopg2
        from contextlib import closing 
        postgres_hook = PostgresHook(postgres_conn_id)
        with closing(postgres_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get("ti").dag_id
                task_id = kwargs.get("ti").task_id
                run_id = kwargs.get("ti").run_id
                msg = "insrt 수행"
                sql = "insert into py_opr_drct_insrt values (%s,%s,%s,%s);"
                cursor.execute(sql,(dag_id, task_id, run_id,msg))
                conn.commit()
    insrt_postres = PythonOperator(
        task_id = 'insrt_postgres',
        python_callable=insrt_postgres,
        op_args={"postgres_conn_id":"conn-db-postgres-custom"}
    )
    
    insrt_postgres