import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator


with DAG(
    dag_id= "dag_python_with_postgres",
    schedule=None,
    start_date=pendulum.datetime(2024,8,27, tz="Asia/Seoul"),
    catchup=False
) as dag :
    
    def insrt_postgres(ip, port, dbname, user, passwd, **kwargs):
        import psycopg2
        from contextlib import closing
        
        with closing(psycopg2.connect(host = ip, dbname = dbname,user = user, password = passwd, port= int(port) )) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get("ti").dag_id
                task_id = kwargs.get("ti").dag_id
                run_id = kwargs.get("ti").dag_id
                msg = "insrt 수행"
                sql = "insert into py_opr_drct_inrt value (%s,%s,%s,%s);"
                cursor.execute(sql,(dag_id,task_id, run_id,msg))
                conn.commit()
    insrt_postres = PythonOperator(
        task_id = 'insrt_postgres',
        python_callable=insrt_postgres,
        op_args=["172.28.0.3", "5432", "airflow", "airflow","airflow"]
    )
    
    insrt_postgres