from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from hooks.custom_postgres_hook import CustomPostgresHook
from airflow.utils.dates import days_ago

# Define the start and end dates
start_date = pendulum.datetime(2023, 4, 19, tz='Asia/Seoul')
end_date = pendulum.datetime(2023, 5, 31, tz='Asia/Seoul')

with DAG(
        dag_id='dags_python_with_custom_hook_bulk_load',
        start_date=start_date,
        schedule=None,  # Since we'll manage the schedule manually
        catchup=False
) as dag:
    
    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter=',', is_header=True, is_replace=True)

    # Create a task for each date in the range
    current_date = start_date
    while current_date <= end_date:
        formatted_date = current_date.format('YYYYMMDD')
        file_path = f'/opt/airflow/files/TbCorona19CountStatus/TbCorona19CountStatus.csv'
        
        insrt_postgres_task = PythonOperator(
            task_id=f'insrt_postgres_{formatted_date}',
            python_callable=insrt_postgres,
            op_kwargs={
                'postgres_conn_id': 'conn-db-postgres-custom',
                'tbl_nm': 'TbCorona19CountStatus_bulk2',
                'file_nm': file_path
            }
        )
        
        current_date = current_date.add(days=1)