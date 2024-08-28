from operator.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id= "dags_seoul_api_corona",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2024,8,27, tz="Asia/Seoul"),
    catchup=False
) as dag :
    """대중교통 분실물"""
    tb_lost_article_info = SeoulApiToCsvOperator(
        task_id = "tb_lost_article_info",
        dataset_nm="lostArticleBizInfo",
        path="/opt/airflow/files/TbLostArticleInfo{{data_interval_end.in_timezone('Asia/Seoul') | ds_nodash}}",
        file_name= "TbLostArticleInfo"
    )
    """주차장"""
    tb_use_year_status_view = SeoulApiToCsvOperator(
        task_id = "tb_use_year_status_view",
        dataset_nm="TbUseYearstatusView",
        path="/opt/airflow/files/TbUseYearstatusView{{data_interval_end.in_timezone('Asia/Seoul') | ds_nodash}}",
        file_name= "TbUseYearstatusView"
    )
    
    tb_lost_article_info >> tb_use_year_status_view