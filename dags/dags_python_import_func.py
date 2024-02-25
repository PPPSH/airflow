 
from airflow import DAG 
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp 
 
with DAG(
    dag_id="dags_python_import_operator", #airflow에서 보여지는 화면, 파일명이랑 DagID랑 일치하는게 좋음 
    schedule="30 6 * * *", # 분/시/일/월/요일
    
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False # 시작일부터 돌릴것이냐(누락본 일자 다 돌릴것이냐)
    
) as dag:
    
    task_get_sftp = PythonOperator(
        task_id ="task_get_sftp",
        python_callable=get_sftp
    )
    