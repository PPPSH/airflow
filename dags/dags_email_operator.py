 
from airflow import DAG 
import datetime
import pendulum
from airflow.operators.email import EmailOperator
 
with DAG(
    dag_id="dags_email_operator", #airflow에서 보여지는 화면, 파일명이랑 DagID랑 일치하는게 좋음 
    schedule="0 8 1 * *", # 분/시/일/월/요일
    
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False # 시작일부터 돌릴것이냐(누락본 일자 다 돌릴것이냐)
    
) as dag:
    
    send_email_task = EmailOperator(
        task_id ="send_email_task",
        to='dbtn751@naver.com',
        subject="Airflow  성공메일",
        html_content="Airflow  작업이 완료되었습니다. "
    )
    