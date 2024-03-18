from airflow import DAG 
import datetime
import pendulum

from  airflow.operators.email import EmailOperator


with DAG (
    dag_id = "dags_email_operator",
    schedule="0 0 0 0 0",
    start_date=pendulum.datetime(2023,3,1, tz= "Asia/Seoul"),
    catchup=False
) as dag : 
    
    send_email_task =EmailOperator(
        task_id = "send_email_task",
        to="dbtn751@naver.com",
        subject="Airflow",
        html_content="Airflow Success"
    )
    