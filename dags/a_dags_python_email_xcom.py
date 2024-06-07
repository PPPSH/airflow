from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="a_dags_python_email_xcom",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    @task(task_id = 'make_result')
    def make_result(**kwargs):
        from random import choice
        return choice['Success','fail']
    
    mailing_task = EmailOperator(
        task_id = 'mailing_task',
        to='dbtn751@naver.com',
        subject="{{data_interval_end.in_timezone('Asia/Seoul') | ds}} 배치 결과",
        html_content="{{data_interval_end.in_timezone('Asia/Seoul') | ds}} 배치는\
            {{ti.xcom_pull(task_ids='make_result')}} 되었습니다. "
    )
    
    make_result() >> make_result()