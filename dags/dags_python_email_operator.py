 
from airflow import DAG 
import datetime
import pendulum
from airflow.operators.email import EmailOperator
from airflow.decorators import task 
 
with DAG(
    dag_id="dags_email_operator", #airflow에서 보여지는 화면, 파일명이랑 DagID랑 일치하는게 좋음 
    schedule="0 8 1 * *", # 분/시/일/월/요일
    
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False # 시작일부터 돌릴것이냐(누락본 일자 다 돌릴것이냐)
    
) as dag:
    
    @task(task_id = 'something_task')
    def some_logic(**kwargs):
        from random import choice
        return choice(['Success','Fail'])
    
    send_email = EmailOperator(
        task_id = 'send_email',
        to='dbtn751@naver.com',
        subject= '{{data_interval_end.in_timezone("Asia/Seoul")|ds}} somelogic 처리결과',
        html_content='{{data_interval_end.in_timezone("Asia/Seoul")|ds}} 처리 결과는 <br> \
                        {{ti.xcom_pull(task_ids="something_task")}} 했습니다 <br>'
    )
    
    some_logic() >> send_email