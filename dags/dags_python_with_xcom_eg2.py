from airflow import DAG 
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import task
import random 
 
with DAG(
    dag_id="dags_python_with_xcom_eg2", #airflow에서 보여지는 화면, 파일명이랑 DagID랑 일치하는게 좋음 
    schedule="30 6 * * *", # 분/시/일/월/요일
    
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False # 시작일부터 돌릴것이냐(누락본 일자 다 돌릴것이냐)
    
) as dag:
    
   @task(task_id ='python_xcom_push_by_return')
   def xcom_push_result(**kwargs):
       return 'Success'
   
   @task(task_id='python_xcom_pull_1')
   def xcom_pull_1(**kwargs):
       ti =kwargs['ti']
       value1 = ti.xcom_pull(task_ids ='python_xcom_push_by_return')
       print('xcom_pull 메서드로 직접 찾은 리턴값:'+value1)
    
   @task(task_id='python_xcom_pull_2')
   def xcom_pull_2(status,**kwargs):
       print('함수 입력값으로 받은 값:' + status)
       
   python_xcom_push_by_return = xcom_push_result()
   xcom_pull_2(python_xcom_push_by_return)
   python_xcom_push_by_return >> xcom_pull_1
    