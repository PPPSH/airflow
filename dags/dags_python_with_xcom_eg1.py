from airflow import DAG 
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import task
import random 
 
with DAG(
    dag_id="dags_python_operator", #airflow에서 보여지는 화면, 파일명이랑 DagID랑 일치하는게 좋음 
    schedule="30 6 * * *", # 분/시/일/월/요일
    
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False # 시작일부터 돌릴것이냐(누락본 일자 다 돌릴것이냐)
    
) as dag:
    
    @task(task_id = 'python_xcom_push_task1')
    def xcom_push1(**kwargs):
        ti=kwargs['ti']
        ti.xcom_push(key='result1',value='value_1')
        ti.xcom_push(key='result2',value=[1,2,3])
        
    @task(task_id = 'python_xcom_push_task2')
    def xcom_push2(**kwargs):
        ti=kwargs['ti']
        ti.xcom_push(key='result1',value='value_2')
        ti.xcom_push(key='result2',value=[1,2,3,4])
        
        
    @task(task_id ='python_xcom_pull_task')
    def xcom_pull(**kwargs):
        ti=kwargs['ti']
        value1 = ti.xcom_pull(key='result1')
        value2 = ti.xcom_pull(key='result2', task_id = 'python_xcom_push_task1')
        print(value1)
        print(value2)
        
    xcom_push1() >> xcom_push2() >> xcom_pull
        
        
        
        