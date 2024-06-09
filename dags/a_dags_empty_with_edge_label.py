
from airflow import DAG 
import pendulum, datetime

from airflow.operators.python import PythonOperator
from airflow.utils.edgemodifier import Label

from airflow.decorators import task

with DAG(
    dag_id="a_dags_empty_with_edge_label",
    schedule=None,
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    
    @task(task_id = 'task1')
    def task_1():
        print('task1')
        
    @task(task_id = 'task2')
    def task_2():
        print('task2')
        
    task_1() >>Label('1과 2사이') >> task_2()