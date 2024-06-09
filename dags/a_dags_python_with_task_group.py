from airflow import DAG 
import pendulum
import datetime

from airflow.decorators import task
from airflow.utils.task_group import TaskGroup

from airflow.operators.python import PythonOperator




with DAG(
    dag_id="a_dags_python_with_task_group",
    schedule=None,
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    with TaskGroup(group_id='group_1', tooltip='첫번째 그룹') as group_1:
        @task(task_id='task_1')
        def inner_task1():
            print('group 1, task 1')
            

        @task(task_id = 'task_2')
        def inner_task2():
            print('group 1 , task 2')        
            
        inner_task1() >> inner_task2()
        
    with TaskGroup(group_id='group_2', tooltip='두번쨰 그룹') as group_2:
        @task(task_id='task_1')
        def inner_task1():
            print('group 2 , task 1')
        
        @task(task_id = 'task_2')
        def inner_task2():
            print('group 2 , task 2')
            
        inner_task1() >> inner_task2()
        
    group_1 >> group_2