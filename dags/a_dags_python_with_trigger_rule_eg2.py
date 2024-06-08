from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException

import pendulum

with DAG(
    dag_id='a_dags_python_with_trigger_rule_eg2',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    
    @task.branch(task_id = 'branch_task1')
    def select_random():
        from random import choice
        lst = ['1','2','3']
        item = choice(lst)
        
        if item == '1':
            return 'upstream_task_1'
        elif item =='2':
            return 'upstream_task_2'
        else :
            return 'upstream_task_3'
        
    @task(task_id = 'upstream_task_1')
    def python_upstream_1():
        print('upstream_task_1')
    
    @task(task_id = 'upstream_task_2')
    def python_upstream_2():
        print('upstream_task_2')
    
    @task(task_id = 'upstream_task_3')
    def python_upstream_3():
        print('upstream_task_3')
    
    
    @task(task_id = 'downstream_task_1', trigger_rule = 'none_skipped')
    def python_downstream_1():
        print('downstream_task_1')
    
    
    select_random() >> [python_upstream_1(), python_upstream_2(),python_upstream_3()] >> python_downstream_1()
    