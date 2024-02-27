from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from airflow.decorators import task
import pendulum
import datetime

with DAG (
    dag_id='dags_empty_with_edge_label',
    start_date=pendulum.datetime(2024,2,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag :
    
    @task(task_id='empty_1')
    def empty_1():
        print('empty_1')
    
    @task(task_id='empty_2')
    def empty_2():
        print('empty_2')
    
    empty_1() >> Label('1과 2사이') >> empty_2()
    
    empty_3 = EmptyOperator(
        task_id ='empty_3'
    )
    
    empty_4 = EmptyOperator(
        task_id ='empty_4'
    )
    
    empty_5 = EmptyOperator(
        task_id ='empty_5'
    )
    
    empty_6 = EmptyOperator(
        task_id ='empty_6'
    )
    
    empty_2() >>Label('start branch') >> [empty_3, empty_4,empty_5]>>Label('end branch') >> empty_6