from airflow import DAG
import pendulum, datetime
from airflow.decorators import task
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_task_decorator',
    schedule='0 2 * * 1',
    start_date=pendulum.datetime(2024,11,20,tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    @task(task_id = 'python_task_1')
    def print_context(some_input):
        print(some_input)
        
    python_task1 = print_context('task_decorator 실행')
        