
from airflow import DAG 
import datetime, pendulum
from  airflow.operators.bash import BashOperator

with DAG(
    dag_id = 'test_dag',
    start_date= None,
    schedule=None,
    catchup=False
) as dag :
    bash_task1 = BashOperator(
        task_id = 'bash_task1',
        bash_command='echo Succescc'
    )

    bash_task2 =BashOperator(
        task_id = 'bash_task2',
        bash_command= 'echo Success @ '
    )

    bash_task >> bash_task2