from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id = 'a_dags_bash_with_variable',
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    gv = Variable.get('sample_key')
    
    bash_task1 = BashOperator(
        task_id = 'bash_task1',
        bash_command= f'echo var1 is {gv}'
    )
    
    bash_tast2 = BashOperator(
        task_id='bash_task2',
        bash_command='echo var2 is {{var.variable.sample_key}}'
    )
    
    bash_task1 >> bash_tast2