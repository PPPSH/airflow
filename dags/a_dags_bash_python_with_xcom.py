from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="a_dags_bash_python_with_xcom",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    bash_push =BashOperator(
        task_id = 'bash_push',
        bash_command='echo start PUSH_BY_BASH '
                    '{{ti.xcom_push(key="key1", value=200)}}'
                    'echo Complete'
    )
    
    bash_pull = BashOperator(
        task_id = 'bash_pull',
        env = {
            'val1' : '{{ti.xcom_pull(key="key1")}}'
        } ,
        bash_command= 'echo val is $val1'
    )
    
    bash_push >> bash_pull