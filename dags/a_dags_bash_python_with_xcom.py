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
    
    bash_push = BashOperator(
        task_id = 'bash_push',
        bash_command="echo push Start"
                    "{{ti.xcom_push(key='key1', value='value1')}} && "
                    "{{ti.xcom_push(key='key2', value='value2')}} &&"
                    "echo push Finish"
    )
    
    bash_pull = BashOperator(
        task_id = 'bash_pull',
        env = {
            "key1": "{{ti.xcom_pull(key='key1')}}",
            "key2": "{{ti.xcom_pull(key='key2')}}",
            "return": "{{ti.xcom_pull(task_ids ='bash_push')}}"
        },
        bash_command="echo result1 is $key1 and key2 is $key2 and return is $return"
    )
    
    bash_push >> bash_pull