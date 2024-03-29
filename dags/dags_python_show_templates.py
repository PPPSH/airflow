

from airflow.models.dag import DAG

import pendulum
from airflow.decorators import task



with DAG(
    dag_id="dags_python_show_templates",
    schedule="30 9 10 * *",
    start_date=pendulum.datetime(2024, 1, 5, tz="Asia/Seoul"),
    catchup=True,
    
) as dag:
    
    @task(task_id="python_task")
    def show_templates(**kwarges):
        from pprint import pprint 
        pprint(kwarges)
        
    show_templates()

    
    