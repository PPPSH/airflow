

from airflow.models.dag import DAG

import pendulum
from airflow.decorators import task



with DAG(
    dag_id="dags_python_show_templates",
    schedule="30 0 * * *",
    start_date=pendulum.datetime(2024, 5, 30, tz="UTC"),
    catchup=True,
    
) as dag:
    
    @task(task_id="python_task")
    def show_templates(**kwarges):
        from pprint import pprint 
        pprint(kwarges)
        
    show_templates()

    
    