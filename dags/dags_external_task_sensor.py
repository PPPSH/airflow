from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
from datetime import timedelta
from airflow.utils.state import State 

with DAG(
    dag_id='dags_external_task_sensor',
    start_date=pendulum.datetime(2024,9,20, tz='Asia/Seoul'), 
    schedule='0 7 * * *',
    catchup=True
) as dag:
    external_task_sensor_a = ExternalTaskSensor(
        task_id='external_task_sensor_a',
        external_dag_id='dags_branch_python_operator',
        external_task_id='task_a',
        allowed_states=[State.SKIPPED],
        poke_interval=10        #10초
    )

    external_task_sensor_b = ExternalTaskSensor(
        task_id='external_task_sensor_b',
        external_dag_id='dags_branch_python_operator',
        external_task_id='task_b',
        failed_states=[State.SKIPPED],
        poke_interval=10        #10초
    )

    external_task_sensor_c = ExternalTaskSensor(
        task_id='external_task_sensor_c',
        external_dag_id='dags_branch_python_operator',
        external_task_id='task_c',
        allowed_states=[State.SUCCESS],
        poke_interval=10        #10초
    )
    
 