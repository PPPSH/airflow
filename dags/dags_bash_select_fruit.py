from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime
import pendulum

with DAG(
    dag_id= 'dags_bash_select_fruit',
    schedule='10 0 * * 6#1',
    start_date=pendulum.datetime(2024,10,20,tz='Asia/Seoul'),
    catchup=False
) as dag : 
    
    t1_orange = BashOperator(
        task_id = 't1_orange',
        bash_command='/opt/airflow/plugins/shell/selected_fruit.sh ORAGE',
    )


    t2_avocado = BashOperator(
        task_id = 't2_avocado',
        bash_command='/opt/airflow/plugins/shell/selected_fruit.sh AVOCADO',
    )
    
    t1_orange >> t2_avocado