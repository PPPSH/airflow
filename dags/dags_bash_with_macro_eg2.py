from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime, pendulum

with DAG(
    dag_id='dags_bash_with_macros_eg2',
    schedule='10 0 * * 6#2',
    start_date=pendulum.datetime(2024,10,5,tz='Asia/Seoul'),
    catchup=False
)as dag:
    # START_DATE : 2주전 월요일, END_DATE: 2주전 토요일
    bash_task2 = BashOperator(
        task_id ='bash_task2',
        env={
            'START_DATE' : '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=19)) | ds }}',
            'END_DATE':'{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=14)) | ds }}',
        },
    bash_command='echo "START_DATE":"$START_DATE" && echo "END_DATE":"$END_DATE"'
    ) 