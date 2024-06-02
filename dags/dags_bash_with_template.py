 
from airflow import DAG 
import datetime
import pendulum
 
from airflow.operators.bash import BashOperator
 
with DAG(
    dag_id="dags_bash_with_templated", 
    schedule="0 0 * * *", # 분/시/일/월/요일
    
    start_date=pendulum.datetime(2024, 5, 15, tz="Asia/Seoul"),
    catchup=False, # 시작일부터 돌릴것이냐(누락본 일자 다 돌릴것이냐)
    
) as dag:
    
     bash_t1 = BashOperator( 
        task_id="bash_t1",   
        bash_command='echo "data_interval_end: {{ data_interval_end}} " ',
    )
     
     bash_t2 = BashOperator( 
        task_id="bash_t2",   
        env ={
            'START_DATE':'{{data_interval_start | ds}}',
            'END_DATE' : '{{data_interval_end | ds}}'
        } ,
        bash_command='echo $START_DATE && echo $END_DATE ',
    )
     
     
     bash_t1 >> bash_t2