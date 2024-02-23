 
from airflow import DAG 
import datetime
import pendulum
 
from airflow.operators.bash import BashOperator
 
with DAG(
    dag_id="dags_bash_operator", #airflow에서 보여지는 화면, 파일명이랑 DagID랑 일치하는게 좋음 
    schedule="0 0 * * *", # 분/시/일/월/요일
    
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False, # 시작일부터 돌릴것이냐(누락본 일자 다 돌릴것이냐)
    
    #dagrun_timeout=datetime.timedelta(minutes=60),
    
    #tags=["example", "example2"],
    #params={"example_key": "example_value"},
) as dag:
     bash_t1 = BashOperator( # 객체명
        task_id="bash_t1",   # tast id , 객체명이랑 일치 시키는게 좋음 
        bash_command="echo whoami",
    )
     
     bash_t2 = BashOperator( # 객체명
        task_id="bash_t2",   # tast id , 객체명이랑 일치 시키는게 좋음 
        bash_command="echo $HOSTNAME",
    )
     
     bash_t1 >> bash_t2