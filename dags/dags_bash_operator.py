from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dags_bash_operator",  # ui 화면에서 보이는 DAG 이름 , .py 파일명이랑 dag_id 는 일치 시키는게 좋음 
    schedule="0 0 * * *", # 분 시 일 월 요일
    start_date=pendulum.datetime(2021,1,1, tz = "Asia/Seoul"),
    catchup=False, # 위 시작 일부터 돌릴것이냐(한꺼번에 돌려) = True, 오늘부터 돌릴것이냐 = False 
) as dag: 
    
    bash_t1 = BashOperator(
        task_id="bash_t1", # UI Graph에서 보이는 이름 , 객체명과 task_id명은 동일하게 작성하는게 편리함
        bash_command="echo blue sky!",
    )
    
    bash_t2 = BashOperator(
        task_id="bash_t2", # UI Graph에서 보이는 이름 , 객체명과 task_id명은 동일하게 작성하는게 편리함
        bash_command="echo green ground!",
    )
    
    bash_t1 >> bash_t2