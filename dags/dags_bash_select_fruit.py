 
from airflow import DAG 
import datetime
import pendulum
 
from airflow.operators.bash import BashOperator
 
with DAG(
    dag_id="dags_bash_select_fruit", #airflow에서 보여지는 화면, 파일명이랑 DagID랑 일치하는게 좋음 
    schedule="10 0 * * *", # 분/시/일/월/요일
    
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False, # 시작일부터 돌릴것이냐(누락본 일자 다 돌릴것이냐)
    
) as dag:
    
    t1_orage = BashOperator(
        tast_id ="t1_orage",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORAGE",
    )
    
    t2_avocado = BashOperator(
        tast_id ="t1_avocado",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh AVOCADO",
    )
    
    t1_orage >> t2_avocado