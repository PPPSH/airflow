
 
from airflow import DAG 
import datetime
import pendulum
 
from airflow.operators.empty import EmptyOperator
 
with DAG(
    dag_id="dags_conn_operator", #airflow에서 보여지는 화면, 파일명이랑 DagID랑 일치하는게 좋음 
    schedule = None,
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False, # 시작일부터 돌릴것이냐(누락본 일자 다 돌릴것이냐)
    
    #dagrun_timeout=datetime.timedelta(minutes=60),
    
    #tags=["example", "example2"],
    #params={"example_key": "example_value"},
) as dag:
     t1 = EmptyOperator( # 객체명
        task_id="t1",   # tast id , 객체명이랑 일치 시키는게 좋음 
    )
     
     t2 = EmptyOperator( # 객체명
        task_id="t2",   # tast id , 객체명이랑 일치 시키는게 좋음 
    )
     
     
     t3 = EmptyOperator( # 객체명
        task_id="t3",   # tast id , 객체명이랑 일치 시키는게 좋음 
    )
     
     t4 = EmptyOperator( # 객체명
        task_id="t4",   # tast id , 객체명이랑 일치 시키는게 좋음 
    )
     
     t5 = EmptyOperator( # 객체명
        task_id="t5",   # tast id , 객체명이랑 일치 시키는게 좋음 
    )
     
     
     t6 = EmptyOperator( # 객체명
        task_id="t6",   # tast id , 객체명이랑 일치 시키는게 좋음 
    )
     
     t7 = EmptyOperator( # 객체명
        task_id="t7",   # tast id , 객체명이랑 일치 시키는게 좋음 
    )
     
     t8 = EmptyOperator( # 객체명
        task_id="t8",   # tast id , 객체명이랑 일치 시키는게 좋음 
    )
     
     
     
     t1 >> [t2, t3] >> t4
     t5 >> t4
     [t7,t4] >> t6 >> t8
     