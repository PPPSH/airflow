 
from airflow import DAG 
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from common.common_func import regist

import random 
 
with DAG(
    dag_id="dags_python_with_op_operator", #airflow에서 보여지는 화면, 파일명이랑 DagID랑 일치하는게 좋음 
    schedule="30 6 * * *", # 분/시/일/월/요일
    
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False # 시작일부터 돌릴것이냐(누락본 일자 다 돌릴것이냐)
    
) as dag:
    
    regist_t1 = PythonOperator(
        task_id='regist_t1',
        python_callable=regist,
        op_args=['pppsh','man','kr','seoul']

    )
    
    regist_t1