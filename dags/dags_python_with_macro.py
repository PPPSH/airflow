 
from airflow import DAG 
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_macro", #airflow에서 보여지는 화면, 파일명이랑 DagID랑 일치하는게 좋음 
    schedule="10 0 * * *", # 분/시/일/월/요일
    
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=False # 시작일부터 돌릴것이냐(누락본 일자 다 돌릴것이냐)
    
) as dag:
    @task(task_id = 'task_using_macros',
          templates_dict={
              'start_date':'{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1,day=1)) | ds }}',
              'end_date': '{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds }}'
    })  
    def get_datetime_macros(**kwargs):
        templated_dict = kwargs.get('templated_dict') or {}
        if templated_dict :
            start_date = templated_dict.get('start_date') or 'start_date없음'
            end_date = templated_dict.get('end_date') or 'end_date없음'
            print(start_date)
            print(end_date)
            
    
    @task(task_id = 'task_direct_calc')
    def get_datetime_calc(**kwargs):
        from dateutil.relativedelta import relativedelta
        
        data_interval_end = kwargs['data_interval_end']
        prev_month_day_first = data_interval_end.in_timezone("Asia/Seoul") + relativedelta(months=-1, day= 1)
        prev_month_day_last = data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + relativedelta(days=-1)
        print(prev_month_day_first.strftime('%Y-%m-%d'))
        print(prev_month_day_last.strftime('%Y-%m-%d'))
        
        
        