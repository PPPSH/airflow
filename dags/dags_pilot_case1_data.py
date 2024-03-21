from airflow import DAG

import datetime
import pendulum

from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator


with DAG (
    dag_id = "dags_pilot_case1_data",
    schedule='0 7 * * 1-5',
    start_date= pendulum.datetime(2024,3,1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    def select_random():
        import random
        
        result_lst = ['Success','Fail']
        resulted = random.choice(result_lst)
        
        if resulted =='Success' :
            return 'next_task'
        elif resulted =='Fail':
            return 'send_email_task'
        
    python_branch_task = BranchPythonOperator(
        task_id = 'python_branch_task',
        python_callable=select_random
    )
    
    def common_func(**kwargs):
        print(kwargs['selected'])
    
    
    next_task = PythonOperator(
        task_id = 'next_task',
        python_callable=common_func,
        op_kwargs={'selected':'Success'}
    )
    
    send_email_task = EmailOperator(
        task_id = "send_email_task",
        to="dbtn751@naver.com",
        subject="Airflow",
        html_content="Data Not yet"
    )
    
    
    python_branch_task >> [next_task,send_email_task]