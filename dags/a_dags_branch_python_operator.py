from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.decorators import task

with DAG(
    dag_id='a_dags_branch_python_operator',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'), 
    schedule='0 1 * * *',
    catchup=False
) as dag:
    @task.branch(task_id = 'upload_task1')
    def select_item():
        from random import choice
        item_lst = ['1','2','3']
        selected_item = choice(item_lst)
        
        if select_item == '1':
            return 'task_1'
        
        elif select_item == '2':
            return 'task_2'
        else :
            return 'task_3'
        
    def common_func(**kwargs):
        ti=kwargs['selected']
        print(f'selected is : {ti}')
        
    task_1 = PythonOperator(
        task_id = 'task_1',
        op_kwargs= {'selected':'task1'},
        python_callable=common_func
    )
    
    task_2 =PythonOperator(
        task_id = 'task_2',
        op_kwargs= {'selected':'task2'},
        python_callable=common_func
    )
    
    task_3 = PythonOperator(
        task_id = 'task_3',
        op_kwargs={'selected':'task3'},
        python_callable=common_func
    )
    
    select_item() >> [task_1,task_2,task_3]
    
    
    
