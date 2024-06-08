from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator

with DAG(
    dag_id='a_dags_branch_python_operator',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'), 
    schedule='0 1 * * *',
    catchup=False
) as dag:
    def select_random():
        from random import choice
        
        item_lst = ['1','2','3']
        selected_item = choice(item_lst)
        
        if selected_item == '1':
            return 'task_1'
        
        else:
            return ['task_2', 'task_3'] 
    
    def common_func(**kwargs):
        print(kwargs['selected'])
    
    python_branch_task = BranchPythonOperator(
        task_id ='python_branch_task',
        python_callable= select_random
    )    
    
    task_1 = PythonOperator(
        task_id = 'task_1',
        python_callable=common_func,
        op_kwargs= {'selected':'1'}
    )
    
    task_2 = PythonOperator(
        task_id = 'task_2',
        python_callable=common_func,
        op_kwargs= {'selected':'2'}
    )
    
    task_3 = PythonOperator(
        task_id = 'task_3',
        python_callable=common_func,
        op_kwargs={'selected':'3'}
    )
    
    python_branch_task >>[task_1,task_2,task_3]
    