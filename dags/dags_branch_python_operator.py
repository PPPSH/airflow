
from airflow import DAG 
import datetime
import pendulum
from airflow.operators.python import BranchPythonOperatora
from airflow.operators.python import PythonOperator
from airflow.models import Variable 



with DAG(
   dag_id='dags_branch_python_operator' ,
   start_date=datetime(2024,2,1),
   schedule=None,
   catchup=False
) as dag:
    def select_random():
        import random
    
        item_lst = ['A','B','C']
        selected_item = random.choice(item_lst)
        if selected_item =='A':
            return 'task_a'
        elif selected_item in ['B','C']:
            return ['task_b','task_c']
        
    python_branch_task = BranchPythonOperatora(
        task_id = 'python_branch_task',
        python_callable=select_random
    )

    
    def common_func(**kwargs):
        print(kwargs['selected'])
    
    
    task_a = PythonOperator(
        task_id = 'task_a',
        python_callable=common_func,
        op_kwargs={'seleted':'A'}    
    )
    
    task_b = PythonOperator(
        task_id = 'task_b',
        python_callable=common_func,
        op_kwargs={'seleted':'B'}    
    )
    
    task_c = PythonOperator(
        task_id = 'task_c',
        python_callable=common_func,
        op_kwargs={'seleted':'C'}
        
    )
    
    python_branch_task >> [task_a,task_b,task_c]
    