from airflow import DAG
from airflow.operators.python import PythonOperator


with DAG(
    dag_id = 'a_dags_python_bashoperator',
    catchup=False
) as dag:
    def func1():
        print('tttest')
    task_1 = PythonOperator(
        task_id = 'task_1',
        python_callable=func1
    )

    task_1