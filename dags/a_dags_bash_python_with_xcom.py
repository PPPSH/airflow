from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="a_dags_bash_python_with_xcom",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id = "python_xcom_push")
    def python_push_xcom(** kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key = "key1", value = "val1")
        ti.xcom_push(key = "key2", value = "val2")

    @task(task_id = "pyton_xcom_pull")
    def python_xcom_pull(**kwargs):
        ti = kwargs['ti']
        key1 = ti.xcom_pull(key="key1")
        key2 = ti.xcom_pull(key="key2")

    python_push_xcom() >> python_xcom_pull()
