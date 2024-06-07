from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="a_dags_python_email_xcom",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    @task(task_id = 'check_result_task')
    def create_result(**kwargs):
        from random import choice
        return choice(['Success','Fail'])
    
<<<<<<< HEAD
    send_email_task = EmailOperator(
        task_id = 'send_email_task',
        to="dbtn751@naver.com",
        subject= "{{data_interval_end.in_timezone('Asia/Seoul') | ds}} 배치 처리결과" , 
        html_content= "{{data_interval_end.in_timezone('Asia/Seoul') | ds}} 처리 결과는 <br> \
                        {{ti.xcom_pull(task_ids = 'check_result_task')}} 입니다."
    )

    create_result() >> send_email_task
=======
    bash_push = BashOperator(
        task_id = 'bash_push',
        bash_command= "echo start &&"
                      "{{ti.xcom_push(key='key1', value= ''val)}}" 
    )
    
    bash_pull = BashOperator(
        task_id = 'bash_pull',
        env = {
            'id' : "{{ti.xcom_pull(key='val1')}}"
        },
        bash_command= "echo $id"
    )
>>>>>>> 9f02bab (dags x-com study1)
