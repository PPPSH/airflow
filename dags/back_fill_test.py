from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import random

# 기본 설정
default_args = {
    'owner': 'pppsh',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 18),  # 백필을 위해 과거 날짜로 설정
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG 정의
dag = DAG(
    'back_fill_test',
    default_args=default_args,
    description='Generates a txt file with id, date, and random number every minute',
    schedule_interval='*/1 * * * *',  # 매 분 실행
    catchup=False  # 백필을 위한 옵션
)


# Task 1: 텍스트 파일 생성 함수
def generate_txt(**kwargs):
    # UTC execution_date를 사용
    execution_time = kwargs['execution_date'].strftime('%Y-%m-%d-%H-%M')  # 파일명에 시간을 포함하여 저장
    full_execution_time = kwargs['execution_date'].strftime('%Y-%m-%d-%H-%M')  # 파일 내 기록할 전체 시간
    
    file_name = f"/opt/airflow/files/test1/data_{execution_time}.txt"
    
    # 파일 생성 및 저장
    with open(file_name, 'w') as f:
        f.write("id,full_execution_time,random_number\n")
        for i in range(1, 11):
            random_number = random.randint(1, 100)  # 1에서 100 사이의 랜덤 숫자 생성
            f.write(f"{i},{full_execution_time},{random_number}\n")    
  
    print(f"File {file_name} has been generated.")

# PythonOperator로 Task 1 정의
t1 = PythonOperator(
    task_id='generate_txt',
    python_callable=generate_txt,
    provide_context=True,
    dag=dag,
)

