from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import os
import csv
import random

# 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG 정의
dag = DAG(
    'back_fill_test',
    default_args=default_args,
    description='Generate file and insert into PostgreSQL using XCom for file passing',
    schedule_interval='*/1 * * * *',
    catchup=True
)

# Task 1: 파일 생성 함수
def generate_txt(**kwargs):
    execution_time = kwargs['execution_date'].strftime('%Y-%m-%d-%H-%M')  # 파일명에 시간을 포함
    full_execution_time = kwargs['execution_date'].strftime('%Y-%m-%d-%H-%M')  # 파일 내 시간 기록
    
    # 파일 경로 설정
    file_name = f"/opt/airflow/files/test1/data_{execution_time}.txt"
    
    # 파일 생성 및 저장
    with open(file_name, 'w') as f:
        f.write("id,full_execution_time,random_number\n")
        for i in range(1, 11):
            random_number = random.randint(1, 100)  # 1에서 100 사이의 랜덤 숫자 생성
            f.write(f"{i},{full_execution_time},{random_number}\n")
    
    print(f"File {file_name} has been generated.")
    
    # 파일명을 XCom으로 반환
    return file_name

# PythonOperator로 Task 1 정의 (파일 생성 작업)
t1 = PythonOperator(
    task_id='generate_txt',
    python_callable=generate_txt,
    provide_context=True,
    dag=dag,
)

# Task 2: 파일을 PostgreSQL에 삽입하는 함수
def insert_into_postgres(**kwargs):
    # XCom으로 전달된 파일명 가져오기
    file_name = kwargs['ti'].xcom_pull(task_ids='generate_txt')
    
    if not file_name or not os.path.exists(file_name):
        raise FileNotFoundError(f"File {file_name} not found.")
    
    # Postgres 연결
    pg_hook = PostgresHook(postgres_conn_id='airflow_db')  # Airflow DB 연결 (postgres_conn_id는 미리 설정되어 있어야 함)
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    # 텍스트 파일에서 데이터 읽어오기
    with open(file_name, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # PostgreSQL에 데이터 삽입
            cursor.execute(
                """
                INSERT INTO public.test1 (id, full_execution_time, random_number)
                VALUES (%s, %s, %s)
                """,
                (row['id'], row['full_execution_time'], row['random_number'])
            )

    # 변경 사항 커밋
    connection.commit()
    cursor.close()
    connection.close()
    print(f"Data from {file_name} inserted into PostgreSQL.")

# PythonOperator로 Task 2 정의 (DB 삽입 작업)
t2 = PythonOperator(
    task_id='insert_into_postgres',
    python_callable=insert_into_postgres,
    provide_context=True,
    dag=dag,
)

# Task 1 (파일 생성) -> Task 2 (DB 삽입)
t1 >> t2
