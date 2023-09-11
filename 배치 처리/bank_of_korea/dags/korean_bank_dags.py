from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow import DAG
from datetime import datetime
import json
import yaml
import boto3

# DAG 정의
default_args = {
    'start_date': datetime(2023, 8, 26),
    'schedule': '0 1 * * *',  # 매일 한국시간 10:00에 실행
    'catchup': False, # 매일 Refresh
}

dag = DAG('korean_bank_dags',
    schedule= '0 1 * * *',
    default_args=default_args
          )

# S3에서 한국은행 API관련 데이터 불러오기
s3 = boto3.client('s3')
res = s3.get_object(Bucket='de-1-2-dokyo', Key='etc_file/korean_bank_code.yaml')
code_data = yaml.safe_load(res['Body'])

# Task 정의 및 의존성 설정
lambda_invoke_tasks = []
s3_to_redshift_copy_tasks = []

for n, data in enumerate(code_data):
    table = data['table']
    cycle = data['cycle']
    first_code = data['first_code']
    second_code = data['second_code']
    schema = data['schema']
    s3_bucket = data['s3_bucket']

    if cycle == 'D':
        s3_key = 'data/korean_bank/' + table + '/' + table + '.parquet.gzip'
        start = '19700101'
        day = datetime.today().strftime("%Y%m%d")
    elif cycle == 'M':
        s3_key = 'data/korean_bank/' + table + '/' + table + '.parquet.gzip'
        start = '197001'
        day = datetime.today().strftime("%Y%m")
    elif cycle == 'A':
        s3_key = 'data/korean_bank/' + table + '/' + table + '.parquet.gzip'
        start = '1970'
        day = datetime.today().strftime("%Y")
    else:
        print("Code Error")

    payload = {
        "cycle": cycle,
        "s3_bucket": s3_bucket,
        "s3_key": s3_key,
        "service_key": Variable.get("bok_secret_key"),
        "first_code": first_code,
        "second_code": second_code,
        "start": start,
        "day": day
    }

    lambda_invoke = LambdaInvokeFunctionOperator(
        task_id=f'lambda_invoke_{table}_{n+1}',
        function_name='bok_lambda',
        payload=json.dumps(payload),
        aws_conn_id='aws_default',
        invocation_type='RequestResponse',
        dag=dag
    )

    s3_to_redshift_copy = S3ToRedshiftOperator(
        task_id=f's3_to_redshift_copy_{table}_{n+1}',
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        schema=schema,
        table=table,
        copy_options=['FORMAT AS PARQUET'],
        redshift_conn_id="redshift_default",
        aws_conn_id="aws_default",
        method="REPLACE",
        dag=dag
    )

    lambda_invoke_tasks.append(lambda_invoke)
    s3_to_redshift_copy_tasks.append(s3_to_redshift_copy)

start_task = EmptyOperator(
    task_id = 'start_dummy',
    dag=dag
)

end_task = EmptyOperator(
    task_id = 'end_dummy',
    dag=dag
)

# 병렬 실행 설정 // UI 관리를 위해 start,end의 task 추가
for lambda_task, copy_task in zip(lambda_invoke_tasks, s3_to_redshift_copy_tasks):
    start_task >> lambda_task >> copy_task >> end_task