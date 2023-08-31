from airflow.decorators import task
from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import boto3
import json
import re

#DAG 정의
dag = DAG(
    dag_id = 'sec_product_dag',
    start_date = datetime(2023,8,1),
    schedule = '0 1 * * *',
    catchup = False
)

#url 정보 json 읽기
s3 = boto3.client('s3')
res = s3.get_object(Bucket='de-1-2-dokyo', Key='etc_file/public_data_urls_product.json')
json_value = json.loads(res['Body'].read().decode('utf-8'))

# Task 정의 및 의존성 설정
lambda_invoke_tasks = []
s3_to_redshift_copy_tasks = []

s3_bucket = 'de-1-2-dokyo'
schema = 'raw_data'
service_key = Variable.get("sm_key")

_underscorer1 = re.compile(r'(.)([A-Z][a-z]+)')
_underscorer2 = re.compile('([a-z0-9])([A-Z])')

for i in range(len(json_value['urls'])):
    bas_url = json_value['urls'][i]['base_url']
    for j in range(len(json_value['urls'][i]['api_list'])):
        sub_url = json_value['urls'][i]['api_list'][j]

        tmpStr = sub_url.lstrip('/get')
        subbed = _underscorer1.sub(r'\1_\2',tmpStr)
        table =  _underscorer2.sub(r'\1_\2', subbed).lower()
        
        s3_key = f'data/securities/{table}/{table}'

        payload ={
            "bas_url":bas_url,
            "sub_url":sub_url,
            "service_key":service_key,
            "s3_key": s3_key,
            "execution_date":"{{execution_date.strftime('%Y%m%d')}}"
        }

        lambda_invoke = LambdaInvokeFunctionOperator(
            task_id = f'lambda_invoke_{i}_{j}',
            function_name='g_data_to_s3',
            payload=json.dumps(payload),
            aws_conn_id='aws_default',
            invocation_type='RequestResponse',
            dag=dag
        )

        s3_to_redshift = S3ToRedshiftOperator(
        task_id=f's3_to_redshift_{i}_{j}',
        schema=schema,
        table=table,
        copy_options=['FORMAT AS PARQUET'],
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        aws_conn_id="aws_default",
        redshift_conn_id="redshift_default",
        method="APPEND",
        dag=dag
        )

        
        lambda_invoke_tasks.append(lambda_invoke)
        s3_to_redshift_copy_tasks.append(s3_to_redshift)

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