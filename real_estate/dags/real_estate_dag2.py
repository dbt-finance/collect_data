from airflow.decorators import task
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.models import Variable
import boto3
import json

s3_bucket = 'de-1-2-dokyo'
schema = 'raw_data'
s3_key_prefix = 'data/real_estate'
tables = [
    "house_price_index", #지역별 주택가격지수
    "house_price", #지역별 주택가격
    "house_sale_deposit_rate", # 매매가격대비 전세가격 비율
    "jeonse_rent_change_rate_size", # 규모별 전월세 전환율
    "jeonse_rent_change_rate", # 지역별 전월세 전환율
    "jeonse_snd_trend", # 전세 수급 동향
    "real_estate_trading_area_age" # 매입자 연령대별 부동산 거래면적
]


dag = DAG(dag_id = 'real_estate_dag2', 
        start_date = datetime(2023, 8, 1),
        schedule_interval = '0 1 * * *',
        catchup = False
        )

@task
def invoke_lambda_function():
    lambda_client = boto3.client('lambda', region_name='ap-northeast-1')

    response = lambda_client.invoke(
        FunctionName='real_estate_lambda2',  
        InvocationType='RequestResponse',  
        LogType='Tail', 
        Payload=json.dumps({"service_key": Variable.get("jh_key")})
    )

# Lambda 함수 호출 작업
lambda_invoke_task = invoke_lambda_function()

# 람다 호출 후에 실행되는 작업들
s3_to_redshift_tasks = []

for table_name in tables:
    s3_key = f'{s3_key_prefix}/{table_name}/{table_name}'
    
    s3_to_redshift_task = S3ToRedshiftOperator(
        task_id=f's3_to_redshift_{table_name}',
        schema=schema,
        table=table_name,
        copy_options=['FORMAT AS PARQUET'],
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        aws_conn_id="aws_default",
        redshift_conn_id="redshift_default",
        method="REPLACE",
        dag=dag
    )
    
    s3_to_redshift_tasks.append(s3_to_redshift_task)

lambda_invoke_task >> s3_to_redshift_tasks
