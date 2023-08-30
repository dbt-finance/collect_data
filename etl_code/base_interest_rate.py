from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.models import Variable
from airflow import DAG
from datetime import datetime
import json

cycle = 'D'
first_code = '722Y001'
second_code = '0101000'
schema = 'raw_data'
table = 'base_interest_rate'
s3_bucket = 'de-1-2-dokyo'

if cycle == 'D':
    s3_key = 'data/korean_bank/' + table + '/' + datetime.today().strftime("%Y%m%d") + table + '.parquet.gzip'
    start = '19700101'
    day = datetime.today().strftime("%Y%m%d")
elif cycle == 'M':
    s3_key = 'data/korean_bank/' + table + '/' + datetime.today().strftime("%Y%m") + table + '.parquet.gzip'
    start ='197001'
    day = datetime.today().strftime("%Y%m")
elif cycle == 'A':
    s3_key = 'data/korean_bank/' + table + '/' + datetime.today().strftime("%Y") + table + '.parquet.gzip'
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
    "day" : day
}

with DAG(
    dag_id=table,
    start_date=datetime(2023, 8, 13),
    schedule='0 19 * * *',
    catchup=False,
) as dag:
    lambda_invoke = LambdaInvokeFunctionOperator(
        task_id='lambda_invoke',
        function_name='bok_lambda',
        payload=json.dumps(payload),
        aws_conn_id='aws_default',
        invocation_type='RequestResponse',
        dag=dag
    )
    s3_to_redshift_copy = S3ToRedshiftOperator(
        task_id='s3_to_redshift_copy',
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

lambda_invoke >> s3_to_redshift_copy

