#KRX 상장종목 정보
from airflow import DAG
from datetime import datetime
import pandas as pd
from io import StringIO
import boto3
from airflow.decorators import task
import requests
import json
import math
from time import sleep
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

s3_bucket = 'de-1-2-dokyo'
schema = 'raw_data'
table = 'krx_listed_info'
s3_key = 'data/securities/' + table

@task
def get_data():
    page = 1
    total = 1
    max_page = 1
    items = []

    while True:
        print(page)
        url = "https://apis.data.go.kr/1160100/service/GetKrxListedInfoService/getItemInfo?serviceKey=JRSwmFXw7pZtbZasZpND9kIP01T8pX0JDSGm0DfEhFp9uTaS6B6xFWde%2BLO1aqrQ0Fl595piXoWXo3l0YGCIYQ%3D%3D&pageNo={}&numOfRows=10000&resultType=json&beginBasDt='20200101'".format(page)
        
        res = requests.get(url)
        data = res.json()
        
        items.extend(data['response']['body']['items']['item'])
        if page == 1:
            total = int(data['response']['body']['totalCount'])
            max_page = math.ceil(total / 10000)

        page += 1

        if page > max_page:
            break

        sleep(1)

    df = pd.DataFrame(items)
    df.columns = ['basDt', 'srtnCd', 'isinCd', 'mrktCtg', 'itmsNm', 'crno', 'corpNm']
    df_dict = df.to_dict(orient='records')
    return json.dumps(df_dict)

@task
def df_to_s3(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='get_data')
    data_frame = pd.read_json(df)
    csv_buffer = StringIO()
    data_frame.to_csv(csv_buffer, index=False)
    s3 = boto3.client('s3')
    bucket_name = 'de-1-2-dokyo'
    object_key = s3_key +'/' + table + '.csv'  # S3 내에서의 파일 경로 및 이름
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=object_key)

    print(f'CSV 데이터 업로드 완료: {object_key}')

with DAG(
    dag_id='Krx_Listed_Info',
    start_date=datetime(2023,8,13),
    schedule='0 10 * * *',
    catchup=False,
) as dag:
    s3_to_redshift = S3ToRedshiftOperator(
        task_id='s3_to_redshift',
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        schema=schema,
        table=table,
        copy_options=['csv', "IGNOREHEADER 1"],
        redshift_conn_id="redshift_default",
        aws_conn_id="aws_default",
        method="REPLACE",
        dag=dag
    )
    data_task = get_data()
    s3_task = df_to_s3(task_id='df_to_s3', provide_context=True)
    data_task >> s3_task >> s3_to_redshift
