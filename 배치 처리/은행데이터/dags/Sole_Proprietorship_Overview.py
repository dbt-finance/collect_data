from airflow import DAG
from datetime import datetime
import pandas as pd
from io import StringIO
import boto3
from airflow.decorators import task
import requests
import json
import logging
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator


# s3 관련 정보 
s3_bucket = 'de-1-2-dokyo'
schema = 'raw_data'
table = 'sole_proprietorship_overview'
s3_key = 'data/bank/' + table 


@task
def get_data():
    logging.info(table+":get data")
    all_data = []
    # 개인 사업자 개요 정보 조회
    base_url = 'http://openapi.fsc.go.kr/service/GetSBProfileInfoService/getOtlInfo?serviceKey=YeAuEKy2TOoVsF8mQ72JLW0%2FRj17Xa8JeFqJGO6DZoTiCVsbNGeGJY6oyqi8ZfwEJWWxDpM37pbe22VMFtE5wg%3D%3D'

    for page in range(1, 42):
        url = f"{base_url}&pageNo={page}&numOfRows=9999&resultType=json"
        res = requests.get(url)

        if res.status_code == 200:
            data = res.json()
            items = data["response"]["body"]["items"]["item"]
            all_data.extend(items)

        else:
            print(f"{page} 페이지 데이터를 가져오는데 실패했습니다.")

    df = pd.DataFrame(all_data)
    df = df[['OFFER_DATE', 'GENDER', 'AGE_GROUP', 'ESTABLISHED_YEAR', 'REGION', 'INDUSTRY_CD', 'INDUSTRY_NM', 'EMPLOYEE_NUM']]
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
    dag_id='sole_proprietorship_overview',
    start_date=datetime(2023,8,16),
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