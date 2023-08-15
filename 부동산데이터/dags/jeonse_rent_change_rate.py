# 지역별 전월세 전환율
from airflow import DAG
from datetime import datetime
import pandas as pd
from io import StringIO
import boto3
from airflow.decorators import task
import requests
import json
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator


s3_bucket = 'de-1-2-dokyo'
schema = 'raw_data'
table = 'jeonse_rent_change_rate'
s3_key = 'data/real_estate/' + table + '/' + table


@task
def get_data():
    data_list = []
    base_url = "https://api.odcloud.kr/api/RentPriceTrendSvc/v1/getJeonseRentChangeRate?page=1&perPage=20000&serviceKey=EfRw3aOGo7zd4ZuNdkbHJ4iopA%2F0dlXrGy%2Ft5y7573uHnQrrWeD9FXNlNgGtBLUVnokBHCbBUcrWdwGPaFsogg%3D%3D&type=json"
    date_ranges = [
        ("202307", datetime.today().strftime("%Y%m")),
        ("202301", "202306"),
        ("202207", "202212"),
        ("202201", "202206"),
        ("202107", "202112"),
        ("202101", "202106"),
        ("202007", "202012"),
        ("202001", "202006")
    ]


    for start_date, end_date in date_ranges:
        url = f"{base_url}&cond%5BRESEARCH_DATE%3A%3AGTE%5D={start_date}&cond%5BRESEARCH_DATE%3A%3ALTE%5D={end_date}"

        res = requests.get(url)
        data = res.json()

        if 'data' in data:
            data_list.extend(data['data'])
        
    df = pd.DataFrame(data_list)
    # 필요한 컬럼 선택
    selected_columns = ['REGION_CD', 'REGION_NM', 'RESEARCH_DATE', 'APT_TYPE', 'RATE', 'LEVEL_NO']
    df = df[selected_columns]
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
    object_key = s3_key + '.csv'
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=object_key)

    print(f'CSV 데이터 업로드 완료 : {object_key}')


with DAG(
    dag_id = 'jeonse_rent_change_rate',
    start_date = datetime(2023,8,1),
    schedule = '0 10 * * *',
    catchup = False,
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