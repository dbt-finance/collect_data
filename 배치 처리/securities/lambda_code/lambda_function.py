import requests
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
from datetime import timedelta
import pyarrow.parquet as pq
import math

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    s3_key=event['s3_key']
    service_key =event['service_key']
    bas_url=event['bas_url']
    sub_url=event['sub_url']
    day_ago= datetime.strptime(event['execution_date'],'%Y%m%d') -timedelta(days=1)
    str_day = day_ago.strftime("%Y%m%d")
    
    page = 1
    total = 1
    max_page = 1
    data_list = []

    while True:
        url = f'http://apis.data.go.kr/1160100/service{bas_url}{sub_url}?serviceKey={service_key}&pageNo={page}&numOfRows=10000&resultType=json&basDt={str_day}'
        
        res = requests.get(url)
        data = res.json()
        
        data_list.extend(data['response']['body']['items']['item'])
        if page == 1:
            total = int(data['response']['body']['totalCount'])
            max_page = math.ceil(total / 10000)

        page += 1

        if page > max_page:
            break
    
    df = pd.DataFrame(data_list)
    
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow')
    
    bucket_name = 'de-1-2-dokyo'
    object_key = s3_key  +'.parquet'
    
    s3.put_object(Body=parquet_buffer.getvalue(), Bucket=bucket_name, Key=object_key)


    return {
        'statusCode': 200,
        'body': 'Data fetched and saved to S3 as Parquet'
    }
