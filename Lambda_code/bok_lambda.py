from datetime import datetime
from io import BytesIO
import pandas as pd
import boto3
import requests
import json
import logging
import time


def lambda_handler(event, context):
    # get_info
    s3_bucket = event['s3_bucket']
    s3_key = event['s3_key']
    service_key = event['service_key']
    first_code = event['first_code']
    second_code = event['second_code']
    cycle = event['cycle']
    start = event['start']
    day = event['day']

    # requests
    logging.info("API CALL TO S3 START")
    urls = f'https://ecos.bok.or.kr/api/StatisticSearch/{service_key}/JSON/kr/1/10000/{first_code}/{cycle}/{start}/{day}/{second_code}/?/?/?'
    res = requests.get(urls)

    try:
        # df -> buffer
        json_data = res.json()['StatisticSearch']['row']
        df = pd.json_normalize(json_data)
        df = df[['STAT_CODE', 'STAT_NAME', 'ITEM_CODE1', 'ITEM_NAME1', 'UNIT_NAME', 'TIME', 'DATA_VALUE']]
        df['DATA_VALUE'] = df['DATA_VALUE'].astype(float)
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', compression='gzip')

        # buffer -> s3
        s3 = boto3.client('s3')
        s3.put_object(Body=parquet_buffer.getvalue(), Bucket=s3_bucket, Key=s3_key)

        # log
        logging.info("API CALL TO S3 SUCCESS")
        return json.dumps({"Status": "Success"})

    except Exception:
        # log
        logging.info("API CALL TO S3 FAIL")
        raise
