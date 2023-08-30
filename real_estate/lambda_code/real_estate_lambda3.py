import requests
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import pyarrow.parquet as pq


def lambda_handler(event, context):
    service_key = event['service_key'] 
    base_url_1 = "https://api.odcloud.kr/api/"
    base_url_2 = f"&serviceKey={service_key}&type=json"

    urls_tables = [
        ("WeeklyAptTrendSvc/v1/getAptTradingMarketTrend?page=1&perPage=30000", "apt_trading_market_trend"), # 매매수급동향_주간아파트동
        ("WeeklyAptTrendSvc/v1/getAptTradingPriceIndexAge?page=1&perPage=30000", "apt_trading_price_index_age"), # 연령별 거래유형별 가격지수
        ("WeeklyAptTrendSvc/v1/getAptTradingPriceIndexSize?page=1&perPage=30000", "apt_trading_price_index_size"), # 규모별 거래유형별 가격지수
        ("WeeklyAptTrendSvc/v1/getAptTradingPriceIndex?page=1&perPage=30000", "apt_trading_price_index") # 지역별 거래유형별 가격지수  
    ]

    date_ranges = [
        ("20230101", datetime.today().strftime("%Y%m%d")),
        ("20220101", "20221231"),
        ("20210101", "20211231"),
        ("20200101", "20201231")
    ]

    for url, table in urls_tables :
        s3_key = f'data/real_estate/{table}/{table}'
        data_list = []
        for start_date, end_date in date_ranges:
            real_url = f"{base_url_1}{url}{base_url_2}&cond%5BRESEARCH_DATE%3A%3AGTE%5D={start_date}&cond%5BRESEARCH_DATE%3A%3ALTE%5D={end_date}"
        
            response = requests.get(real_url)
            data = response.json()
        
            if 'data' in data:
                data_list.extend(data['data'])
        
        df = pd.DataFrame(data_list)
        
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow')
        
        s3 = boto3.client('s3')
        bucket_name = 'de-1-2-dokyo'
        object_key = s3_key + '.parquet'
        
        s3.put_object(Body=parquet_buffer.getvalue(), Bucket=bucket_name, Key=object_key)

        
    return {
        'statusCode': 200,
        'body': 'Data fetched and saved to S3 as Parquet'
    }