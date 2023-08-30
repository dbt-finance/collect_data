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
        ("RealTradingPriceIndexSvc/v1/getAptRealTradingPriceIndexSize?page=1&perPage=20000", "apt_real_trading_price_index_size"), # 규모별 실거래가격지수
        ("RealTradingPriceIndexSvc/v1/getAptRealTradingPriceIndex?page=1&perPage=20000", "apt_real_trading_price_index"), #지역별 실거래가격지수
        ("HousePriceTrendSvc/v1/getHouseDemandSupplyTrend?page=1&perPage=20000", "house_demand_supply_trend"), # 매매수급동향_전국주택가격동향조사
        ("HousePriceTrendSvc/v1/getHousePercentilePrice?page=1&perPage=20000", "house_percentile_price"), # 주택가격 5분위 배
        ("HousePriceTrendSvc/v1/getHousePriceIndexAge?page=1&perPage=25000", "house_price_index_age"), # 연령별 주택가격지수
        ("HousePriceTrendSvc/v1/getHousePriceIndexSeason?page=1&perPage=25000", "house_price_index_season"), #계절조정 주택가격지수
        ("HousePriceTrendSvc/v1/getHousePriceIndexSize?page=1&perPage=25000", "house_price_index_size"), # 규모별 주택가격지수    
    ]

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

    for url, table in urls_tables :
        s3_key = f'data/real_estate/{table}/{table}'
        data_list = []

        if url == "RealTradingPriceIndexSvc/v1/getAptRealTradingPriceIndex?page=1&perPage=20000" and table == "apt_real_trading_price_index" : 
            size_gbn = [0,1,2,3,4,5]
            for start_data, end_date in date_ranges:
                for size in size_gbn:
                    real_url = f"{base_url_1}{url}{base_url_2}&cond%5BRESEARCH_DATE%3A%3AGTE%5D={start_date}&cond%5BRESEARCH_DATE%3A%3ALTE%5D={end_date}&cond%5BSIZE_GBN%3A%3AEQ%5D={size}"
                    response = requests.get(real_url)
                data = response.json()
        
                if 'data' in data:
                    data_list.extend(data['data'])
        else :                         
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