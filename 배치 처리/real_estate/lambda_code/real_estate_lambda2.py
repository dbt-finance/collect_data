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
        ("HousePriceTrendSvc/v1/getHousePriceIndex?page=1&perPage=25000", "house_price_index"), #지역별 주택가격지수
        ("HousePriceTrendSvc/v1/getHousePrice?page=1&perPage=30000", "house_price"), #지역별 주택가격
        ("HousePriceTrendSvc/v1/getHouseSaleDepositRate?page=1&perPage=20000", "house_sale_deposit_rate"), # 매매가격대비 전세가격 비율
        ("RentPriceTrendSvc/v1/getJeonseRentChangeRateSize?page=1&perPage=20000", "jeonse_rent_change_rate_size"), # 규모별 전월세 전환율
        ("RentPriceTrendSvc/v1/getJeonseRentChangeRate?page=1&perPage=20000", "jeonse_rent_change_rate"), # 지역별 전월세 전환율
        ("HousePriceTrendSvc/v1/getJeonseSndTrend?page=1&perPage=20000", "jeonse_snd_trend"), # 전세 수급 동향
        ("RealEstateTradingBuyerAge/v1/getRealEstateTradingAreaAge?page=1&perPage=20000", "real_estate_trading_area_age"), # 매입자 연령대별 부동산 거래면적
        ("RealEstateTradingBuyerAge/v1/getRealEstateTradingCountAge?page=1&perPage=20000", "real_estate_trading_count_age"), # 매입자 연령대별 부동산 거래건수
        # ("HousePriceTrendSvc/v1/getJeonseTradingTrend?page=1&perPage=20000", "jeonse_trading_trend"), # 전세 거래 동향 (데이터 X)
        # ("HousePriceTrendSvc/v1/getHouseTradingTrend?page=1&perPage=20000", "house_trading_trend"), # 매매 거래 동향 (데이터 X)
        # ("RentPriceTrendSvc/v1/getRentDemandSupplyTrend?page=1&perPage=20000", "rent_demand_supply_trend"), # 지역별 수급 동향 (데이터 X)
        # ("RentPriceTrendSvc/v1/getRentPriceIndexSize?page=1&perPage=20000", "rent_price_index_size"), # 규모별 월세가격지수 (데이터 X)
        # ("RentPriceTrendSvc/v1/getRentPriceIndex?page=1&perPage=20000", "rent_price_index"), # 지역별 주택유형별 월세가격지수 (데이터 X)
        # ("RentPriceTrendSvc/v1/getRentTradingTrend?page=1&perPage=20000", "rent_trading_trend") # 지역별 거래 동향 (데이터 X)
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