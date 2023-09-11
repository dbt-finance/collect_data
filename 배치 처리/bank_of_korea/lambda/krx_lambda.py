import json
from datetime import datetime,timedelta
import boto3
import pandas as pd
import requests
from time import sleep


def lambda_handler(event, context):
    # get_info
    day_minus_one = datetime.strptime(event['day'],'%Y%m%d') - timedelta(days=1)
    service_key = event['service_key']
    s3_bucket = event['s3_bucket']
    table = event['table']
    s3_key = 'data/securities/' + table + '/' + f'dt={day_minus_one.strftime("%Y-%m")}' '/' + day_minus_one.strftime('%Y%m%d') + table + '.parquet.gzip'
    basDt = day_minus_one.strftime('%Y%m%d')


    # paging
    pageNo = 1
    total_count = 1
    df = pd.DataFrame()
    while True:
        url = f'https://apis.data.go.kr/1160100/service/GetKrxListedInfoService/getItemInfo?serviceKey={service_key}&numOfRows=10000&pageNo={pageNo}&resultType=json&basDt={basDt}'
        res = requests.get(url)

        # service_key error solved
        if "SERVICE_KEY_IS_NOT_REGISTERED_ERROR" in res.text:
            print("SERVICE_KEY_ERROR")
            sleep(1)
            continue

        try :
            # if return XML(etc Error) then res.json() occurs Error
            dict_data = res.json()
            total_count = dict_data["response"]["body"]['totalCount']

            if dict_data["response"]["body"]['totalCount'] == 0 and day_minus_one.weekday() < 5 :
                raise Exception

            else:
                if pageNo == 1:
                    df = pd.json_normalize(dict_data['response']['body']['items']['item'])
                else :
                    tmp_df = pd.json_normalize(dict_data['response']['body']['items']['item'])
                    df = pd.concat([df,tmp_df],axis=0)
                if pageNo > (total_count // 10000):
                    df.to_parquet('/tmp/tmp.parquet.gzip',engine='pyarrow',compression='gzip')

                    break
                pageNo += 1
                sleep(1)
        except:
            print(res.text)
            raise Exception

    s3 = boto3.client('s3')
    s3.upload_file('/tmp/tmp.parquet.gzip', Bucket=s3_bucket, Key=s3_key)
    return json.dumps({"success": True})