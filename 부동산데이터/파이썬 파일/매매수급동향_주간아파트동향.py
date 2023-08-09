import requests
import csv
import json

data_list=[]

base_url= "https://api.odcloud.kr/api/WeeklyAptTrendSvc/v1/getAptTradingMarketTrend?page=1&perPage=30000&serviceKey=EfRw3aOGo7zd4ZuNdkbHJ4iopA%2F0dlXrGy%2Ft5y7573uHnQrrWeD9FXNlNgGtBLUVnokBHCbBUcrWdwGPaFsogg%3D%3D&type=json"
date_ranges = [
    ("20230101", "20231231"),
    ("20220101", "20221231"),
    ("20210101", "20211231"),
    ("20200101", "20201231")
]

for start_date, end_date in date_ranges:
    url = f"{base_url}&cond%5BRESEARCH_DATE%3A%3AGTE%5D={start_date}&cond%5BRESEARCH_DATE%3A%3ALTE%5D={end_date}"

    while True:
        response = requests.get(url)
        data = response.json()

        if 'data' in data:
            data_list.extend(data['data'])
            break

csv_file_path = "매매수급동향_주간아파트동향.csv"

with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['region_cd', 'region_nm', 'research_date', 'buy_dom_indices', 'level_no'])

    for item in data_list:
        region_cd = item['REGION_CD']
        region_nm = item['REGION_NM']
        research_date = item['RESEARCH_DATE']
        buy_dom_indices = item['BUY_DOM_INDICES']
        level_no = item['LEVEL_NO']
        writer.writerow([region_cd, region_nm, research_date, buy_dom_indices, level_no])


