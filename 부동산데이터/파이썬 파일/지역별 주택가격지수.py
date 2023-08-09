import requests
import csv
import json


data_list = []
base_url = "https://api.odcloud.kr/api/HousePriceTrendSvc/v1/getHousePriceIndex?page=1&perPage=25000&serviceKey=EfRw3aOGo7zd4ZuNdkbHJ4iopA%2F0dlXrGy%2Ft5y7573uHnQrrWeD9FXNlNgGtBLUVnokBHCbBUcrWdwGPaFsogg%3D%3D&type=json"

date_ranges = [
    ("202307", "202312"),
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

    while True:
        response = requests.get(url)
        data = response.json()

        if 'data' in data:
            data_list.extend(data['data'])
            break


csv_file_path = "지역별 주택가격지수.csv"

with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['region_cd', 'region_nm', 'research_date', 'apt_type', 'tr_gbn', 'indices', 'level_no'])

    for item in data_list:
        region_cd = item['REGION_CD']
        region_nm = item['REGION_NM']
        research_date = item['RESEARCH_DATE']
        apt_type = item['APT_TYPE']
        tr_gbn = item['TR_GBN']
        indices = item['INDICES']
        level_no = item['LEVEL_NO']
        writer.writerow([region_cd, region_nm, research_date, apt_type, tr_gbn, indices, level_no])


