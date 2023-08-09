import requests
import csv
import json


data_list = []
base_url = "https://api.odcloud.kr/api/RealEstateTradingBuyerAge/v1/getRealEstateTradingAreaAge?page=1&perPage=20000&serviceKey=EfRw3aOGo7zd4ZuNdkbHJ4iopA%2F0dlXrGy%2Ft5y7573uHnQrrWeD9FXNlNgGtBLUVnokBHCbBUcrWdwGPaFsogg%3D%3D&type=json"

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


csv_file_path = "매입자 연령대별 부동산 거래면적.csv"

with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['region_cd', 'region_nm', 'research_date', 'deal_obj', 'all_area', 'age01_area', 'age02_area', 'age04_area', 'age05_area', 'age06_area', 'age07_area'])

    for item in data_list:
        region_cd = item['REGION_CD']
        region_nm = item['REGION_NM']
        research_date = item['RESEARCH_DATE']
        deal_obj = item['DEAL_OBJ']
        all_area = item['ALL_AREA']
        age01_area = item['AGE01_AREA']
        age02_area = item['AGE02_AREA']
        age03_area = item['AGE03_AREA']
        age04_area = item['AGE04_AREA']
        age05_area = item['AGE05_AREA']
        age06_area = item['AGE06_AREA']
        age07_area = item['AGE07_AREA']
        writer.writerow([region_cd, region_nm, research_date, deal_obj, all_area, age01_area, age02_area, age04_area, age05_area, age06_area, age07_area])


