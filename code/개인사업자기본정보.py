import requests
import pandas as pd
import time

# 시작 시간 기록
start_time = time.time()

total_page = 100

base_url = 'http://openapi.fsc.go.kr/service/GetSBProfileInfoService/getOtlInfo?serviceKey=YeAuEKy2TOoVsF8mQ72JLW0%2FRj17Xa8JeFqJGO6DZoTiCVsbNGeGJY6oyqi8ZfwEJWWxDpM37pbe22VMFtE5wg%3D%3D&resultType=json'

all_data = []

for page in range(1, total_page + 1):
    url = f"{base_url}&pageNo={page}"
    res = requests.get(url)

    if res.status_code == 200:
        data = res.json()
        items = data["response"]["body"]["items"]["item"]
        all_data.extend(items)


    else:
        print(f"페이지 {page} 데이터를 가져오는데 실패했습니다.")

# DataFrame으로 변환
# columns = all_data[0].keys() 
df = pd.DataFrame(all_data)
df.columns = ['OFFER_DATE', 'GENDER', 'AGE_GROUP', 'ESTABLISHED_YEAR', 'REGION', 'INDUSTRY_CD', 'INDUSTRY_NM', 'EMPLOYEE_NUM']
df.to_csv('csv/개인사업자기본정보.csv',index=False, encoding="utf-8-sig")



# 시간 체크
end_time = time.time()
execution_time = end_time - start_time

print("모든 페이지 데이터 완료")
print(f"코드 실행 시간: {execution_time:.4f} 초")


