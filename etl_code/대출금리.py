import pandas as pd
import requests

# 대출금리,121Y006,BECBLA01,[A,M,Q],199601~202306
urls = 'https://ecos.bok.or.kr/api/StatisticSearch/EQYGCD4OHB16CGJTCC3K/JSON/kr/1/10000/121Y006/M/197001/202308/BECBLA01/?/?/?'
res = requests.get(urls)
json_data = res.json()['StatisticSearch']['row']
df = pd.json_normalize(json_data)
df = df[['STAT_CODE','STAT_NAME','ITEM_CODE1','ITEM_NAME1','UNIT_NAME','TIME','DATA_VALUE']]
df.to_csv('../csv/대출금리.csv',index=False)