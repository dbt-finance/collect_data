import pandas as pd
import requests

# 731Y001,0000001
urls = 'https://ecos.bok.or.kr/api/StatisticSearch/EQYGCD4OHB16CGJTCC3K/JSON/kr/1/100000/731Y001/D/19500101/20230808/0000001/?/?/?'
res = requests.get(urls)
json_data = res.json()['StatisticSearch']['row']
df = pd.json_normalize(json_data)
df = df[['STAT_CODE','STAT_NAME','ITEM_CODE1','ITEM_NAME1','UNIT_NAME','TIME','DATA_VALUE']]
df.to_csv('../csv/원_달러.csv',index=False)