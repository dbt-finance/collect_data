# 프로젝트 소개


## 금융 데이터 수집을 통한 데이터 분석 환경 구축

- 추진배경 및 목표 : Streaming & Batch 금융 데이터를 한 곳에 모아 데이터 분석가를 위한 인사이트 제공
- [<img src="https://img.shields.io/badge/REPORT-4285F4?style=flat&logo=googledocs&logoColor=white"/>](assets%2Ffinal_project_ppt.pdf)

## ️Team member
   | 김수민                            | 김형준                    | 이수영                       | 조주혜                                     | 한기호                                 |
|--------------------------------|------------------------|---------------------------|-----------------------------------------|-------------------------------------|
| - 배치 파이프라인<br/>- 금융위원회| - 실시간 파이프라인<br/>- 암호화폐 | - 실시간 파이프라인<br/>- 배치 파이프라인<br/>- 주식 | - 배치 파이프라인<br/> - 한국부동산원<br/> - 데이터 시각화 | - 배치 파이프라인<br/>- 한국은행<br/>- AWS 인프라 |


<br>

## Usage data
![data_source.png](assets%2Fdata_source.png)

<br>

## Main Tech Stack
| Role          | Stack                                                                                                                                                                                                                                                                                                        |
|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Language      | <img src="https://img.shields.io/badge/python-3776AB?style=flat&logo=python&logoColor=white"/>                                                                                                                                                                                                               |
 | DataLake      | <img src="https://img.shields.io/badge/AWS S3-569A31?style=flat&logo=amazons3&logoColor=white"/>                                                                                                                                                                                                             |
| DataMart      | <img src="https://img.shields.io/badge/AWS Redshift-527FFF?style=flat&logo=amazonredshift&logoColor=white"/>                                                                                                                                                                                                 |
| ETL           | <img src="https://img.shields.io/badge/EMR Spark-E25A1C?style=flat&logo=apache spark&logoColor=white"/><img src="https://img.shields.io/badge/Lambda-FF9900?style=flat&logo=Awslambda&logoColor=white"/> <img src="https://img.shields.io/badge/AWS Glue-232F3E?style=flat&logo=amazonAWS&logoColor=white"/> | 
| ETL Scheduler | <img src="https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=Apacheairflow&logoColor=white"/>                                                                                                                                                                                                       |
| Streaming | <img src="https://img.shields.io/badge/Kafka-23F20?style=flat&logo=Apachekafka&logoColor=white"/> <img src="https://img.shields.io/badge/FastAPI-009688?style=flat&logo=FastAPI&logoColor=white"/> <img src="https://img.shields.io/badge/AWS RDS-527FFF?style=flat&logo=Amazon RDS&logoColor=white"/>       |

<br>

## Project Architecture
![image](https://github.com/dbt-finance/collect_data/assets/54103240/8f2acad9-e83a-40e6-a99c-041463421da5)


<br>

## Batch Pipeline
![image](https://github.com/dbt-finance/collect_data/assets/54103240/217263e6-3ba4-4bdf-a432-f9439dc2e745)



- 1차 적재 : 데이터 소스로부터 MWAA를 통해 일정 시점에 데이터를 추출하여 S3 및 Redshift로 적재
- 2차 가공 : EMR Spark을 통해 데이터 전처리 및 Redshift 통계 데이터 적재
- 시각화 : Redshift에 적재된 데이터를 이용하여 시각화


## Batch data visualization - AWS QuickSight
### [ 경제 지표 데이터 ] 
![eco_indicator.png](assets%2Feco_indicator.png)
---
### [ 부동산 데이터 ]
![real_estate.png](assets%2Freal_estate.png)

<br>

## Realtime Streaming Pipeline
![image](https://github.com/dbt-finance/collect_data/assets/54103240/e10e555c-a57b-47eb-9e77-1dd3e632339c)


1. 실시간 스트리밍 전송 서버
   - FastAPI로 데이터 수집 및 변환
   - Kafka로 topic 생성 및 전송
2. 실시간 스트리밍 수신 서버
   - Kafka로 데이터 수신
   - 실시간으로 변동율 계산
   - AWS RDS에 데이터 적재
   - 임계값 기준 Slack alarm 
<br>

## Streaming Data result
### Realtime Streaming
![GIFMaker_me](https://github.com/dbt-finance/collect_data/assets/54103240/9986d1fa-e4bf-41b5-87b1-304f3fa8553d)


### RDS load & Slack alarm
암호화폐|국내주식|slack alarm|
|------|---|---|
|![image (4)](https://github.com/dbt-finance/collect_data/assets/54103240/aba928c0-5df7-4fb2-aaa2-b7eb01373f4f)|![image (5)](https://github.com/dbt-finance/collect_data/assets/54103240/02771dbc-1fa0-40ea-9570-44c1198ac150)|![스크린샷 2023-09-04 오후 1 51 38](https://github.com/dbt-finance/collect_data/assets/54103240/28d5c090-eef6-41b3-a707-d3a3124ef047)|




<br>


## Producer Setting

### Virtual environment settings & activation

```bash
python3 -m venv venv
source venv/bin/activate
```

### confluent-kafka, fastapi와 같은 패키지 설치

```bash
pip install -r requirements.txt
```

### Environment variable settings

- .env 파일을 작성하고 BOOTSTRAP_SERVERS 변수를 설정하세요.
    
    ```jsx
    BOOTSTRAP_SERVERS=your_bootstrap_servers
    ```
    

### Application execution

- Uvicorn, Hypercorn, Gunicorn 등을 사용하여 FastAPI 애플리케이션을 실행합니다.
    
    ```bash
    uvicorn main:app --reload
    ```
    

### [main.py](http://main.py)

- Binance Market Data: /binance_market_data/{symbol}
주어진 symbol에 대한 Binance 시장 데이터를 반환합니다.
- KuCoin Market Data: /kucoin_market_data/{symbol}
주어진 symbol에 대한 KuCoin 시장 데이터를 반환합니다.
- Upbit Market Data: /upbit_market_data/{symbol}
주어진 symbol에 대한 Upbit 시장 데이터를 반환합니다.
- Bithumb Market Data: /bithumb_market_data/{symbol}
주어진 symbol에 대한 Bithumb 시장 데이터를 반환합니다.

### [schema.py](http://schema.py)

MarketData: 시장 데이터를 정의하는 모델입니다.
아래 항목을 포함합니다: 

`{"opening_price":2221000.0,"high_price":2221000.0,"low_price":2219000.0,"last_price":2219000.0,"price_change":-9000.0,"price_change_percent":-0.4,"volume":3195.5}`

