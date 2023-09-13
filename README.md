# 프로젝트 소개


## 금융 데이터 수집을 통한 데이터 분석 환경 구축

- 추진배경 및 목표 : Streaming & Batch 금융 데이터를 한 곳에 모아 데이터 분석가를 위한 인사이트 제공
- [<img src="https://img.shields.io/badge/REPORT-4285F4?style=flat&logo=googledocs&logoColor=white"/>](assets%2Ffinal_project_ppt.pdf)

## ️팀원소개
   | 김수민                            | 김형준                    | 이수영                       | 조주혜                                     | 한기호                                 |
|--------------------------------|------------------------|---------------------------|-----------------------------------------|-------------------------------------|
| - 배치 파이프라인<br/>- 금융위원회| - 실시간 파이프라인<br/>- 암호화폐 | - 실시간 및 배치 파이프라인<br/>- 주식 | - 배치 파이프라인<br/> - 한국부동산원<br/> - 데이터 시각화 | - 배치 파이프라인<br/>- 한국은행<br/>- AWS 인프라 |

## 사용 데이터

![data_source.png](assets%2Fdata_source.png)
## Main Tech Stack
| Role          | Stack                                                                                                                                                                                                                                                                                                        |
|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Language      | <img src="https://img.shields.io/badge/python-3776AB?style=flat&logo=python&logoColor=white"/>                                                                                                                                                                                                               |
 | DataLake      | <img src="https://img.shields.io/badge/AWS S3-569A31?style=flat&logo=amazons3&logoColor=white"/>                                                                                                                                                                                                             |
| DataMart      | <img src="https://img.shields.io/badge/AWS Redshift-527FFF?style=flat&logo=amazonredshift&logoColor=white"/>                                                                                                                                                                                                 |
| ETL           | <img src="https://img.shields.io/badge/EMR Spark-E25A1C?style=flat&logo=apache spark&logoColor=white"/><img src="https://img.shields.io/badge/Lambda-FF9900?style=flat&logo=Awslambda&logoColor=white"/> <img src="https://img.shields.io/badge/AWS Glue-232F3E?style=flat&logo=amazonAWS&logoColor=white"/> | 
| ETL Scheduler | <img src="https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=Apacheairflow&logoColor=white"/>                                                                                                                                                                                                       |
| Streaming | <img src="https://img.shields.io/badge/Kafka-23F20?style=flat&logo=Apachekafka&logoColor=white"/> <img src="https://img.shields.io/badge/FastAPI-009688?style=flat&logo=FastAPI&logoColor=white"/> <img src="https://img.shields.io/badge/AWS RDS-527FFF?style=flat&logo=Amazon RDS&logoColor=white"/>       |

## 배치 처리 아키텍쳐
![batch](https://github.com/dbt-finance/collect_data/assets/98998726/75533891-a8e7-440c-a917-caf60ca4a34b)

- 1차 적재 : 데이터 소스로부터 MWAA를 통해 일정 시점에 데이터를 추출하여 S3 및 Redshift로 적재
- 2차 가공 : EMR Spark을 통해 데이터 전처리 및 Redshift 통계 데이터 적재
- 시각화 : Redshift에 적재된 데이터를 이용하여 시각화


## 데이터 시각화 - AWS QuickSight
### [ 경제 지표 데이터 ] 
![eco_indicator.png](assets%2Feco_indicator.png)
---
### [ 부동산 데이터 ]
![real_estate.png](assets%2Freal_estate.png)

## 실시간 처리 아키텍쳐
![realtime](https://github.com/dbt-finance/collect_data/assets/98998726/57386470-0809-4a9e-8106-96ecdbb4cb44)

## Producer Setting

### 가상 환경 설정 및 활성화

```bash
python3 -m venv venv
source venv/bin/activate
```

### confluent-kafka, fastapi와 같은 패키지 설치

```bash
pip install -r requirements.txt
```

### 환경 변수 설정

- .env 파일을 작성하고 BOOTSTRAP_SERVERS 변수를 설정하세요.
    
    ```jsx
    BOOTSTRAP_SERVERS=your_bootstrap_servers
    ```
    

### 애플리케이션 실행

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

