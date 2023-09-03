## 배치 처리 아키텍쳐
![batch](https://github.com/dbt-finance/collect_data/assets/98998726/75533891-a8e7-440c-a917-caf60ca4a34b)

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
