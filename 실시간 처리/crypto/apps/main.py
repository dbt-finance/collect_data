from fastapi import FastAPI, HTTPException
from confluent_kafka import Producer
from dotenv import load_dotenv
import os
import requests
from schema import MarketData

app = FastAPI()

load_dotenv()

conf = {
    "bootstrap.servers": os.getenv("BOOTSTRAP_SERVERS"),
    "client.id": "crypto-producer",
}
producer = Producer(conf)


@app.get("/binance_market_data/{symbol}", response_model=MarketData)
async def get_binance_ohlcv(symbol: str):
    url = "https://api.binance.com/api/v3/ticker/24hr"
    response = requests.get(url, params={"symbol": symbol})

    if response.status_code != 200:
        raise HTTPException(status_code=404, detail="Binance data not found")

    binance_data = response.json()

    exchange_url = (
        "https://quotation-api-cdn.dunamu.com/v1/forex/recent?codes=FRX.KRWUSD"
    )
    exchange_response = requests.get(exchange_url).json()
    krw_rate = exchange_response[0]["basePrice"]

    market_data = {
        "opening_price": float(binance_data["openPrice"]) * krw_rate,
        "high_price": float(binance_data["highPrice"]) * krw_rate,
        "low_price": float(binance_data["lowPrice"]) * krw_rate,
        "last_price": float(binance_data["lastPrice"]) * krw_rate,
        "price_change": float(binance_data["priceChange"]) * krw_rate,
        "price_change_percent": binance_data["priceChangePercent"],
        "volume": float(binance_data["volume"]),
    }

    producer.produce("binance-ohlcv", key=symbol, value=str(market_data))
    producer.flush()

    return market_data


@app.get("/kucoin_market_data/{symbol}", response_model=MarketData)
async def get_kucoin_ohlcv(symbol: str):
    url = "https://api.kucoin.com/api/v1/market/stats"
    response = requests.get(url, params={"symbol": symbol})

    if response.status_code != 200:
        raise HTTPException(status_code=404, detail="KuCoin data not found")

    kucoin_data = response.json()["data"]

    exchange_url = (
        "https://quotation-api-cdn.dunamu.com/v1/forex/recent?codes=FRX.KRWUSD"
    )
    exchange_response = requests.get(exchange_url).json()
    krw_rate = exchange_response[0]["basePrice"]

    market_data = {
        "opening_price": float(kucoin_data["averagePrice"]) * krw_rate,
        "high_price": float(kucoin_data["high"]) * krw_rate,
        "low_price": float(kucoin_data["low"]) * krw_rate,
        "last_price": float(kucoin_data["last"]) * krw_rate,
        "price_change": float(kucoin_data["changePrice"]) * krw_rate,
        "price_change_percent": kucoin_data["changeRate"],
        "volume": float(kucoin_data["vol"]),
    }

    producer.produce("kucoin-ohlcv", key=symbol, value=str(market_data))
    producer.flush()

    return market_data


@app.get("/upbit_market_data/{symbol}", response_model=MarketData)
async def get_upbit_ohlcv(symbol: str):
    url = "https://api.upbit.com/v1/ticker"
    response = requests.get(url, params={"markets": symbol})
    if response.status_code != 200:
        raise HTTPException(status_code=404, detail="Upbit data not found")

    upbit_data = response.json()[0]
    market_data = {
        "opening_price": float(upbit_data["opening_price"]),
        "high_price": float(upbit_data["high_price"]),
        "low_price": float(upbit_data["low_price"]),
        "last_price": float(upbit_data["trade_price"]),
        "price_change": float(upbit_data["signed_change_price"]),
        "price_change_percent": upbit_data["signed_change_rate"],
        "volume": float(upbit_data["acc_trade_volume_24h"]),
    }

    producer.produce("upbit-ohlcv", key=symbol, value=str(market_data))
    producer.flush()

    return market_data


@app.get("/bithumb_market_data/{symbol}", response_model=MarketData)
async def get_bithumb_ohlcv(symbol: str):
    market_url = f"https://api.bithumb.com/public/ticker/{symbol}"
    transaction_url = (
        f"https://api.bithumb.com/public/transaction_history/{symbol}?count=1"
    )
    market_response = requests.get(market_url)
    transaction_response = requests.get(transaction_url)

    if market_response.status_code != 200 or transaction_response.status_code != 200:
        raise HTTPException(status_code=404, detail="Bithumb data not found")

    bithumb_data = market_response.json()["data"]
    transaction_data = transaction_response.json()["data"][0]

    market_data = {
        "opening_price": float(bithumb_data["opening_price"]),
        "high_price": float(bithumb_data["max_price"]),
        "low_price": float(bithumb_data["min_price"]),
        "last_price": float(transaction_data["price"]),
        "price_change": float(bithumb_data["fluctate_24H"]),
        "price_change_percent": bithumb_data["fluctate_rate_24H"],
        "volume": float(bithumb_data["units_traded_24H"]),
    }

    producer.produce("bithumb-ohlcv", key=symbol, value=str(market_data))
    producer.flush()

    return market_data
