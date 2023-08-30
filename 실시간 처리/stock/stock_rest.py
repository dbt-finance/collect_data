import os
import requests
import json
import pandas as pd
import time

from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaAdminClient
from kafka.producer import KafkaProducer
ticker = "005930" # 국내주식(삼성전자)
symbol = "AMZN"  # 해외주식(아마존닷컴)

APP_KEY = os.environ.get("APP_KEY")
APP_SECRET = os.environ.get("APP_SECRET")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")



URL_BASE = "https://openapi.koreainvestment.com:9443"
PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
URL = f"{URL_BASE}/{PATH}"


headers = {
    "Content-Type":"application/json", 
    "authorization": f"Bearer {ACCESS_TOKEN}",
    "appKey":APP_KEY,
    "appSecret":APP_SECRET,
    #"tr_id":"FHKST01010100" # 국내 주식
    "tr_id":"HHDFS00000300"  # 해외 주식
}

''' 국내 주식
params = {
    "fid_cond_mrkt_div_code":"J",
    "fid_input_iscd": ticker # 삼성전자
}
'''

# 해외 주식
params = {
    "AUTH": "",
    "EXCD": "NAS",  
    "SYMB": symbol # 아마존닷컴
}




# 토픽 삭제
def delete_topic(bootstrap_servers, topic_name):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    admin_client.delete_topics([topic_name])
    admin_client.close()


# 토픽 생성
def create_topic(bootstrap_servers, name, partitions, replica=1):
    client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        topic = NewTopic(
            name=name,
            num_partitions=partitions,
            replication_factor=replica)
        client.create_topics([topic])
    except TopicAlreadyExistsError as e:
        print(e)
        pass
    finally:
        client.close()


'''
# 국내주식 현재가 가져오기
def get_stock_price():
    # 현재가 가져오기
    res = requests.get(URL, headers=headers, params=params)
    current_price = res.json()['output']['stck_prpr']  # 국내주식 현재가
    market_cap = res.json()['output']['hts_avls'] # 국내주식 시가총액
    current_timestamp = pd.Timestamp.now().timestamp()

    # Serialize the data and send it
    message_data = {
        "get_data_timestamp": current_timestamp,
        # "ticker": "005930",  # 삼성전자
        "stock_name" : "삼성전자"
        "market_cap" : market_cap,
        "current_price": current_price
    }
    
    print(message_data)
    return message_data
'''

# 해외주식 현재가 가져오기
def get_stock_price():
    # 현재가 가져오기
    res = requests.get(URL, headers=headers, params=params)
    current_price = res.json()['output']['last']  # 해외주식 현재체결가
    current_timestamp = pd.Timestamp.now().timestamp()

    # Serialize the data and send it
    message_data = {
        "get_data_timestamp": current_timestamp,
        "symbol": "AMZN",  # 아마존닷컴
        "stock_name" : "amazon.com",
        "current_price": current_price
    }
    
    print(message_data)
    return message_data



def main():
    
    # topic_name = "samsung" # 국내주식
    topic_name = "amazon.com" # 해외주식
    bootstrap_servers = ["localhost:9092"] 


    # delete the existing topic
    delete_topic(bootstrap_servers, topic_name)

    # create a topic first
    create_topic(bootstrap_servers, topic_name, 4)


    # ingest some random people events
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        # client_id="samsung_Producer",  # 국내주식
        client_id="amazon.com_Producer",
    )

    
    for i in range(30):
        message_data = get_stock_price()
        current_timestamp = pd.Timestamp.now().timestamp()

        producer.send(
            topic=topic_name,
            # 국내주식
            # key = f"{ticker}_{current_timestamp}".encode('utf-8'),
            # 해외주식
            key = f"{symbol}_{current_timestamp}".encode('utf-8'),
            value=json.dumps(message_data).encode('utf-8')
        )
        time.sleep(2)  # 2초에 한번씩 

    producer.flush()

if __name__ == '__main__':
    main()