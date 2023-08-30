import os
import requests
import json

APP_KEY = os.environ.get("APP_KEY")
APP_SECRET = os.environ.get("APP_SECRET")


URL_BASE = "https://openapi.koreainvestment.com:9443" #모의투자서비스 

headers = {"content-type":"application/json"}
body = {"grant_type":"client_credentials",
        "appkey":APP_KEY, 
        "appsecret":APP_SECRET}


PATH = "oauth2/tokenP"


URL = f"{URL_BASE}/{PATH}"


res = requests.post(URL, headers=headers, data=json.dumps(body))


ACCESS_TOKEN = res.json()["access_token"]
print(ACCESS_TOKEN)
