import json
import requests
import sqlite3
import pandas as pd
import configparser

config_ini = configparser.ConfigParser()
config_ini.read("config.ini", encoding="utf-8")
API_KEY = config_ini["API"]["KEY"]
URL = config_ini["API"]["url_list"]
PARAMS = {"appId": API_KEY, "searchWord": "月次", "surveyYears": 2015, "limit": 1500}

response = requests.get(URL, params=PARAMS)
json_data = response.json()

# テーブル形式に変換
table_info = json_data["GET_STATS_LIST"]["DATALIST_INF"]["TABLE_INF"]
df = pd.json_normalize(table_info)

# SQLite3に保存
conn = sqlite3.connect("estat_data.db")
df.to_sql("estat_table_info", conn, if_exists="replace", index=False)
conn.close()

print("データは 'estat_data.db' に 'estat_table_info' テーブルとして保存されました。")
