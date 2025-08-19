import json
import requests
import sqlite3
import pandas as pd
import configparser
import datetime

# 値が列ごとの更新ではなくID事の更新になっている事を改善

# -------------------------------
# 0. 変数の初期化
# -------------------------------
json_data = {}
STATISTICAL_DATA = {}
class_info = {}
table_info = {}
stat_name = ""
title = ""
tab_code = ""
cat01_list = []
cat02_list = []
df_summary = pd.DataFrame()
df_values = pd.DataFrame()
df_transformed = pd.DataFrame()
df_pivoted = pd.DataFrame()

# -------------------------------
# 1. 設定ファイルの読み込み
# -------------------------------
config_ini = configparser.ConfigParser()
config_ini.read("config.ini", encoding="utf-8")
API_KEY = config_ini["API"]["KEY"]
URL = config_ini["API"]["url_data"]
DB_PATH = config_ini["DB"]["data"]

# -------------------------------
# 2. APIパラメータの設定
# -------------------------------
limit_num = 2000
stats_idS = {
    1: "0003423953",  # 機械受注統計調査
    2: "0003348423",  # 景気ウォッチャー調査
}
PARAMS = {"appId": API_KEY, "statsDataId": stats_idS[2], "limit": limit_num}

# -------------------------------
# 3. APIリクエスト
# -------------------------------
response = requests.get(URL, params=PARAMS)
json_data = response.json()
###### 3_複数の同時リクエスト（将来的に使用、将来的に追加でlimit_num=2000以上の処理の対応も必要）
###### for key in stats_idS:
######    print(f"統計表 {key} の処理開始")
######    PARAMS = {"appId": API_KEY, "statsDataId": stats_idS[key], "limit": limit_num}
######    response = requests.get(URL, params=PARAMS)
######    json_data = response.json()

# -------------------------------
# 4. 分類情報の抽出と整形
# -------------------------------
STATISTICAL_DATA = json_data["GET_STATS_DATA"]["STATISTICAL_DATA"]
class_info = STATISTICAL_DATA["CLASS_INF"]
table_info = STATISTICAL_DATA["TABLE_INF"]
stat_name = table_info["STAT_NAME"]["$"]
title = table_info["TITLE"]

class_objs = class_info["CLASS_OBJ"]
tab_code = ""
cat01_list = []
cat02_list = []

for obj in class_objs:
    cid = obj["@id"]
    if cid == "tab":
        tab_code = obj["CLASS"]["@code"]
    elif cid == "cat01":
        for c in obj["CLASS"]:
            cat01_list.append(f"@{c['@code']}:{c['@name']}")
    elif cid == "cat02":
        for c in obj["CLASS"]:
            cat02_list.append(f"@{c['@code']}:{c['@name']}")

cat01_str = ";".join(cat01_list)
cat02_str = ";".join(cat02_list)

df_summary = pd.DataFrame(
    [
        {
            "tab": tab_code,
            "STAT_NAME": stat_name,
            "TITLE": title,
            "cat01": cat01_str,
            "cat02": cat02_str,
        }
    ]
)

# -------------------------------
# 5. 統計値の整形とピボット処理
# -------------------------------
data_values = STATISTICAL_DATA["DATA_INF"]["VALUE"]
df_values = pd.json_normalize(data_values)

df_values["time"] = df_values["@time"]
df_values["@tab+@cat01+@cat02"] = (
    df_values["@tab"].astype(str)
    + ","
    + df_values["@cat01"].astype(str)
    + ","
    + df_values["@cat02"].astype(str)
)


def convert_id_to_date(id_val):
    id_str = str(id_val)
    year = int(id_str[:4])
    month = int(id_str[6:8])
    return datetime.datetime(year, month, 1)


df_values["id"] = df_values["time"].apply(convert_id_to_date)
df_transformed = df_values[["id", "@tab+@cat01+@cat02", "$"]]
df_pivoted = df_transformed.pivot(index="id", columns="@tab+@cat01+@cat02", values="$")
df_pivoted.reset_index(inplace=True)

# -------------------------------
# 6. SQLite保存（差分更新）
# -------------------------------
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# 統計値テーブルの更新・追加
cursor.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name='estat_values'"
)
table_exists = cursor.fetchone()

if not table_exists:
    df_pivoted.to_sql(
        "estat_values",
        conn,
        if_exists="replace",
        index=False,
        dtype={col: "REAL" for col in df_pivoted.columns if col != "id"}
        | {"id": "text"},
    )
else:
    df_existing = pd.read_sql_query("SELECT * FROM estat_values", conn)
    df_combined = pd.concat([df_existing.set_index("id"), df_pivoted.set_index("id")])
    df_combined = df_combined.groupby("id").last().reset_index()
    df_combined["id"] = df_combined["id"].astype(str)
    df_combined.to_sql(
        "estat_values",
        conn,
        if_exists="replace",
        index=False,
        dtype={col: "REAL" for col in df_pivoted.columns if col != "id"}
        | {"id": "text"},
    )

# 分類情報テーブルの更新・追加
cursor.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name='estat_class_info'"
)
class_table_exists = cursor.fetchone()

if not class_table_exists:
    df_summary.to_sql(
        "estat_class_info",
        conn,
        if_exists="replace",
        index=False,
        dtype={col: "text" for col in df_summary if col != "tab"} | {"tab": "INTEGER"},
    )
else:
    df_existing_tab = pd.read_sql_query("SELECT * FROM estat_class_info", conn)
    df_class_table = pd.concat(
        [df_existing_tab.set_index("tab"), df_summary.set_index("tab")]
    )
    df_class_table = df_class_table.groupby("tab").last().reset_index()
    df_class_table.to_sql(
        "estat_class_info",
        conn,
        if_exists="replace",
        index=False,
        dtype={col: "text" for col in df_summary if col != "tab"} | {"tab": "INTEGER"},
    )

conn.close()

print("統計値は 'estat_values' に更新・追加されました。")
print("分類要約は 'estat_class_info' に保存されました。")
