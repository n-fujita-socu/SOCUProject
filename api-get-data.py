import requests
import sqlite3
import pandas as pd
import configparser

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
# ユーティリティ
# -------------------------------
def _normalize_class_list(obj):
    """CLASS_OBJのCLASSがdict/listどちらでも反復可能なlistに正規化"""
    cl = obj.get("CLASS", [])
    if isinstance(cl, dict):
        cl = [cl]
    return cl


def build_code_name_maps(class_info):
    """
    CLASS_INF から 軸ごとの {code: name} マップを作成
    戻り値: maps = {'tab': {...}, 'cat01': {...}, 'cat02': {...}, ...}
    """
    maps = {}
    class_objs = class_info.get("CLASS_OBJ", [])
    if isinstance(class_objs, dict):
        class_objs = [class_objs]

    for obj in class_objs:
        axis = obj.get("@id")  # 'tab', 'cat01', 'cat02', ... など
        if not axis:
            continue
        code_to_name = {}
        for c in _normalize_class_list(obj):
            code = str(c.get("@code", ""))
            name = str(c.get("@name", ""))
            code_to_name[code] = name
        maps[axis] = code_to_name
    return maps


def detect_cat_axes(maps, max_cat=10):
    """
    maps から、存在する cat 軸（cat01..cat10）を順序付きで抽出
    """
    axes = []
    for i in range(1, max_cat + 1):
        a = f"cat{i:02d}"
        if a in maps:
            axes.append(a)
    return axes


def build_col_key_from_row(row, cat_axes):
    """
    1行の値から列キー 'tab-<code>_cat01-<code>_cat02-<code>...' を生成
    - tab は必須
    - cat_axes に列挙されている cat は、値が空/NaNでなければ採用
    """

    def safe_get(r, col):
        v = r.get(col, "")
        if v is None:
            return ""
        s = str(v)
        return "" if s == "nan" else s

    parts = []
    tab_code = safe_get(row, "@tab")
    parts.append(f"tab-{tab_code}")

    for axis in cat_axes:
        code = safe_get(row, f"@{axis}")
        if code != "":
            parts.append(f"{axis}-{code}")
    return "_".join(parts)


def parse_time(t):
    """@time のフォーマット混在に耐える日付パーサ"""
    s = str(t).strip()
    fmts = ["%Y%m%d", "%Y%m", "%Y-%m", "%Y"]  # 必要に応じて拡張
    for f in fmts:
        try:
            return pd.to_datetime(s, format=f)
        except Exception:
            pass
    return pd.to_datetime(s, errors="coerce")


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
limit_num = 5
stats_idS = {
    1: "0003423953",  # 機械受注統計調査
    2: "0003348423",  # 景気ウォッチャー調査
}
chosen_stat = 2
stat_id = stats_idS.get(chosen_stat)
# -------------------------------
# 3. APIリクエスト
# -------------------------------
PARAMS = {"appId": API_KEY, "statsDataId": stat_id, "limit": limit_num}
response = requests.get(URL, params=PARAMS)
response.raise_for_status()
json_data = response.json()
###### 3_複数の同時リクエスト（将来的に使用、将来的に追加でlimit_num=2000以上の処理の対応も必要）
###### for key in stats_idS:
######    print(f"統計表 {key} の処理開始")
######    PARAMS = {"appId": API_KEY, "statsDataId": stats_idS[key], "limit": limit_num}
######    response = requests.get(URL, params=PARAMS)
######    json_data = response.json()

# -------------------------------
# 4. 分類情報の抽出と整形（改良）
# -------------------------------
STATISTICAL_DATA = json_data["GET_STATS_DATA"]["STATISTICAL_DATA"]
class_info = STATISTICAL_DATA["CLASS_INF"]
table_info = STATISTICAL_DATA["TABLE_INF"]
stat_name = table_info["STAT_NAME"]["$"]
title = table_info["TITLE"]

# 軸のマップと cat 軸を検出（追加）
maps = build_code_name_maps(class_info)
cat_axes = detect_cat_axes(maps, max_cat=10)  # 例: ['cat01', 'cat02', ...]


# 既存の df_summary 互換（cat01/cat02のみ反映。cat03以降は列メタで管理）
tab_code = ""
cat01_list, cat02_list = [], []
class_objs = class_info.get("CLASS_OBJ", [])
if isinstance(class_objs, dict):
    class_objs = [class_objs]

for obj in class_objs:
    cid = obj.get("@id")
    if cid == "tab":
        # CLASS が dict/list どちらでも対応
        cls = _normalize_class_list(obj)
        if cls:
            tab_code = cls[0].get("@code", "")
    elif cid == "cat01":
        for c in _normalize_class_list(obj):
            cat01_list.append(f"@{c.get('@code','')}:{c.get('@name','')}")
    elif cid == "cat02":
        for c in _normalize_class_list(obj):
            cat02_list.append(f"@{c.get('@code','')}:{c.get('@name','')}")

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
# 5. 統計値の整形とピボット処理（改良）
# -------------------------------
data_values = STATISTICAL_DATA["DATA_INF"]["VALUE"]
df_values = pd.json_normalize(data_values)

# 存在する cat 軸はすべて DataFrame に補完（cat02が無い統計などに対応）
for axis in cat_axes:
    col = f"@{axis}"
    if col not in df_values.columns:
        df_values[col] = ""

# 値（$）を数値化（変換不能は NaN）
# 必要に応じて前処理（カンマや特殊記号除去）を追加してもよい
df_values["$"] = pd.to_numeric(df_values["$"], errors="coerce")

# 列キーを作成：'tab-<code>_cat01-<code>_cat02-<code>_...'
df_values["col_key"] = df_values.apply(
    lambda r: build_col_key_from_row(r, cat_axes), axis=1
)

# time を堅牢にパース → 粒度に応じて正規化（ここでは月初）
df_values["id"] = df_values["@time"].apply(parse_time)


df_values["id"] = df_values["id"].dt.to_period("M").dt.to_timestamp(how="start")


# ピボット（列＝col_key）
df_transformed = df_values[["id", "col_key", "$"]]
df_pivoted = df_transformed.pivot(index="id", columns="col_key", values="$")
df_pivoted.reset_index(inplace=True)

# SQLite保存は TEXT のため id を ISO 文字列に
df_pivoted["id"] = df_pivoted["id"].dt.strftime("%Y-%m-%d")

# -------------------------------
# 6. SQLite保存（差分更新・列メタ保存）（改良）
# -------------------------------
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# 統計値テーブルの更新・追加（セル単位アップデート）
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

    # id をキーに index 化
    if "id" not in df_existing.columns:
        # 既存テーブルが壊れている場合の保険
        raise RuntimeError("既存テーブル 'estat_values' に id 列が見つかりません。")
    df_existing.set_index("id", inplace=True)
    df_new = df_pivoted.set_index("id")

    # 列の和集合で揃える（新列にも対応）
    all_cols = df_existing.columns.union(df_new.columns)
    df_existing = df_existing.reindex(columns=all_cols)
    df_new = df_new.reindex(columns=all_cols)

    # セル単位で更新：新規側の非欠損セルのみ上書き
    df_existing.update(df_new)

    # まるごと新規の id 行を追加
    new_rows = df_new[~df_new.index.isin(df_existing.index)]
    df_final = pd.concat([df_existing, new_rows])

    # 保存
    df_final = df_final.reset_index()
    df_final.to_sql(
        "estat_values",
        conn,
        if_exists="replace",
        index=False,
        dtype={col: "REAL" for col in df_final.columns if col != "id"} | {"id": "text"},
    )

# 分類情報テーブルの更新・追加（従来どおり）
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

# 列メタテーブル（新規：列名→各軸コード/名称）を保存
# 後から "tab-100_cat01-100_cat02-140..." を人可読に戻す用途
# レコード例：
#   col_key, tab_code, tab_name, cat01_code, cat01_name, cat02_code, cat02_name, ...
meta_rows = []
meta_source_cols = ["col_key", "@tab"] + [f"@{a}" for a in cat_axes]
df_meta_src = df_values[meta_source_cols].drop_duplicates()

for _, r in df_meta_src.iterrows():
    rec = {
        "col_key": r["col_key"],
        "tab_code": str(r["@tab"]),
        "tab_name": maps.get("tab", {}).get(str(r["@tab"]), ""),
    }
    for axis in cat_axes:
        code = str(r.get(f"@{axis}", "") or "")
        rec[f"{axis}_code"] = code if code != "" else None
        rec[f"{axis}_name"] = maps.get(axis, {}).get(code, "") if code != "" else None
    meta_rows.append(rec)

df_colmeta_new = pd.DataFrame(meta_rows)

cursor.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name='estat_column_meta'"
)
meta_exists = cursor.fetchone()

if not meta_exists:
    # 動的 cat 軸の列を持つように dtype を生成
    dtype_meta = {"col_key": "text", "tab_code": "text", "tab_name": "text"}
    for axis in cat_axes:
        dtype_meta[f"{axis}_code"] = "text"
        dtype_meta[f"{axis}_name"] = "text"

    df_colmeta_new.to_sql(
        "estat_column_meta", conn, if_exists="replace", index=False, dtype=dtype_meta
    )
else:
    df_colmeta_old = pd.read_sql_query("SELECT * FROM estat_column_meta", conn)

    # 将来、cat 軸が増えた場合にも対応（列の和集合で揃える）
    all_cols_meta = df_colmeta_old.columns.union(df_colmeta_new.columns)
    df_colmeta_old = df_colmeta_old.reindex(columns=all_cols_meta)
    df_colmeta_new = df_colmeta_new.reindex(columns=all_cols_meta)

    # マージ：col_key で合わせ、既存をベースに新規の欠損で補完（既存情報優先）
    old = df_colmeta_old.set_index("col_key")
    new = df_colmeta_new.set_index("col_key")
    merged = old.combine_first(new)  # 既存を温存しつつ、新規のみ補完

    merged.reset_index().to_sql(
        "estat_column_meta",
        conn,
        if_exists="replace",
        index=False,
        dtype={c: "text" for c in merged.columns},
    )

conn.close()

print("統計値は 'estat_values' に列単位で更新・追加されました。")
print("分類要約は 'estat_class_info' に保存されました。")
print("列メタは 'estat_column_meta' に保存/更新されました。")
