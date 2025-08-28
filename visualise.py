import re
import sqlite3
import pandas as pd

DB = "estat_data.db"

# 1) 本体とメタ情報の読み込み
with sqlite3.connect(DB) as conn:
    df = pd.read_sql_query("SELECT * FROM estat_values", conn)
    meta = pd.read_sql_query("SELECT * FROM estat_column_meta", conn)


# 2) テーブル名（統計表タイトル）の取得
def pick_table_title(meta: pd.DataFrame) -> str:
    # よくある列名の候補を順に探索（手元のスキーマに合わせて必要なら追加）
    candidates = [
        "table_title",
        "title",
        "TITLE",
        "statistics_title",
        "STATISTICS_NAME",
        "STAT_NAME",
        "STAT_NAME_JA",
    ]
    for c in candidates:
        if c in meta.columns:
            s = meta[c].dropna().astype(str).str.strip()
            if not s.empty and s.iloc[0]:
                return s.iloc[0]
    # 取得できない場合はプレースホルダ
    return "テーブル名"


table_title = pick_table_title(meta)


# 3) コード→名称の辞書を用意
def build_map(meta: pd.DataFrame, code_col: str, name_col: str) -> dict:
    if {code_col, name_col}.issubset(meta):
        m = meta[[code_col, name_col]].dropna().drop_duplicates()
        return dict(m.to_records(index=False))
    return {}


tab_map = build_map(meta, "tab_code", "tab_name")
cat01_map = build_map(meta, "cat01_code", "cat01_name")
cat02_map = build_map(meta, "cat02_code", "cat02_name")
# 必要なら cat03〜cat15 も同様に追加

maps = {
    "tab": tab_map,
    "cat01": cat01_map,
    "cat02": cat02_map,
}

# 4) 合成列名 "tab-100_cat01-100_cat02-100" を解析して名称に変換
PAT = re.compile(r"(tab|cat\d{2})-([^_]+)")


def to_label_inside(col: str, maps: dict, joiner: str = " × ") -> str:
    """括弧の内側に入る『名称』部分（tab名×cat01名×…）を作る。"""
    names = []
    for key, code in PAT.findall(col):
        name = maps.get(key, {}).get(code)
        names.append(name if name else f"{key}:{code}")
    return joiner.join(names) if names else col  # パターン外はそのまま


def to_table_style(col: str, table_title: str) -> str:
    inner = to_label_inside(col, maps)
    if inner == col:
        # 解析できなかった（元から普通の列名）→ テーブル名で括ると全部同名になるのでそのまま返す
        return col
    return f"{table_title}（{inner}）"


# 5) 置換マップを作ってリネーム
rename_map = {}
for c in df.columns:
    if c in {"id", "date"}:
        continue
    rename_map[c] = to_table_style(c, table_title)

df_renamed = df.rename(columns=rename_map)


# 6) 同名衝突の自動解消（重複した場合のみ "(2)" "(3)" を付与）
def dedup_columns(cols):
    seen = {}
    out = []
    for col in cols:
        n = seen.get(col, 0)
        out.append(col if n == 0 else f"{col} ({n+1})")
        seen[col] = n + 1
    return out


df_renamed.columns = dedup_columns(df_renamed.columns)

# 結果の確認
