import sqlite3
import pandas as pd
import json

# SQLiteからデータ取得
conn = sqlite3.connect("estat_data.db")
df_pivoted = pd.read_sql("SELECT * FROM estat_data_pivoted", conn)
df_class_info = pd.read_sql("SELECT * FROM estat_class_info", conn)
conn.close()

# コード列名（例: "140,100,100"）→日本語名へのマッピングを作成
code_to_name = {}

for col in df_class_info.columns:
    if col.startswith("CLASS_OBJ"):
        try:
            class_objs = json.loads(df_class_info[col][0])
            if isinstance(class_objs, dict):
                class_objs = [class_objs]
            for obj in class_objs:
                for c in obj.get("CLASS", []):
                    # 必要に応じて@tab, @cat01, @cat02を組み合わせる
                    code = ",".join(
                        [
                            c.get("@tab", ""),
                            c.get("@code", ""),
                            c.get("@cat02", "100"),  # cat02があれば使う
                        ]
                    )
                    code_to_name[code] = c["@name"]
        except Exception:
            continue

# 列名を日本語に変換
new_columns = []
for col in df_pivoted.columns:
    if col in code_to_name:
        new_columns.append(code_to_name[col])
    else:
        new_columns.append(col)
df_pivoted.columns = new_columns

# 数値列をfloat型に変換
for col in df_pivoted.columns:
    if col != "id":
        df_pivoted[col] = pd.to_numeric(df_pivoted[col], errors="coerce")

# 可視化（例：表示）
print(df_pivoted.head())

# 例：matplotlibでグラフ化
import matplotlib.pyplot as plt

df_pivoted.set_index("id").plot()
plt.show()
