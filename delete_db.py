import sqlite3

DB_PATH = "estat_data.db"

with sqlite3.connect(DB_PATH) as conn:
    cursor = conn.cursor()
    # estat_valuesテーブルのデータのみ削除
    cursor.execute("DELETE FROM estat_values;")
    conn.commit()

print("estat_valuesテーブルのデータを削除しました。")