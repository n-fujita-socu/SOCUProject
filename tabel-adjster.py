import argparse
import re
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ---------- 分類 & ワイド化 ----------


def classify_monthly_deviation_wide(
    df: pd.DataFrame,
    id_col: str = "id",
    value_cols: Optional[List[str]] = None,
    bins: Tuple[float, float, float, float, float, float] = (
        -np.inf,
        -1.0,
        -0.3,
        0.3,
        1.0,
        np.inf,
    ),
    labels: Tuple[str, str, str, str, str] = ("e", "d", "c", "b", "a"),
    uppercase: bool = False,
    sort_by_date: str = "asc",  # 'asc' | 'desc' | 'none'
    id_dedupe: str = "none",  # 'none' | 'first' | 'last' | 'mean'
) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    複数の値列について、列×月ごとに 月平均との差 をσベース(a〜e)で分類し、
    最終出力をワイド形式（id + 各列クラス）で返す。
    戻り値: (wide_df, colmap {元の列名: 正規化後の列名})
    """
    data = df.copy()
    data[id_col] = data[id_col].astype(str)

    # 任意：id 重複の集約
    if id_dedupe != "none":
        agg_map = {}
        for c in data.columns:
            if c == id_col:
                continue
            if id_dedupe in ("first", "last"):
                agg_map[c] = id_dedupe
            elif id_dedupe == "mean":
                agg_map[c] = (
                    "mean" if pd.api.types.is_numeric_dtype(data[c]) else "last"
                )
        if agg_map:
            data = data.groupby(id_col, as_index=False).agg(agg_map)

    # 月抽出
    dt = pd.to_datetime(data[id_col], format="%Y%m%d", errors="coerce")
    data["month"] = dt.dt.month

    # 対象列
    if not value_cols:
        numeric_cols = data.select_dtypes(include="number").columns.tolist()
        value_cols = [c for c in numeric_cols if c not in [id_col, "month"]]
        if not value_cols:
            raise ValueError(
                "値列が見つかりません。--value-cols を指定するか、数値列を含めてください。"
            )

    # 縦持ち
    long = data[[id_col, "month"] + value_cols].melt(
        id_vars=[id_col, "month"], var_name="元の列名", value_name="元の値"
    )

    # 月平均・標準偏差（列×月）
    grp = long.groupby(["元の列名", "month"])["元の値"]
    long["MONTHLY_AVE"] = grp.transform("mean")
    long["MONTHLY_STD"] = grp.transform("std")

    # 偏差・z
    long["偏差"] = long["元の値"] - long["MONTHLY_AVE"]
    z = long["偏差"] / long["MONTHLY_STD"].replace(0, np.nan)

    # 5段階分類（σベース）
    cls = pd.cut(z, bins=bins, labels=labels).astype("object")

    # 標準偏差が出ないケースのフォールバック
    tol = 1e-12
    mask_nan = cls.isna()
    cls[mask_nan & (long["偏差"].abs() <= tol)] = "c"
    cls[mask_nan & (long["偏差"] > tol)] = "a"
    cls[mask_nan & (long["偏差"] < -tol)] = "e"

    if uppercase:
        cls = cls.str.upper()

    long["クラス"] = cls

    # (id, 元の列名) の重複があれば最後を採用
    long = long.sort_values([id_col, "元の列名"]).drop_duplicates(
        subset=[id_col, "元の列名"], keep="last"
    )

    # ワイド化
    wide = long.pivot(index=id_col, columns="元の列名", values="クラス").reset_index()
    wide.columns.name = None

    # 列名をDB向けに正規化
    colmap = {col: sanitize_colname(col) for col in wide.columns}
    wide = wide.rename(columns=colmap)

    # 時系列で並べ替え
    if sort_by_date in ("asc", "desc"):
        sort_key = pd.to_datetime(
            wide[colmap[id_col]].astype(str), format="%Y%m%d", errors="coerce"
        )
        wide = (
            wide.assign(_sort_key=sort_key)
            .sort_values("_sort_key", ascending=(sort_by_date == "asc"), kind="stable")
            .drop(columns="_sort_key")
            .reset_index(drop=True)
        )

    return wide, colmap


# ---------- SQLite 書き込み（UPSERT/REPLACE） ----------


def write_sqlite_adj_table(
    wide: pd.DataFrame,
    sqlite_path: str,
    table: str = "adj-table",
    id_col: str = "id",
    mode: str = "upsert",  # 'upsert' | 'replace'
    chunksize: Optional[int] = None,
) -> None:
    """
    ワイドDataFrame（id + 各列クラス）を SQLite に保存。
    既定: "adj-table" に UPSERT（INSERT OR REPLACE）。
    """
    engine = create_engine(f"sqlite:///{sqlite_path}", future=True)

    # ステージ表は安全名で作成（- を含めない）
    stage = f"_{sanitize_colname(table)}_stage"

    # 列型：id は TEXT、クラス列は TEXT(1)
    dtypes = {id_col: String(length=16)}
    for c in wide.columns:
        if c == id_col:
            continue
        dtypes[c] = String(length=1)

    # ステージ表へ書き込み（毎回作り直し）
    wide.to_sql(
        stage,
        engine,
        if_exists="replace",
        index=False,
        dtype=dtypes,
        chunksize=chunksize,
    )

    # ターゲット表（"adj-table"）にマージ
    tgt = quote_ident_sqlite(table)
    cols = list(wide.columns)
    non_id_cols = [c for c in cols if c != id_col]

    # CREATE TABLE 文
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {tgt} (
        {quote_ident_sqlite(id_col)} TEXT PRIMARY KEY,
        {", ".join([f"{quote_ident_sqlite(c)} TEXT" for c in non_id_cols])}
    );
    """

    # 置換 or アップサートの挙動
    with engine.begin() as conn:
        conn.execute(text(create_sql))

        if mode == "replace":
            # 既存テーブルを空にしてから投入（スキーマは維持）
            conn.execute(text(f"DELETE FROM {tgt};"))

        # INSERT OR REPLACE で UPSERT（mode=replace でも同じ式で可）
        insert_cols = ", ".join([quote_ident_sqlite(c) for c in cols])
        select_cols = ", ".join([f"s.{c}" for c in cols])
        merge_sql = f"""
        INSERT OR REPLACE INTO {tgt} ({insert_cols})
        SELECT {select_cols} FROM {stage} s;
        """
        conn.execute(text(merge_sql))

        # ステージ表を削除
        conn.execute(text(f"DROP TABLE IF EXISTS {stage};"))


# ---------- CLI & メイン ----------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="SQLite の estat_data.db に 'adj-table' を作成/更新します。"
    )
    # 入力
    p.add_argument(
        "--input-csv",
        type=str,
        required=True,
        help="入力CSVのパス（id と 複数の値列を含む）",
    )
    p.add_argument("--id-col", type=str, default="id", help="id列名（YYYYMMDD想定）")
    p.add_argument(
        "--value-cols", nargs="*", help="対象の値列名（省略時は自動検出: 数値列）"
    )
    p.add_argument(
        "--id-dedupe",
        type=str,
        choices=["none", "first", "last", "mean"],
        default="none",
        help="入力の id 重複の集約方法（既定: none）",
    )

    # 分類表示
    p.add_argument(
        "--uppercase", action="store_true", help="クラスを A〜E にする（既定は a〜e）"
    )
    p.add_argument(
        "--sort-by-date",
        type=str,
        choices=["asc", "desc", "none"],
        default="asc",
        help="出力を時系列ソート（昇順/降順/なし）",
    )

    # 出力（SQLite）
    p.add_argument(
        "--sqlite", type=str, default="./estat_data.db", help="SQLite DBファイルパス"
    )
    p.add_argument(
        "--table",
        type=str,
        default="adj-table",
        help='作成/更新するテーブル名（例: "adj-table"）',
    )
    p.add_argument(
        "--mode",
        type=str,
        choices=["replace", "upsert"],
        default="upsert",
        help="replace: 全置換, upsert: 既存更新+新規挿入",
    )
    p.add_argument(
        "--chunksize", type=int, default=None, help="DB書き込みチャンクサイズ"
    )
    return p.parse_args()


def main():
    args = parse_args()

    # 1) 入力CSV
    df = pd.read_csv(args.input_csv)

    # 2) 分類→ワイド化
    wide, colmap = classify_monthly_deviation_wide(
        df=df,
        id_col=args.id_col,
        value_cols=args.value_cols,
        uppercase=args.uppercase,
        sort_by_date=args.sort_by_date,
        id_dedupe=args.id_dedupe,
    )

    # 3) SQLite へ保存（"adj-table"）
    write_sqlite_adj_table(
        wide=wide,
        sqlite_path=args.sqlite,
        table=args.table,
        id_col=colmap.get(args.id_col, args.id_col),
        mode=args.mode,
        chunksize=args.chunksize,
    )

    print(
        f"[DONE] SQLite '{args.sqlite}' のテーブル {args.table!r} を更新しました。"
        f" rows={len(wide)}, cols={len(wide.columns)}"
    )


if __name__ == "__main__":
    main()
