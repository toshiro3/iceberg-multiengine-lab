# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marimo",
#     "trino",
#     "pandas",
# ]
# ///

import marimo

__generated_with = "0.10.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    mo.md("""
    # 🔍 Trino: 高速SQLエンジンでIcebergを操作
    
    このノートブックでは、**Trino**を使ってIcebergテーブルにSQLでアクセスします。
    
    ## Trinoとは
    - 分散SQLクエリエンジン（旧PrestoSQL）
    - 高速な分析クエリに最適化
    - Iceberg、Delta Lake、Hiveなど多様なデータソースに対応
    """)
    return (mo,)


@app.cell
def _(mo):
    mo.md("""
    ## Part 1: Trinoに接続
    """)
    return


@app.cell
def _():
    from trino.dbapi import connect
    import pandas as pd

    # Trinoに接続
    conn = connect(
        host="trino",
        port=8080,
        user="marimo",
        catalog="iceberg",
        schema="demo",
    )
    
    print("Trino接続成功")
    return conn, connect, pd


@app.cell
def _(mo):
    mo.md("""
    ## Part 2: カタログとスキーマの確認
    """)
    return


@app.cell
def _(conn, pd):
    # カタログ一覧
    cursor = conn.cursor()
    cursor.execute("SHOW CATALOGS")
    catalogs_df = pd.DataFrame(cursor.fetchall(), columns=["Catalog"])
    catalogs_df
    return cursor, catalogs_df


@app.cell
def _(conn, pd):
    # スキーマ一覧
    cursor2 = conn.cursor()
    cursor2.execute("SHOW SCHEMAS IN iceberg")
    schemas_df = pd.DataFrame(cursor2.fetchall(), columns=["Schema"])
    schemas_df
    return cursor2, schemas_df


@app.cell
def _(conn, pd):
    # テーブル一覧
    cursor3 = conn.cursor()
    cursor3.execute("SHOW TABLES IN iceberg.demo")
    tables_df = pd.DataFrame(cursor3.fetchall(), columns=["Table"])
    tables_df
    return cursor3, tables_df


@app.cell
def _(mo):
    mo.md("""
    ## Part 3: データの読み取り
    
    PyIceberg/Sparkで作成・更新したテーブルをTrinoで読み取ります。
    """)
    return


@app.cell
def _(conn, pd):
    # データを読み取り
    cursor4 = conn.cursor()
    cursor4.execute("SELECT * FROM iceberg.demo.users ORDER BY user_id")
    columns = [desc[0] for desc in cursor4.description]
    users_df = pd.DataFrame(cursor4.fetchall(), columns=columns)
    users_df
    return cursor4, columns, users_df


@app.cell
def _(mo):
    mo.md("""
    ## Part 4: 分析クエリの実行
    
    Trinoの強みである分析クエリを実行します。
    """)
    return


@app.cell
def _(conn, pd):
    # 集計クエリ
    cursor5 = conn.cursor()
    cursor5.execute("""
        SELECT 
            COUNT(*) as total_users,
            AVG(score) as avg_score,
            MAX(score) as max_score,
            MIN(score) as min_score
        FROM iceberg.demo.users
    """)
    columns5 = [desc[0] for desc in cursor5.description]
    stats_df = pd.DataFrame(cursor5.fetchall(), columns=columns5)
    stats_df
    return cursor5, columns5, stats_df


@app.cell
def _(mo):
    mo.md("""
    ## Part 5: Trinoからデータを追加
    """)
    return


@app.cell
def _(conn):
    # Trinoからデータを追加
    cursor6 = conn.cursor()
    cursor6.execute("""
        INSERT INTO iceberg.demo.users (user_id, name, email, score)
        VALUES (6, 'Frank', 'frank@example.com', 82.0)
    """)
    print("Trinoから1件のデータを追加しました")
    return (cursor6,)


@app.cell
def _(conn, pd):
    # 追加後のデータを確認
    cursor7 = conn.cursor()
    cursor7.execute("SELECT * FROM iceberg.demo.users ORDER BY user_id")
    columns7 = [desc[0] for desc in cursor7.description]
    updated_df = pd.DataFrame(cursor7.fetchall(), columns=columns7)
    updated_df
    return cursor7, columns7, updated_df


@app.cell
def _(mo):
    mo.md("""
    ## Part 6: スナップショットとタイムトラベル
    
    Trinoでもタイムトラベルクエリが可能です。
    """)
    return


@app.cell
def _(conn, pd):
    # スナップショット一覧（Icebergのメタデータテーブル）
    cursor8 = conn.cursor()
    cursor8.execute("""
        SELECT 
            snapshot_id,
            committed_at,
            operation,
            summary
        FROM iceberg.demo."users$snapshots"
        ORDER BY committed_at DESC
    """)
    columns8 = [desc[0] for desc in cursor8.description]
    snapshots_df = pd.DataFrame(cursor8.fetchall(), columns=columns8)
    snapshots_df
    return cursor8, columns8, snapshots_df


@app.cell
def _(mo):
    mo.md("""
    ## Part 7: ファイル情報の確認
    
    Icebergのデータファイル情報を確認します。
    """)
    return


@app.cell
def _(conn, pd):
    # データファイル一覧
    cursor9 = conn.cursor()
    cursor9.execute("""
        SELECT 
            file_path,
            file_format,
            record_count,
            file_size_in_bytes
        FROM iceberg.demo."users$files"
    """)
    columns9 = [desc[0] for desc in cursor9.description]
    files_df = pd.DataFrame(cursor9.fetchall(), columns=columns9)
    files_df
    return cursor9, columns9, files_df


@app.cell
def _(mo):
    mo.md("""
    ---
    
    ## まとめ
    
    ✅ **3つのエンジンでIcebergテーブルを操作しました**
    
    | エンジン | 特徴 | 用途 |
    |---------|------|------|
    | **PyIceberg** | 軽量、JVM不要 | Python分析、メタデータ操作 |
    | **PySpark** | 分散処理対応 | 大規模データ処理 |
    | **Trino** | 高速SQL | 分析クエリ、BI連携 |
    
    REST Catalogを介して、すべてのエンジンが同じIcebergテーブルに
    一貫してアクセスできることが確認できました！
    """)
    return


if __name__ == "__main__":
    app.run()
