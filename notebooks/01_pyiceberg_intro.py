# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marimo",
#     "pyiceberg[pyarrow,s3fs]",
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
    # 🧊 PyIcebergでIcebergテーブルを操作する
    
    このノートブックでは、PyIcebergを使ってApache Icebergテーブルの基本操作を学びます。
    
    ## 環境構成
    - **カタログ**: REST Catalog（tabulario/iceberg-rest）
    - **ストレージ**: MinIO（S3互換）
    - **ノートブック**: marimo
    """)
    return (mo,)


@app.cell
def _():
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import StringType, LongType, DoubleType, NestedField
    import pyarrow as pa
    import pandas as pd
    import os

    # カタログに接続
    catalog = load_catalog(
        "rest",
        **{
            "type": "rest",
            "uri": os.environ.get("CATALOG_URI", "http://rest-catalog:8181"),
            "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://minio:9000"),
            "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "admin"),
            "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "password"),
            "s3.region": "us-east-1",
        }
    )
    
    print(f"カタログ接続成功: {catalog}")
    return Schema, NestedField, StringType, LongType, DoubleType, pa, pd, catalog, load_catalog, os


@app.cell
def _(mo):
    mo.md("""
    ## Step 1: ネームスペースの作成
    
    Icebergではテーブルをネームスペース（データベース相当）で整理します。
    """)
    return


@app.cell
def _(catalog):
    # 既存のネームスペース確認
    namespaces = catalog.list_namespaces()
    print(f"既存のネームスペース: {namespaces}")

    # ネームスペースを作成（存在しない場合）
    try:
        catalog.create_namespace("demo")
        print("ネームスペース 'demo' を作成しました")
    except Exception as e:
        print(f"ネームスペース 'demo' は既に存在します: {e}")

    # 確認
    print(f"ネームスペース一覧: {catalog.list_namespaces()}")
    return (namespaces,)


@app.cell
def _(mo):
    mo.md("""
    ## Step 2: テーブルの作成
    
    ユーザー情報を格納するテーブルを作成します。
    """)
    return


@app.cell
def _(Schema, NestedField, StringType, LongType, DoubleType, catalog):
    # スキーマ定義
    schema = Schema(
        NestedField(1, "user_id", LongType(), required=True),
        NestedField(2, "name", StringType(), required=True),
        NestedField(3, "email", StringType(), required=False),
        NestedField(4, "score", DoubleType(), required=False),
    )

    # テーブル作成（存在しない場合）
    table_name = "demo.users"
    try:
        table = catalog.create_table(table_name, schema=schema)
        print(f"テーブル '{table_name}' を作成しました")
    except Exception as e:
        print(f"テーブル '{table_name}' は既に存在します。ロードします。")
        table = catalog.load_table(table_name)

    print(table)
    return schema, table, table_name


@app.cell
def _(mo):
    mo.md("""
    ## Step 3: データの追加
    
    PyArrowを使ってデータを追加します。
    """)
    return


@app.cell
def _(pa, table):
    # PyArrowスキーマ（Icebergスキーマと一致させる）
    arrow_schema = pa.schema([
        pa.field("user_id", pa.int64(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
        pa.field("email", pa.string(), nullable=True),
        pa.field("score", pa.float64(), nullable=True),
    ])

    # サンプルデータ
    data = pa.table({
        "user_id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "email": ["alice@example.com", "bob@example.com", None],
        "score": [85.5, 92.0, 78.5],
    }, schema=arrow_schema)

    # データを追加
    table.append(data)
    print("3件のデータを追加しました")
    return arrow_schema, data


@app.cell
def _(mo):
    mo.md("""
    ## Step 4: データの読み取り
    """)
    return


@app.cell
def _(table):
    # テーブルをリフレッシュして最新状態を取得
    table.refresh()

    # データをPandasで読み取り
    df = table.scan().to_pandas()
    df
    return (df,)


@app.cell
def _(mo):
    mo.md("""
    ## Step 5: スナップショットの確認
    
    Icebergは各操作をスナップショットとして記録します。
    """)
    return


@app.cell
def _(table, mo):
    # スナップショット一覧
    snapshots_info = []
    for snap in table.metadata.snapshots:
        snapshots_info.append({
            "Snapshot ID": snap.snapshot_id,
            "Operation": snap.summary.get("operation", "N/A"),
            "Added Records": snap.summary.get("added-records", "N/A"),
            "Timestamp": snap.timestamp_ms,
        })
    
    mo.ui.table(snapshots_info)
    return (snapshots_info,)


@app.cell
def _(mo):
    mo.md("""
    ## Step 6: メタデータの確認
    
    テーブルのメタデータ情報を確認します。
    """)
    return


@app.cell
def _(table):
    metadata = table.metadata
    print(f"テーブルUUID: {metadata.table_uuid}")
    print(f"フォーマットバージョン: {metadata.format_version}")
    print(f"ロケーション: {metadata.location}")
    print(f"スナップショット数: {len(metadata.snapshots)}")
    return (metadata,)


@app.cell
def _(mo):
    mo.md("""
    ---
    
    ## 次のステップ
    
    - **02_pyspark_multiengine.py**: PySparkで同じテーブルにアクセス
    - **03_trino_sql.py**: TrinoでSQLクエリを実行
    
    マルチエンジンでの相互運用性を確認してみましょう！
    """)
    return


if __name__ == "__main__":
    app.run()
