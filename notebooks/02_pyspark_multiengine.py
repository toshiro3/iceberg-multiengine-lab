# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marimo",
#     "pyspark",
#     "pyiceberg[pyarrow,s3fs]",
#     "pandas",
# ]
# ///

import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    mo.md("""
    # 🚀 マルチエンジン検証: PySpark + PyIceberg

    このノートブックでは、**PySpark**と**PyIceberg**で同じIcebergテーブルにアクセスし、
    マルチエンジンでの相互運用性を確認します。

    ## 検証ポイント
    1. PyIcebergで作成したテーブルをSparkで読み取れるか
    2. Sparkで書き込んだデータをPyIcebergで読み取れるか
    3. スナップショットの共有
    """)
    return (mo,)


@app.cell
def _(mo):
    mo.md("""
    ## Part 1: PySpark セッションの作成

    REST Catalog経由でIcebergテーブルにアクセスするSparkセッションを作成します。
    """)
    return


@app.cell
def _():
    from pyspark.sql import SparkSession
    import os

    # Spark設定
    # JARはPySparkのjarsディレクトリに配置済みのため、spark.jars設定は不要
    spark = SparkSession.builder \
        .appName("IcebergMultiEngine") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.rest.type", "rest") \
        .config("spark.sql.catalog.rest.uri", os.environ.get("CATALOG_URI", "http://rest-catalog:8181")) \
        .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.rest.s3.endpoint", os.environ.get("S3_ENDPOINT", "http://minio:9000")) \
        .config("spark.sql.catalog.rest.s3.access-key-id", os.environ.get("AWS_ACCESS_KEY_ID", "admin")) \
        .config("spark.sql.catalog.rest.s3.secret-access-key", os.environ.get("AWS_SECRET_ACCESS_KEY", "password")) \
        .config("spark.sql.catalog.rest.s3.path-style-access", "true") \
        .config("spark.sql.defaultCatalog", "rest") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    print(f"Spark version: {spark.version}")
    print("Spark セッション作成完了")
    return (spark,)


@app.cell
def _(mo):
    mo.md("""
    ## Part 2: PyIcebergで作成したテーブルをSparkで読む

    前のノートブック（01_pyiceberg_intro.py）で作成した `demo.users` テーブルを
    Sparkで読み取ります。
    """)
    return


@app.cell
def _(spark):
    # Sparkでテーブル一覧を確認
    spark.sql("SHOW NAMESPACES").show()
    return


@app.cell
def _(spark):
    # demo名前空間のテーブル一覧
    spark.sql("SHOW TABLES IN demo").show()
    return


@app.cell
def _(spark):
    # PyIcebergで作成したテーブルをSparkで読み取り
    df_spark = spark.sql("SELECT * FROM rest.demo.users")
    df_spark.show()
    return


@app.cell
def _(mo):
    mo.md("""
    ## Part 3: Sparkでデータを追加

    Sparkから新しいデータを追加し、PyIcebergで読み取れることを確認します。
    """)
    return


@app.cell
def _(spark):
    # Sparkでデータを追加
    spark.sql("""
        INSERT INTO rest.demo.users VALUES
        (4, 'David', 'david@example.com', 88.0),
        (5, 'Eve', 'eve@example.com', 95.5)
    """)
    print("Sparkから2件のデータを追加しました")
    return


@app.cell
def _(spark):
    # 追加後のデータを確認（Spark）
    spark.sql("SELECT * FROM rest.demo.users ORDER BY user_id").show()
    return


@app.cell
def _(mo):
    mo.md("""
    ## Part 4: PyIcebergで追加データを確認

    Sparkで追加したデータをPyIcebergで読み取れるか確認します。
    """)
    return


@app.cell
def _():
    from pyiceberg.catalog import load_catalog
    import os as os2

    # PyIcebergでカタログに接続
    catalog = load_catalog(
        "rest",
        **{
            "type": "rest",
            "uri": os2.environ.get("CATALOG_URI", "http://rest-catalog:8181"),
            "s3.endpoint": os2.environ.get("S3_ENDPOINT", "http://minio:9000"),
            "s3.access-key-id": os2.environ.get("AWS_ACCESS_KEY_ID", "admin"),
            "s3.secret-access-key": os2.environ.get("AWS_SECRET_ACCESS_KEY", "password"),
            "s3.region": "us-east-1",
        }
    )

    # テーブルをロード（リフレッシュ）
    table = catalog.load_table("demo.users")
    table.refresh()

    # PyIcebergでデータを読み取り
    df_pyiceberg = table.scan().to_pandas()
    df_pyiceberg
    return (table,)


@app.cell
def _(mo):
    mo.md("""
    ## Part 5: スナップショットの確認

    PyIcebergとSparkの両方から操作した結果、スナップショットがどのように
    記録されているか確認します。
    """)
    return


@app.cell
def _(mo, table):
    # スナップショット履歴
    snapshots_data = []
    for snap in table.metadata.snapshots:
        app_id = snap.summary.get("app-id") or ""
        snapshots_data.append({
            "Snapshot ID": snap.snapshot_id,
            "Operation": snap.summary.get("operation", "N/A"),
            "Added Records": snap.summary.get("added-records", "0"),
            "Engine": "PyIceberg" if "pyiceberg" in app_id.lower() else "Spark",
        })

    mo.ui.table(snapshots_data)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Part 6: Sparkでスキーマ進化

    Sparkを使ってカラムを追加し、スキーマ進化を確認します。
    """)
    return


@app.cell
def _(spark):
    # カラムを追加
    spark.sql("ALTER TABLE rest.demo.users ADD COLUMNS (created_at TIMESTAMP)")
    print("カラム 'created_at' を追加しました")

    # スキーマを確認
    spark.sql("DESCRIBE rest.demo.users").show()
    return


@app.cell
def _(table):
    # PyIcebergでもスキーマ変更を確認
    table.refresh()

    print("=== PyIcebergでスキーマを確認 ===")
    for field in table.schema().fields:
        print(f"  {field.field_id}: {field.name} ({field.field_type}) required={field.required}")
    return


@app.cell
def _(mo):
    mo.md("""
    ---

    ## まとめ

    ✅ **マルチエンジン相互運用性を確認しました**

    | 操作 | PyIceberg | Spark |
    |------|-----------|-------|
    | テーブル作成 | ✅ | ✅ |
    | データ読み取り | ✅ | ✅ |
    | データ書き込み | ✅ | ✅ |
    | スキーマ進化 | ✅ | ✅ |
    | スナップショット共有 | ✅ | ✅ |

    REST Catalogを介することで、異なるエンジンが同じIcebergテーブルに
    一貫してアクセスできることが確認できました。
    """)
    return


@app.cell
def _():
    # クリーンアップ（必要に応じて）
    # spark.stop()
    return


if __name__ == "__main__":
    app.run()
