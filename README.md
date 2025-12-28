# 🧊 marimo + Iceberg マルチエンジン検証環境

Docker Composeを使った、Apache Icebergのマルチエンジン検証環境です。

PyIceberg、PySpark、Trinoの3つのエンジンから同じIcebergテーブルにアクセスし、マルチエンジンでの相互運用性を検証できます。

## 📖 関連記事

本リポジトリはZennの記事シリーズ「Apache Iceberg マルチエンジン実践ガイド」の補足資料です。

| 回 | 記事 | 対応ノートブック |
|----|------|-----------------|
| 第1回 | [Docker ComposeでApache Icebergマルチエンジン検証環境を構築する](https://zenn.dev/toshiro3) | `01_pyiceberg_intro.py` |
| 第2回 | [PySparkとPyIcebergでIcebergテーブルを相互運用する](https://zenn.dev/toshiro3) | `02_pyspark_multiengine.py` |
| 第3回 | [TrinoでIcebergテーブルをSQL分析する](https://zenn.dev/toshiro3) | `03_trino_sql.py` |

```
┌─────────────────────────────────────────────────────────────┐
│                      marimo (port: 2718)                     │
│              PyIceberg / PySpark / Trino Client              │
└──────────────────────────┬──────────────────────────────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
┌─────────────────────┐    ┌─────────────────────┐
│   REST Catalog      │    │      Trino          │
│   (port: 8181)      │    │   (port: 8080)      │
└──────────┬──────────┘    └──────────┬──────────┘
           │                          │
           └────────────┬─────────────┘
                        │
                        ▼
           ┌─────────────────────┐
           │       MinIO         │
           │  S3 API: 9000       │
           │  Console: 9001      │
           └─────────────────────┘
```

## 🚀 クイックスタート

### 1. リポジトリのクローン

```bash
git clone https://github.com/toshiro3/iceberg-multiengine-lab.git
cd iceberg-multiengine-lab
```

### 2. 環境の起動

```bash
docker compose up -d
```

初回起動時はDockerイメージのビルドに数分かかります。

### 3. サービスへのアクセス

| サービス | URL | 用途 |
|---------|-----|------|
| marimo | http://localhost:2718 | ノートブック編集 |
| MinIO Console | http://localhost:9001 | ストレージ管理（admin/password） |
| Trino | http://localhost:8080 | SQLクエリUI |
| REST Catalog | http://localhost:8181 | カタログAPI |

### 4. ノートブックの実行

1. http://localhost:2718 にアクセス
2. `01_pyiceberg_intro.py` から順に実行

## 📒 ノートブック一覧

| ファイル | 内容 | 対応記事 |
|---------|------|---------|
| `01_pyiceberg_intro.py` | PyIcebergの基本操作（テーブル作成、データ追加） | 第1回 |
| `02_pyspark_multiengine.py` | PySparkとの相互運用性検証 | 第2回 |
| `03_trino_sql.py` | TrinoでのSQLクエリ、メタデータテーブル活用 | 第3回 |

## ⚠️ 注意事項

### REST Catalogのデータ永続性

REST Catalogはデフォルトで**SQLiteのメモリモード**で動作します。そのため、コンテナを再起動するとカタログ情報（テーブル定義など）がリセットされます。

```bash
# コンテナ再起動後は01から再実行が必要
docker compose down
docker compose up -d
# → 01_pyiceberg_intro.py からやり直し
```

本番環境ではPostgreSQLなどの外部DBを指定することで永続化が可能です。

### MinIOのデータ

MinIOのデータは `minio-data/` ディレクトリに永続化されます。完全にリセットしたい場合：

```bash
docker compose down -v
rm -rf minio-data/*
```

## 🔧 構成コンポーネント

### marimo
- Python製のリアクティブノートブック
- PyIceberg + PySpark対応
- Git-friendlyな.pyファイル形式

### REST Catalog
- Iceberg REST Catalog仕様準拠
- 複数エンジンからの共通アクセスポイント
- tabulario/iceberg-rest イメージ使用

### MinIO
- S3互換オブジェクトストレージ
- Icebergのデータ/メタデータを保存

### Trino
- 高速分散SQLエンジン
- Icebergコネクタ内蔵

## 📁 ディレクトリ構成

```
iceberg-multiengine-lab/
├── docker-compose.yml      # Docker Compose設定
├── marimo/
│   ├── Dockerfile          # marimoイメージ定義
│   └── requirements.txt    # Python依存関係
├── notebooks/              # marimoノートブック（完成版）
│   ├── 01_pyiceberg_intro.py
│   ├── 02_pyspark_multiengine.py
│   └── 03_trino_sql.py
├── trino/
│   └── catalog/
│       └── iceberg.properties  # Trinoカタログ設定
├── warehouse/              # Icebergローカルデータ（デバッグ用）
└── minio-data/             # MinIOデータ（永続化）
```

## 🛠️ トラブルシューティング

### コンテナのログ確認

```bash
# 全サービスのログ
docker compose logs -f

# 特定サービスのログ
docker compose logs -f marimo
docker compose logs -f rest-catalog
```

### 環境のリセット

```bash
# コンテナ停止・削除
docker compose down

# データも含めて完全リセット
docker compose down -v
rm -rf minio-data/*
```

### marimoコンテナの再ビルド

ノートブックやDockerfileを変更した場合：

```bash
docker compose build --no-cache marimo
docker compose up -d
```

## 📚 参考資料

- [Apache Iceberg公式](https://iceberg.apache.org/)
- [PyIceberg](https://py.iceberg.apache.org/)
- [marimo](https://marimo.io/)
- [Trino](https://trino.io/)
- [MinIO](https://min.io/)

## ライセンス

MIT
