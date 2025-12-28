# ===========================================
# marimo + PyIceberg + PySpark
# マルチエンジンIceberg検証環境
# ===========================================

FROM python:3.11-slim

# Java（Spark用）- default-jdkを使用
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# 環境変数
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYSPARK_PYTHON=python3

WORKDIR /app

# Python依存関係
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# PySparkに内包されたSparkのjarsディレクトリにIceberg JARを追加
RUN PYSPARK_JARS=$(python -c "import pyspark; print(pyspark.__path__[0])")/jars && \
    curl -L -o ${PYSPARK_JARS}/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar && \
    curl -L -o ${PYSPARK_JARS}/iceberg-aws-bundle-1.6.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar

# ノートブック用ディレクトリ
RUN mkdir -p /app/notebooks /app/warehouse

EXPOSE 2718

# marimo起動（編集モード）
CMD ["marimo", "edit", "--host", "0.0.0.0", "-p", "2718", "--no-token", "/app/notebooks"]
