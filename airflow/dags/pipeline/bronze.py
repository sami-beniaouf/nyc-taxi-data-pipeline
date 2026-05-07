"""
Bronze ingestion module — NYC Taxi pipeline.
Downloads Yellow Taxi Parquet from NYC TLC public source and writes to Delta.
"""

import os
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date


SOURCE_URL      = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
SOURCE_FILENAME = "yellow_tripdata_2024-01.parquet"
INGESTION_MONTH = "2024-01"
LOCAL_TMP       = "/tmp/nyc_taxi"


def get_spark():
    return (
        SparkSession.builder
        .appName("nyc-taxi-bronze")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def download_source() -> str:
    """Download source Parquet from NYC TLC to local tmp. Returns local file path."""
    os.makedirs(LOCAL_TMP, exist_ok=True)
    local_file = f"{LOCAL_TMP}/{SOURCE_FILENAME}"
    if os.path.exists(local_file):
        print(f"[Bronze] Source file already exists at {local_file} — skipping download")
        return local_file
    print(f"[Bronze] Downloading {SOURCE_URL} ...")
    urllib.request.urlretrieve(SOURCE_URL, local_file)
    size_mb = os.path.getsize(local_file) / (1024 * 1024)
    print(f"[Bronze] Downloaded — size: {size_mb:.2f} MB")
    return local_file


def run(bronze_path: str) -> int:
    """
    Full Bronze ingestion run.
    Returns record count written to Delta.
    """
    spark      = get_spark()
    local_file = download_source()

    df_source = spark.read.parquet(f"file://{local_file}")
    source_count = df_source.count()
    print(f"[Bronze] Source record count: {source_count:,}")

    df_bronze = (
        df_source
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_ingestion_date",      to_date(current_timestamp()))
        .withColumn("_source_file",         lit(SOURCE_FILENAME))
        .withColumn("_ingestion_month",     lit(INGESTION_MONTH))
    )

    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("_ingestion_month")
        .save(bronze_path)
    )

    bronze_count = spark.read.format("delta").load(bronze_path).count()
    assert source_count == bronze_count, \
        f"Row count mismatch: source={source_count}, bronze={bronze_count}"

    print(f"[Bronze] ✓ Written {bronze_count:,} rows to {bronze_path}")
    return bronze_count
