"""
Gold aggregation module — NYC Taxi pipeline.
Produces 3 business-ready KPI tables from Silver Delta.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg,
    round as spark_round, countDistinct,
    to_date, desc
)


def get_spark():
    return (
        SparkSession.builder
        .appName("nyc-taxi-gold")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def build_daily_metrics(df_silver):
    """Grain: 1 row per pickup_date."""
    return (
        df_silver
        .withColumn("pickup_date", to_date("tpep_pickup_datetime"))
        .groupBy("pickup_date")
        .agg(
            count("*").alias("total_trips"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("total_amount"), 2).alias("avg_fare"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance_miles"),
            spark_round(avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
            spark_round(spark_sum("tip_amount"), 2).alias("total_tips"),
            spark_round(avg("tip_amount") / avg("fare_amount") * 100, 2).alias("avg_tip_pct"),
            spark_sum("passenger_count").alias("total_passengers")
        )
        .orderBy("pickup_date")
    )


def build_hourly_patterns(df_silver):
    """Grain: 1 row per (pickup_hour, is_weekend)."""
    return (
        df_silver
        .groupBy("pickup_hour", "is_weekend")
        .agg(
            count("*").alias("total_trips"),
            spark_round(avg("total_amount"), 2).alias("avg_fare"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance_miles"),
            spark_round(avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
            spark_round(avg("avg_speed_mph"), 2).alias("avg_speed_mph"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue")
        )
        .orderBy("is_weekend", "pickup_hour")
    )


def build_zone_performance(df_silver):
    """Grain: 1 row per pickup zone (PULocationID)."""
    return (
        df_silver
        .groupBy("PULocationID")
        .agg(
            count("*").alias("total_trips"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("total_amount"), 2).alias("avg_fare"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance_miles"),
            spark_round(avg("tip_amount"), 2).alias("avg_tip"),
            countDistinct("DOLocationID").alias("unique_destinations")
        )
        .orderBy(desc("total_revenue"))
    )


def run(silver_path: str, gold_daily_path: str, gold_hourly_path: str, gold_zone_path: str) -> dict:
    """
    Full Gold aggregation run.
    Returns dict with row counts for each KPI table.
    """
    spark      = get_spark()
    df_silver  = spark.read.format("delta").load(silver_path)
    print(f"[Gold] Silver record count: {df_silver.count():,}")

    tables = {
        "daily_metrics":    (build_daily_metrics(df_silver),   gold_daily_path),
        "hourly_patterns":  (build_hourly_patterns(df_silver),  gold_hourly_path),
        "zone_performance": (build_zone_performance(df_silver), gold_zone_path),
    }

    counts = {}
    for name, (df, path) in tables.items():
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(path)
        )
        counts[name] = spark.read.format("delta").load(path).count()
        print(f"[Gold] ✓ {name:20s} — {counts[name]:>5} rows → {path}")

    return counts
