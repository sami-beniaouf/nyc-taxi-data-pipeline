"""
Silver transformation module — NYC Taxi pipeline.
Two-stage quality framework: completeness check → validity check.
Valid rows enriched with derived columns and validated via Great Expectations.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, unix_timestamp, hour,
    dayofweek, round as spark_round
)
import great_expectations as ge


CRITICAL_COLS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance",
    "fare_amount", "total_amount"
]


def get_spark():
    return (
        SparkSession.builder
        .appName("nyc-taxi-silver")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def split_completeness(df_bronze):
    """Split Bronze into complete and incomplete rows."""
    null_predicate = None
    for c in CRITICAL_COLS:
        expr = col(c).isNull()
        null_predicate = expr if null_predicate is None else (null_predicate | expr)
    return df_bronze.filter(~null_predicate), df_bronze.filter(null_predicate)


def split_validity(df_complete):
    """Split complete rows into valid and invalid based on business rules."""
    valid_predicate = (
        (col("passenger_count") > 0)
        & (col("trip_distance") > 0)
        & (col("trip_distance") < 500)
        & (col("fare_amount") > 0)
        & (col("total_amount") > 0)
        & (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
    )
    return df_complete.filter(valid_predicate), df_complete.filter(~valid_predicate)


def tag_invalid(df_invalid):
    """Tag invalid rows with granular rejection reason codes."""
    return df_invalid.withColumn(
        "quarantine_reason",
        when(col("passenger_count") <= 0,                                  lit("INVALID_PASSENGER_COUNT"))
        .when(col("trip_distance") <= 0,                                   lit("INVALID_DISTANCE_ZERO_OR_NEG"))
        .when(col("trip_distance") >= 500,                                 lit("INVALID_DISTANCE_TOO_LONG"))
        .when(col("fare_amount") <= 0,                                     lit("INVALID_FARE"))
        .when(col("total_amount") <= 0,                                    lit("INVALID_TOTAL_AMOUNT"))
        .when(col("tpep_dropoff_datetime") <= col("tpep_pickup_datetime"), lit("INVALID_TIME_ORDER"))
        .otherwise(                                                         lit("INVALID_OTHER"))
    )


def enrich(df_valid):
    """Add derived columns for downstream Gold aggregations."""
    df = (
        df_valid
        .withColumn(
            "trip_duration_minutes",
            spark_round(
                (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60,
                2
            )
        )
        .withColumn("pickup_hour",        hour("tpep_pickup_datetime"))
        .withColumn("pickup_day_of_week", dayofweek("tpep_pickup_datetime"))
        .withColumn(
            "is_weekend",
            when(dayofweek("tpep_pickup_datetime").isin([1, 7]), True).otherwise(False)
        )
        .withColumn(
            "avg_speed_mph",
            spark_round(col("trip_distance") / (col("trip_duration_minutes") / 60), 2)
        )
    )
    # Remove trips with unreasonable durations
    return df.filter(
        (col("trip_duration_minutes") > 0) & (col("trip_duration_minutes") < 1440)
    )


def validate_ge(df_silver) -> None:
    """
    Great Expectations validation gate.
    Raises ValueError if any expectation fails — Silver write is aborted.
    """
    sample_pdf = df_silver.limit(100_000).toPandas()
    ge_df      = ge.from_pandas(sample_pdf)

    results = [
        ge_df.expect_column_values_to_not_be_null("tpep_pickup_datetime"),
        ge_df.expect_column_values_to_not_be_null("tpep_dropoff_datetime"),
        ge_df.expect_column_values_to_be_between("passenger_count",       min_value=1,  max_value=9),
        ge_df.expect_column_values_to_be_between("trip_distance",         min_value=0,  max_value=500),
        ge_df.expect_column_values_to_be_between("fare_amount",           min_value=0,  max_value=10000),
        ge_df.expect_column_values_to_be_between("trip_duration_minutes", min_value=0,  max_value=1440),
        ge_df.expect_column_values_to_be_in_set("is_weekend",            [True, False]),
    ]

    failed = [r for r in results if not r["success"]]
    print(f"[Silver] GE — passed: {len(results) - len(failed)}/{len(results)}")

    if failed:
        for f in failed:
            print(f"  ❌ {f['expectation_config']['expectation_type']}: {f['result']}")
        raise ValueError("GE validation gate FAILED — Silver write aborted")

    print("[Silver] ✓ All GE expectations passed")


def run(bronze_path: str, silver_path: str, quarantine_path: str) -> int:
    """
    Full Silver transformation run.
    Returns record count written to Silver Delta.
    """
    spark     = get_spark()
    df_bronze = spark.read.format("delta").load(bronze_path)
    print(f"[Silver] Bronze record count: {df_bronze.count():,}")

    # Stage 1: completeness
    df_complete, df_incomplete = split_completeness(df_bronze)

    # Stage 2: validity
    df_valid, df_invalid = split_validity(df_complete)

    print(f"[Silver] Valid      : {df_valid.count():,}")
    print(f"[Silver] Invalid    : {df_invalid.count():,}")
    print(f"[Silver] Incomplete : {df_incomplete.count():,}")

    # Write quarantine
    df_quarantine = (
        df_incomplete.withColumn("quarantine_reason", lit("INCOMPLETE_NULL_IN_CRITICAL_FIELD"))
        .unionByName(tag_invalid(df_invalid))
    )
    (
        df_quarantine.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("quarantine_reason")
        .save(quarantine_path)
    )
    print(f"[Silver] ✓ Quarantine written: {quarantine_path}")

    # Enrich valid rows
    df_silver = enrich(df_valid)

    # GE validation gate
    validate_ge(df_silver)

    # Write Silver
    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("pickup_hour")
        .save(silver_path)
    )

    silver_count = spark.read.format("delta").load(silver_path).count()
    print(f"[Silver] ✓ Written {silver_count:,} rows to {silver_path}")
    return silver_count
