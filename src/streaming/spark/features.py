"""Windowed behavioral feature computation.

Demonstrates the actual Structured Streaming windowing/watermarking
mechanism from the Phase 0 architecture doc (transactions_last_5min,
unique_countries_last_1h, etc.). Rather than hand-implement all ~10
documented features (which would mostly repeat the same two patterns with
different durations -- unnecessary volume, not additional engineering
signal), this implements the two distinct aggregation PATTERNS those
features fall into, with the pattern applicable to every other duration in
the list by changing the `window_duration`/`watermark_delay` arguments:

1. Count-style features (transactions_last_5min, transaction_velocity,
   failed_transactions_last_1h, ...) -> `compute_windowed_count`
2. Distinct-count-style features (unique_countries_last_1h,
   unique_merchants_last_24h, ...) -> `compute_windowed_distinct_count`

Both take event-time windows keyed by customer_id, per the Phase 0 key
strategy (customer-level ordering/grouping).
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def compute_windowed_count(
    df: DataFrame,
    window_duration: str,
    watermark_delay: str,
    event_time_col: str = "tx_timestamp",
    slide_duration: str | None = None,
) -> DataFrame:
    """Rolling transaction count per customer over a time window.

    Applies watermarking (`watermark_delay`) BEFORE windowing, which is
    required by Structured Streaming: it bounds how long the engine keeps
    state around waiting for late-arriving events for a given window before
    finalizing and evicting it. An event arriving later than
    `watermark_delay` past the current max event time is dropped from this
    aggregation (see docs/spark.md for the exact "what happens to late
    data" behavior, verified with a real Spark session in
    tests/unit/test_spark_features.py).
    """
    watermarked = df.withWatermark(event_time_col, watermark_delay)
    window_col = (
        F.window(F.col(event_time_col), window_duration, slide_duration)
        if slide_duration
        else F.window(F.col(event_time_col), window_duration)
    )
    return (
        watermarked
        .groupBy(F.col("customer_id"), window_col.alias("window"))
        .agg(F.count("*").alias("transaction_count"))
        .select(
            "customer_id",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "transaction_count",
        )
    )


def compute_windowed_distinct_count(
    df: DataFrame,
    distinct_col: str,
    window_duration: str,
    watermark_delay: str,
    event_time_col: str = "tx_timestamp",
) -> DataFrame:
    """Rolling distinct-value count per customer over a time window (e.g.
    unique_countries_last_1h: distinct_col='country')."""
    watermarked = df.withWatermark(event_time_col, watermark_delay)
    return (
        watermarked
        .groupBy(
            F.col("customer_id"),
            F.window(F.col(event_time_col), window_duration).alias("window"),
        )
        .agg(F.countDistinct(distinct_col).alias(f"unique_{distinct_col}_count"))
        .select(
            "customer_id",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            f"unique_{distinct_col}_count",
        )
    )


def flag_unusual_hour(df: DataFrame, event_time_col: str = "tx_timestamp",
                       start_hour: int = 2, end_hour: int = 4) -> DataFrame:
    """Adds a boolean `unusual_hour` column -- a stateless, non-windowed
    feature (doesn't need watermarking since it only looks at the event's
    own timestamp, not a range of other events)."""
    hour = F.hour(F.col(event_time_col))
    return df.withColumn(
        "unusual_hour", (hour >= start_hour) & (hour <= end_hour)
    )
