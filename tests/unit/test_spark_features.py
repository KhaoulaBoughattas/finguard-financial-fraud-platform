from datetime import datetime, timedelta

from src.streaming.spark.features import (
    compute_windowed_count,
    compute_windowed_distinct_count,
    flag_unusual_hour,
)

# Deliberately naive (no tzinfo): these feed directly into Spark
# TimestampType columns via createDataFrame, which in this test suite's
# local-timezone-free setup expects naive datetimes -- adding tzinfo here
# would change what Spark actually stores, not just satisfy the linter.
BASE = datetime(2026, 1, 1, 10, 0, 0)  # noqa: DTZ001


def test_windowed_count_groups_by_customer_and_window(spark):
    rows = [
        ("C001", BASE), ("C001", BASE + timedelta(minutes=2)),
        ("C001", BASE + timedelta(minutes=3)), ("C002", BASE),
    ]
    df = spark.createDataFrame(rows, ["customer_id", "tx_timestamp"])
    result = compute_windowed_count(df, "5 minutes", "1 minute").collect()
    counts = {row["customer_id"]: row["transaction_count"] for row in result}
    assert counts["C001"] == 3
    assert counts["C002"] == 1


def test_windowed_count_separates_events_in_different_windows(spark):
    rows = [
        ("C001", BASE),
        ("C001", BASE + timedelta(minutes=10)),  # different 5-minute window
    ]
    df = spark.createDataFrame(rows, ["customer_id", "tx_timestamp"])
    result = compute_windowed_count(df, "5 minutes", "1 minute").collect()
    assert len(result) == 2
    assert all(row["transaction_count"] == 1 for row in result)


def test_windowed_distinct_count_counts_unique_values_per_window(spark):
    rows = [
        ("C001", BASE, "FR"), ("C001", BASE + timedelta(minutes=1), "FR"),
        ("C001", BASE + timedelta(minutes=2), "DE"),
    ]
    df = spark.createDataFrame(rows, ["customer_id", "tx_timestamp", "country"])
    result = compute_windowed_distinct_count(df, "country", "5 minutes", "1 minute").collect()
    assert result[0]["unique_country_count"] == 2  # FR, DE -- not 3 (FR repeats)


def test_flag_unusual_hour_marks_late_night_transactions(spark):
    rows = [
        ("C001", datetime(2026, 1, 1, 3, 0, 0)),  # noqa: DTZ001 -- naive on purpose, see BASE above
        ("C001", datetime(2026, 1, 1, 14, 0, 0)),  # noqa: DTZ001
    ]
    df = spark.createDataFrame(rows, ["customer_id", "tx_timestamp"])
    result = {row["tx_timestamp"].hour: row["unusual_hour"] for row in flag_unusual_hour(df).collect()}
    assert result[3] is True
    assert result[14] is False
