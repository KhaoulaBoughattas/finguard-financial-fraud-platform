"""Demonstrates, with a REAL running Structured Streaming query (not a
batch-mode stand-in), that an event arriving later than the configured
watermark delay for its window is dropped rather than counted -- the
concrete mechanism behind the Phase 0/2 answer to "what happens to late
data" and "what happens if Spark crashes mid-batch" (checkpointed state
persists watermark/window progress across query restarts, exactly as it
would across a crash-and-restart).

Uses a JSON file source + `trigger(availableNow=True)` across several
separate query starts sharing one checkpoint directory, rather than Kafka
or a fixed-duration `awaitTermination(timeout)` -- deterministic (no
sleep-and-hope timing), and requires nothing beyond local Spark.
"""
import json

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from src.streaming.spark.features import compute_windowed_count

SCHEMA = StructType([
    StructField("customer_id", StringType()),
    StructField("tx_timestamp", TimestampType()),
])


def _write_batch(input_dir, filename: str, rows: list[tuple[str, str]]) -> None:
    path = input_dir / filename
    with open(path, "w") as f:
        f.writelines(json.dumps({"customer_id": customer_id, "tx_timestamp": ts}) + "\n" for customer_id, ts in rows)


def _run_available_now(spark, input_dir, checkpoint_dir, captured: list) -> None:
    stream = spark.readStream.schema(SCHEMA).json(str(input_dir))
    windowed = compute_windowed_count(stream, "5 minutes", "2 minutes")
    query = (
        windowed.writeStream
        .foreachBatch(lambda df, batch_id: captured.append(df.collect()))
        .option("checkpointLocation", str(checkpoint_dir))
        .outputMode("update")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


def test_late_event_beyond_watermark_is_dropped_not_counted(spark, tmp_path):
    input_dir = tmp_path / "input"
    checkpoint_dir = tmp_path / "checkpoint"
    input_dir.mkdir()
    checkpoint_dir.mkdir()
    captured: list = []

    # Batch 1: one on-time event for C001 at 10:00 -> window [10:00, 10:05)
    _write_batch(input_dir, "batch1.json", [("C001", "2026-01-01T10:00:00")])
    _run_available_now(spark, input_dir, checkpoint_dir, captured)

    window_10_00_rows = [
        row for batch in captured for row in batch
        if str(row["window_start"]) == "2026-01-01 10:00:00"
    ]
    assert len(window_10_00_rows) == 1
    assert window_10_00_rows[0]["transaction_count"] == 1

    # Batch 2: an event far enough ahead (10:08) that the watermark
    # (max_event_time - 2min = 10:06) now exceeds the [10:00,10:05) window's
    # end (10:05) -> that window is finalized/evicted from state.
    _write_batch(input_dir, "batch2.json", [("C001", "2026-01-01T10:08:00")])
    _run_available_now(spark, input_dir, checkpoint_dir, captured)

    # Batch 3: a LATE event at 10:01 -- inside the [10:00,10:05) window on
    # event-time, but arriving after that window was already finalized.
    _write_batch(input_dir, "batch3.json", [("C001", "2026-01-01T10:01:00")])
    _run_available_now(spark, input_dir, checkpoint_dir, captured)

    # The critical assertion: no batch ever reports count=2 (or any updated
    # value) for the 10:00 window after it was finalized -- the late event
    # was dropped, not merged in.
    late_window_rows = [
        row for batch in captured for row in batch
        if str(row["window_start"]) == "2026-01-01 10:00:00"
    ]
    assert len(late_window_rows) == 1, (
        "the 10:00 window should only have been emitted once (batch 1); "
        "any further emission would mean the late event was NOT dropped"
    )
    assert late_window_rows[0]["transaction_count"] == 1
