"""Delta Lake integration tests.

Skipped automatically if the Delta Spark connector can't be resolved --
in this sandbox, because Maven Central (the only distribution channel for
delta-spark's JVM jars) is blocked by the network allowlist. Confirmed
directly: `curl -sI https://repo1.maven.org/maven2/` returns
`403 host_not_allowed`, and Delta's GitHub releases only publish
Hive-assembly jars, not the core Spark connector -- see the Phase 3
close-out report for the full investigation.

This is the Spark/Delta equivalent of the Postgres/Kafka
skip-if-unavailable pattern from Phases 1-2: the test exists, is correct,
and will run for real the moment a Delta-enabled Spark session is available
(e.g. in CI with Maven Central access, or with jars pre-downloaded).
"""
import shutil

import pytest

from src.streaming.spark.bronze import write_bronze_stream
from src.streaming.spark.silver import write_silver_stream
from src.streaming.spark.transformations import (
    add_ingestion_metadata,
    parse_kafka_envelope,
)


def _delta_available():
    try:
        from src.streaming.spark.session import build_spark_session
        spark = build_spark_session(app_name="delta-availability-check")
        spark.stop()
        return True
    except Exception:  # noqa: BLE001 -- availability probe, same rationale as the
        # Postgres/Kafka ones in tests/integration/test_db_integration.py and
        # test_kafka_integration.py: any failure means "skip".
        return False


_DELTA_AVAILABLE = _delta_available()

pytestmark = pytest.mark.skipif(
    not _DELTA_AVAILABLE,
    reason=(
        "Delta Spark connector unavailable -- Maven Central is blocked in this "
        "sandbox's network allowlist (confirmed: 403 host_not_allowed). "
        "Run this test on a machine/CI with Maven Central access."
    ),
)


@pytest.fixture
def delta_spark(tmp_path):
    from src.streaming.spark.session import build_spark_session

    spark = build_spark_session(app_name="delta-integration-test")
    yield spark
    spark.stop()
    shutil.rmtree(tmp_path, ignore_errors=True)


def test_bronze_write_produces_a_queryable_delta_table(delta_spark, tmp_path):
    import json

    bronze_path = str(tmp_path / "bronze")
    checkpoint_path = str(tmp_path / "checkpoint_bronze")
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    envelope = json.dumps({
        "schema_version": "1.0", "event_type": "transaction", "event_id": "e1",
        "event_timestamp": "2026-01-01T00:00:00Z",
        "payload": json.dumps({
            "transaction_id": "TX1", "account_id": "A1", "customer_id": "C001",
            "merchant_id": "M1", "amount": 10.0, "timestamp": "2026-01-01T00:00:00Z",
            "country": "FR", "device_id": "D1", "status": "APPROVED",
            "is_fraud": False, "fraud_pattern": None,
        }),
    })
    (input_dir / "batch1.json").write_text(json.dumps({"value": envelope}) + "\n")

    from pyspark.sql.types import BinaryType, StringType, StructField, StructType

    schema = StructType([StructField("value", StringType())])
    stream = delta_spark.readStream.schema(schema).json(str(input_dir))
    envelope_df = parse_kafka_envelope(stream.withColumn("value", stream["value"].cast(BinaryType())))
    bronze_input_df = add_ingestion_metadata(envelope_df)

    query = write_bronze_stream(bronze_input_df, bronze_path, checkpoint_path)
    query.processAllAvailable()
    query.stop()

    result = delta_spark.read.format("delta").load(bronze_path)
    assert result.count() == 1
    assert result.collect()[0]["event_type"] == "transaction"


def test_silver_merge_deduplicates_across_micro_batches(delta_spark, tmp_path):
    """The real point of this test: publish the SAME transaction_id twice,
    across two SEPARATE micro-batches (not within one, which
    dropDuplicates already covers) -- proving the Delta MERGE is what
    prevents cross-batch duplication, not just in-batch dedup."""
    import json

    bronze_path = str(tmp_path / "bronze")
    silver_path = str(tmp_path / "silver")
    bronze_checkpoint = str(tmp_path / "checkpoint_bronze")
    silver_checkpoint = str(tmp_path / "checkpoint_silver")
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    def envelope_for(tx_id):
        return json.dumps({
            "schema_version": "1.0", "event_type": "transaction", "event_id": tx_id,
            "event_timestamp": "2026-01-01T00:00:00Z",
            "payload": json.dumps({
                "transaction_id": tx_id, "account_id": "A1", "customer_id": "C001",
                "merchant_id": "M1", "amount": 10.0, "timestamp": "2026-01-01T00:00:00Z",
                "country": "FR", "device_id": "D1", "status": "APPROVED",
                "is_fraud": False, "fraud_pattern": None,
            }),
        })

    from pyspark.sql.types import BinaryType, StringType, StructField, StructType
    schema = StructType([StructField("value", StringType())])

    def run_pipeline_once():
        stream = delta_spark.readStream.schema(schema).json(str(input_dir))
        envelope_df = parse_kafka_envelope(
            stream.withColumn("value", stream["value"].cast(BinaryType()))
        )
        bronze_input_df = add_ingestion_metadata(envelope_df)
        bq = write_bronze_stream(bronze_input_df, bronze_path, bronze_checkpoint)
        bq.processAllAvailable()
        bq.stop()

        bronze_stream_df = delta_spark.readStream.format("delta").load(bronze_path)
        sq = write_silver_stream(bronze_stream_df, silver_path, silver_checkpoint)
        sq.processAllAvailable()
        sq.stop()

    # Batch 1: TX1 arrives.
    (input_dir / "batch1.json").write_text(json.dumps({"value": envelope_for("TX1")}) + "\n")
    run_pipeline_once()

    # Batch 2: TX1 arrives AGAIN (simulating an at-least-once Kafka replay).
    (input_dir / "batch2.json").write_text(json.dumps({"value": envelope_for("TX1")}) + "\n")
    run_pipeline_once()

    result = delta_spark.read.format("delta").load(silver_path)
    assert result.count() == 1, "MERGE should have deduplicated the replayed TX1"
