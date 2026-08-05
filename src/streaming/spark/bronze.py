"""Bronze layer: raw, immutable, append-only Delta write.

Bronze exists to preserve exactly what was received (envelope-parsed, with
ingestion metadata added -- no business validation, no filtering to a
single event_type, no dedup) so that any bug found later in Silver-layer
logic can be fixed and replayed from Bronze without re-reading Kafka (whose
retention is much shorter than Delta's). This mirrors the Phase 0
architecture's stated purpose for the Bronze layer. Silver is where
event_type filtering, payload parsing, and validation happen -- see
silver.py.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

# Partitioned by event_type (a handful of distinct values -- customers,
# accounts, transactions, merchant_events -- so this doesn't fragment into
# many small partitions) and ingestion_date (bounds partition growth to one
# new partition per type per day, and lets a replay/backfill job prune to a
# specific day without scanning the whole table). See
# docs/spark-streaming.md §partitioning for the full reasoning, including
# why customer_id was explicitly rejected as a Bronze partition column.
BRONZE_PARTITION_COLUMNS = ["event_type", "ingestion_date"]


def write_bronze_stream(envelope_df: DataFrame, delta_path: str,
                         checkpoint_path: str, trigger_interval: str = "10 seconds"
                         ) -> StreamingQuery:
    """Append parsed envelopes (all event types, ingestion metadata
    included, no business validation) to the Bronze Delta table.

    - outputMode("append"): Bronze never updates or deletes existing rows.
    - partitionBy: see BRONZE_PARTITION_COLUMNS above.
    - mergeSchema=true: additive schema changes (the documented V1->V2
      transaction evolution: new optional fields like `currency`,
      `merchant_category`) are accepted without manual migration; a
      genuinely incompatible change would still fail loudly here rather
      than being silently coerced, since mergeSchema only allows adding
      columns, not retyping/removing them.
    - checkpointLocation: Spark records processed Kafka offsets AND Delta
      transaction state here after each successful micro-batch commit --
      see docs/spark-streaming.md for the precise "what happens if this job
      crashes mid-batch" answer (the Delta-Lake-specific version of the
      same at-least-once-with-idempotent-writes story from Phase 2's Kafka
      consumer).
    """
    return (
        envelope_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .partitionBy(*BRONZE_PARTITION_COLUMNS)
        .trigger(processingTime=trigger_interval)
        .start(delta_path)
    )
