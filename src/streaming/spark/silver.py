"""Silver layer: schema-validated, cleaned, deduplicated Delta write.

Reads the raw Bronze stream (all event types) and, per micro-batch:
filters to transaction events, parses the payload, validates against
business rules, deduplicates by transaction_id, assigns the partition
column, then MERGEs into Silver. Unlike Bronze (blind append), Silver
writes use `foreachBatch` + Delta `MERGE ... WHEN NOT MATCHED THEN INSERT`
keyed on `transaction_id`. This is the concrete mechanism behind the
"effectively-once" processing semantics documented in the Phase 0
architecture amendments: if this streaming job crashes after writing a
micro-batch's rows but before its checkpoint offset is advanced, Spark
reprocesses that exact micro-batch on restart -- MERGE makes that
reprocessing a no-op for rows already committed, rather than a
duplicate-row bug.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from src.streaming.spark.transformations import (
    add_event_date,
    deduplicate_transactions,
    filter_and_parse_transaction_payloads,
    split_valid_invalid,
)

MERGE_KEY = "transaction_id"

# Partitioned by event_date (the transaction's own event-time date, not
# ingestion date) -- this is what analytical queries and Power BI reports
# actually filter on ("fraud alerts last week"), and keeps daily partition
# sizes roughly even regardless of ingestion timing/replays. customer_id
# was rejected: it's high-cardinality (thousands of customers), which would
# produce many small per-customer files rather than a few well-sized daily
# ones -- the classic Spark "small file problem." See
# docs/spark-streaming.md §partitioning.
SILVER_PARTITION_COLUMNS = ["event_date"]


def upsert_silver_batch(bronze_micro_batch_df: DataFrame, batch_id: int, delta_path: str) -> None:
    """The `foreachBatch` function: filters/parses/validates/dedups one
    micro-batch of raw Bronze envelope rows, then MERGEs the result into
    the Silver Delta table.

    `batch_id` is unused directly but is part of the foreachBatch contract
    (Spark passes it so the function can, if needed, make behavior
    idempotent on retries of the SAME batch_id -- MERGE already gives us
    that here via the natural key, so batch_id doesn't need to be consulted
    explicitly).
    """
    from delta.tables import (
        DeltaTable,  # local import: only needed on the Delta-enabled path
    )

    parsed_df = filter_and_parse_transaction_payloads(bronze_micro_batch_df)
    valid_df, invalid_df = split_valid_invalid(parsed_df)
    deduped_df = deduplicate_transactions(valid_df)
    final_df = add_event_date(deduped_df)

    if invalid_df.take(1):
        # Quarantine, not silent drop -- mirrors the Phase 2 DLQ contract at
        # the Spark layer. A production job would write this to its own
        # Delta table (e.g. silver/transactions_rejected); omitted here to
        # keep Phase 3 scoped to the documented Bronze/Silver requirement.
        invalid_df.write.format("delta").mode("append").save(
            delta_path.replace("silver/transactions", "silver/transactions_rejected")
        )

    spark = bronze_micro_batch_df.sparkSession
    if DeltaTable.isDeltaTable(spark, delta_path):
        target = DeltaTable.forPath(spark, delta_path)
        (
            target.alias("t")
            .merge(final_df.alias("s"), f"t.{MERGE_KEY} = s.{MERGE_KEY}")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            final_df.write.format("delta")
            .partitionBy(*SILVER_PARTITION_COLUMNS)
            .mode("overwrite")
            .save(delta_path)
        )


def write_silver_stream(bronze_stream_df: DataFrame, delta_path: str,
                         checkpoint_path: str, trigger_interval: str = "30 seconds"
                         ) -> StreamingQuery:
    """Reads the raw Bronze stream and, via foreachBatch, filters/parses/
    validates/dedups/MERGEs into Silver, one micro-batch at a time."""
    return (
        bronze_stream_df.writeStream
        .foreachBatch(lambda df, batch_id: upsert_silver_batch(df, batch_id, delta_path))
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start()
    )
