"""Builds the SparkSession used by the Phase 3 streaming pipeline.

Two builders are provided deliberately:

- `build_spark_session()`: the REAL production path -- registers the Delta
  Lake catalog/extensions and resolves the delta-spark JVM artifact via
  Maven coordinates (`spark.jars.packages`). This is what a real deployment
  (and a real Docker/CI environment) would use.
- `build_local_test_session()`: used ONLY by this repository's own tests.
  It builds a plain Spark session with no Delta extension, because the
  sandbox this code was developed in has no access to Maven Central (Delta's
  JVM connector is not distributed any other way -- see docs/spark.md and
  the Phase 3 close-out report). Tests that need Delta itself are skipped
  against this session, not silently run against a stand-in.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

DELTA_MAVEN_COORDINATE = "io.delta:delta-spark_2.12:3.2.1"


def build_spark_session(app_name: str = "finguard-streaming",
                         master: str = "local[*]") -> SparkSession:
    """Build a SparkSession with Delta Lake enabled.

    Requires network access to Maven Central (or a configured internal
    artifact mirror / pre-downloaded jars on the classpath) to resolve
    `DELTA_MAVEN_COORDINATE` the first time it runs on a given machine.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.jars.packages", DELTA_MAVEN_COORDINATE)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Shuffle partitions default (200) is tuned for a real cluster;
        # locally it just adds overhead for small dev-scale data.
        .config("spark.sql.shuffle.partitions", "4")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_local_test_session(app_name: str = "finguard-streaming-test") -> SparkSession:
    """Plain local Spark session, no Delta extension.

    Used by this repository's own test suite so transformation/windowing/
    watermarking logic can be verified without requiring Maven Central
    access. See module docstring.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
