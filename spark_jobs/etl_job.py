"""
etl_job.py
──────────
Spark ETL Job — reads from Delta Lake, cleans and transforms data,
then loads aggregated daily totals into ScyllaDB.

Pipeline steps:
  1. Extract   — read customer_transactions Delta table
  2. Deduplicate — drop rows with duplicate transaction_id
  3. Validate  — filter out amount <= 0
  4. Transform — extract date, compute daily totals per customer
  5. Load      — upsert into ScyllaDB daily_customer_totals

Run inside Spark container:
    spark-submit \
      --packages io.delta:delta-spark_2.12:3.1.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
      --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
      /opt/spark/jobs/etl_job.py
"""

import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("ETL")

# ── Configuration ─────────────────────────────────────────────────────────────
DELTA_PATH      = "/opt/spark/delta-lake/customer_transactions"
SCYLLA_HOST     = "scylladb"           # Docker service name
SCYLLA_PORT     = "9042"
SCYLLA_KEYSPACE = "transactions_ks"
SCYLLA_TABLE    = "daily_customer_totals"


# ─────────────────────────────────────────────────────────────────────────────
# 1. Spark Session
# ─────────────────────────────────────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    log.info("Creating Spark session...")
    builder = (
        SparkSession.builder
        .appName("CustomerTransactionsETL")
        .master("spark://spark-master:7077")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0",
        )
        # ScyllaDB / Cassandra connection settings
        .config("spark.cassandra.connection.host", SCYLLA_HOST)
        .config("spark.cassandra.connection.port", SCYLLA_PORT)
        # Performance tuning
        .config("spark.sql.shuffle.partitions", "8")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created ✅")
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# 2. Extract
# ─────────────────────────────────────────────────────────────────────────────
def extract(spark: SparkSession) -> DataFrame:
    log.info(f"Reading Delta table from {DELTA_PATH} ...")
    df = spark.read.format("delta").load(DELTA_PATH)
    count = df.count()
    log.info(f"Extracted {count:,} raw records ✅")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 3. Transform
# ─────────────────────────────────────────────────────────────────────────────
def transform(df: DataFrame) -> DataFrame:
    """
    Steps:
      a) Parse timestamp column to proper TimestampType
      b) Deduplicate on transaction_id  (keep row with latest timestamp)
      c) Filter  amount <= 0
      d) Add transaction_date column
      e) Aggregate: daily_total per (customer_id, transaction_date)
    """

    # ── a) Parse timestamp ─────────────────────────────────────────────
    log.info("Parsing timestamp column...")
    df = df.withColumn(
        "timestamp",
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    )

    # ── b) Deduplication ───────────────────────────────────────────────
    log.info("Deduplicating on transaction_id...")
    before_dedup = df.count()

    # Keep the record with the latest timestamp for each transaction_id
    window = Window.partitionBy("transaction_id").orderBy(F.col("timestamp").desc())
    df = (
        df.withColumn("row_num", F.row_number().over(window))
          .filter(F.col("row_num") == 1)
          .drop("row_num")
    )
    after_dedup = df.count()
    log.info(f"Removed {before_dedup - after_dedup:,} duplicates "
             f"({after_dedup:,} records remaining) ✅")

    # ── c) Data validation — filter out amount <= 0 ────────────────────
    log.info("Filtering invalid amounts (amount <= 0)...")
    before_filter = after_dedup
    df = df.filter(F.col("amount") > 0)
    after_filter = df.count()
    log.info(f"Removed {before_filter - after_filter:,} invalid-amount rows "
             f"({after_filter:,} valid records) ✅")

    # ── d) Extract transaction_date ────────────────────────────────────
    log.info("Adding transaction_date column...")
    df = df.withColumn("transaction_date", F.to_date(F.col("timestamp")))

    # ── e) Aggregate: daily totals per customer ────────────────────────
    log.info("Aggregating daily totals per customer...")
    agg_df = (
        df.groupBy("customer_id", "transaction_date")
          .agg(F.round(F.sum("amount"), 2).alias("daily_total"))
          .orderBy("customer_id", "transaction_date")
    )

    total_rows = agg_df.count()
    log.info(f"Aggregation complete — {total_rows:,} (customer, date) combinations ✅")

    # Show a sample
    log.info("Sample output:")
    agg_df.show(10, truncate=False)

    return agg_df


# ─────────────────────────────────────────────────────────────────────────────
# 4. Data Quality Check
# ─────────────────────────────────────────────────────────────────────────────
def data_quality_check(df: DataFrame) -> bool:
    """Simple DQ gate before loading into ScyllaDB."""
    log.info("Running data quality checks...")
    issues = []

    # Check: no null customer_id
    null_customers = df.filter(F.col("customer_id").isNull()).count()
    if null_customers > 0:
        issues.append(f"Found {null_customers} rows with null customer_id")

    # Check: no null transaction_date
    null_dates = df.filter(F.col("transaction_date").isNull()).count()
    if null_dates > 0:
        issues.append(f"Found {null_dates} rows with null transaction_date")

    # Check: all daily_total > 0 (already filtered, sanity check)
    bad_totals = df.filter(F.col("daily_total") <= 0).count()
    if bad_totals > 0:
        issues.append(f"Found {bad_totals} rows with daily_total <= 0")

    if issues:
        for msg in issues:
            log.error(f"DQ FAIL: {msg}")
        return False

    log.info("All data quality checks passed ✅")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# 5. Load into ScyllaDB
# ─────────────────────────────────────────────────────────────────────────────
def load_to_scylladb(df: DataFrame) -> None:
    """
    Upsert aggregated data into ScyllaDB using the Spark-Cassandra connector.
    Cassandra's INSERT is naturally an upsert (last-write-wins).
    """
    log.info(f"Loading data into ScyllaDB {SCYLLA_KEYSPACE}.{SCYLLA_TABLE} ...")

    # Cast transaction_date to string so Cassandra connector maps it to DATE
    df_to_load = df.withColumn(
        "transaction_date", F.col("transaction_date").cast("string")
    )

    (
        df_to_load.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")                       # Cassandra: append = upsert
        .options(
            table=SCYLLA_TABLE,
            keyspace=SCYLLA_KEYSPACE,
        )
        .save()
    )
    log.info(f"Successfully loaded {df.count():,} rows into ScyllaDB ✅")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    start_time = datetime.now()
    log.info("=" * 60)
    log.info("  Customer Transactions ETL Job — START")
    log.info("=" * 60)

    spark = create_spark_session()

    try:
        # Step 1: Extract
        raw_df = extract(spark)

        # Step 2 & 3: Transform + clean
        agg_df = transform(raw_df)

        # Step 4: Data quality gate
        if not data_quality_check(agg_df):
            log.error("Data quality check FAILED — aborting load.")
            sys.exit(1)

        # Step 5: Load
        load_to_scylladb(agg_df)

        elapsed = (datetime.now() - start_time).total_seconds()
        log.info(f"\n🎉  ETL Job completed successfully in {elapsed:.1f}s")

    except Exception as e:
        log.error(f"ETL Job FAILED: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
