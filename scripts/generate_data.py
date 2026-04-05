"""
generate_data.py
────────────────
Generates 1000+ customer transaction records and writes them as a
Delta Lake table. Intentionally includes data quality issues (duplicates,
negative/zero amounts) to exercise the cleaning pipeline.

Usage (run inside the Spark container or locally with delta-spark installed):
    python scripts/generate_data.py
"""

import uuid
import random
from datetime import datetime, timedelta, timezone

from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from delta import configure_spark_with_delta_pip

# ── Spark session with Delta Lake support ───────────────────────────────────
builder = (
    SparkSession.builder
    .appName("DataGeneration")
    .master("spark://spark-master:7077")          # change to "local[*]" for local run
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.1.0,"
        "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

DELTA_PATH = "/opt/spark/delta-lake/customer_transactions"

# ── Merchants and customer pool ─────────────────────────────────────────────
fake = Faker()
MERCHANTS = [
    "STORE_23", "ONLINE_SHOP", "GAS_STATION", "SUPERMARKET",
    "RESTAURANT_A", "PHARMACY", "ELECTRONICS_HUB", "COFFEE_SHOP",
    "BOOKSTORE", "TRAVEL_AGENCY",
]
CUSTOMER_IDS = [f"C{str(i).zfill(5)}" for i in range(1, 201)]  # 200 unique customers


def random_timestamp(days_back: int = 30) -> str:
    """Return a random UTC timestamp within the last `days_back` days."""
    start = datetime.now(timezone.utc) - timedelta(days=days_back)
    random_seconds = random.randint(0, days_back * 86400)
    dt = start + timedelta(seconds=random_seconds)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_records(n: int = 1100) -> list[dict]:
    """
    Generate n records including:
      - ~5% duplicates  (same transaction_id reused)
      - ~3% zero amounts
      - ~3% negative amounts
    """
    records = []
    generated_ids = []

    for i in range(n):
        # Decide record type
        roll = random.random()

        if roll < 0.05 and generated_ids:
            # ── Duplicate: reuse a previous record exactly ───────────────
            records.append(random.choice(records))
            continue

        transaction_id = str(uuid.uuid4())
        customer_id = random.choice(CUSTOMER_IDS)
        merchant = random.choice(MERCHANTS)
        ts = random_timestamp(days_back=60)

        if roll < 0.08:
            # Zero or negative amount (data quality issue)
            amount = round(random.uniform(-200.0, 0.0), 2)
        else:
            # Normal positive amount
            amount = round(random.uniform(0.50, 2000.0), 2)

        record = {
            "transaction_id": transaction_id,
            "customer_id": customer_id,
            "amount": amount,
            "timestamp": ts,
            "merchant": merchant,
        }
        records.append(record)
        generated_ids.append(transaction_id)

    return records


# ── Schema ──────────────────────────────────────────────────────────────────
schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id",    StringType(), False),
    StructField("amount",         FloatType(),  False),
    StructField("timestamp",      StringType(), False),   # stored as string; cast in ETL
    StructField("merchant",       StringType(), True),
])


def main():
    print("🔄  Generating records...")
    records = generate_records(1100)
    print(f"✅  Generated {len(records)} records "
          f"({sum(1 for r in records if r['amount'] <= 0)} with amount ≤ 0, "
          f"including duplicates)")

    # Convert to Spark DataFrame
    df = spark.createDataFrame(records, schema=schema)

    print(f"📝  Writing Delta table to {DELTA_PATH} ...")
    (
        df.write
        .format("delta")
        .mode("overwrite")          # version 0 — first write
        .save(DELTA_PATH)
    )
    print("✅  Initial write complete (version 0)")

    # ── Second write to demonstrate versioning ───────────────────────────
    print("🔄  Appending 100 more records (creates version 1)...")
    extra_records = generate_records(100)
    df_extra = spark.createDataFrame(extra_records, schema=schema)
    df_extra.write.format("delta").mode("append").save(DELTA_PATH)
    print("✅  Append complete (version 1)")

    # ── Show Delta table history ─────────────────────────────────────────
    from delta.tables import DeltaTable
    dt = DeltaTable.forPath(spark, DELTA_PATH)
    print("\n📜  Delta Table History:")
    dt.history().select("version", "timestamp", "operation", "operationParameters").show(
        truncate=False
    )

    # ── Quick sanity check ───────────────────────────────────────────────
    total = spark.read.format("delta").load(DELTA_PATH).count()
    print(f"\n📊  Total records in Delta table: {total}")

    spark.stop()
    print("\n🎉  Data generation complete!")


if __name__ == "__main__":
    main()
