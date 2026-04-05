"""
pipeline_dag.py
───────────────
Airflow DAG that orchestrates the full ETL pipeline:
  Task 1 (extract_delta)    → reads Delta Lake, validates row count
  Task 2 (transform_spark)  → runs Spark ETL job via spark-submit
  Task 3 (load_scylladb)    → verifies data landed in ScyllaDB

Schedule: daily at 02:00 UTC
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# ── DAG default arguments ────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,       # set to True + add email for alerts
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# ── Config ───────────────────────────────────────────────────────────────────
DELTA_PATH      = "/opt/airflow/delta-lake/customer_transactions"
SPARK_MASTER    = "spark://spark-master:7077"
SPARK_JOB_PATH  = "/opt/airflow/spark_jobs/etl_job.py"
SCYLLA_HOST     = "scylladb"
SCYLLA_KEYSPACE = "transactions_ks"
SCYLLA_TABLE    = "daily_customer_totals"
MIN_ROW_COUNT   = 100               # DQ gate: pipeline fails if fewer rows than this

# ── Spark submit command ──────────────────────────────────────────────────────
SPARK_SUBMIT_CMD = f"""
spark-submit \
  --master {SPARK_MASTER} \
  --deploy-mode client \
  --packages io.delta:delta-spark_2.12:3.1.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.cassandra.connection.host={SCYLLA_HOST} \
  --conf spark.cassandra.connection.port=9042 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  {SPARK_JOB_PATH}
"""


# ─────────────────────────────────────────────────────────────────────────────
# Python callables
# ─────────────────────────────────────────────────────────────────────────────

def check_delta_table(**context) -> str:
    """
    Data quality gate: verify the Delta table exists and has enough rows.
    Returns the id of the next task to run (branch operator).
    """
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    log.info(f"Checking Delta table at {DELTA_PATH} ...")
    builder = (
        SparkSession.builder
        .appName("AirflowDQCheck")
        .master(SPARK_MASTER)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.format("delta").load(DELTA_PATH)
        row_count = df.count()
        log.info(f"Delta table has {row_count:,} rows")

        # Push row count to XCom so downstream tasks can use it
        context["ti"].xcom_push(key="raw_row_count", value=row_count)

        if row_count < MIN_ROW_COUNT:
            log.warning(f"Row count {row_count} below threshold {MIN_ROW_COUNT} — skipping pipeline")
            return "dq_fail_alert"

        return "transform_and_load"

    finally:
        spark.stop()


def dq_fail_alert(**context):
    """Called when DQ check fails."""
    raw_count = context["ti"].xcom_pull(key="raw_row_count")
    msg = (
        f"Pipeline aborted: Delta table row count ({raw_count}) "
        f"is below minimum threshold ({MIN_ROW_COUNT})."
    )
    log.error(msg)
    raise ValueError(msg)


def verify_scylladb_load(**context):
    """Spot-check that ScyllaDB received data after the Spark job."""
    from cassandra.cluster import Cluster
    from cassandra.policies import DCAwareRoundRobinPolicy

    log.info(f"Verifying ScyllaDB load ({SCYLLA_KEYSPACE}.{SCYLLA_TABLE})...")

    cluster = Cluster(
        [SCYLLA_HOST],
        port=9042,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
    )
    session = cluster.connect(SCYLLA_KEYSPACE)

    try:
        row = session.execute(
            f"SELECT COUNT(*) FROM {SCYLLA_TABLE} LIMIT 1"
        ).one()
        count = row.count
        log.info(f"ScyllaDB row count: {count:,}")

        if count == 0:
            raise ValueError("ScyllaDB table is empty after Spark job — load may have failed!")

        context["ti"].xcom_push(key="scylla_row_count", value=count)
        log.info(f"ScyllaDB verification passed ✅ ({count:,} rows)")

    finally:
        cluster.shutdown()


# ─────────────────────────────────────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="customer_transactions_etl",
    description="Daily ETL: Delta Lake → Spark → ScyllaDB",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",        # 02:00 UTC daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "delta-lake", "spark", "scylladb"],
    doc_md="""
## Customer Transactions ETL Pipeline

**Flow:**
```
check_delta_dq → [dq_fail_alert | transform_and_load] → verify_scylladb → pipeline_success
```

**Tasks:**
1. `check_delta_dq` — validates Delta Lake table exists and has ≥ 100 rows
2. `transform_and_load` — spark-submit runs full ETL (dedup, filter, aggregate, load)
3. `verify_scylladb` — confirms rows landed in ScyllaDB
4. `pipeline_success` — final marker task
    """,
) as dag:

    # ── Start ──────────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Task 1: DQ check on Delta table (branch) ───────────────────────────
    check_delta_dq = BranchPythonOperator(
        task_id="check_delta_dq",
        python_callable=check_delta_table,
    )

    # ── Branch: DQ failure path ────────────────────────────────────────────
    dq_fail = PythonOperator(
        task_id="dq_fail_alert",
        python_callable=dq_fail_alert,
    )

    # ── Task 2: Spark ETL (extract + transform + load) ─────────────────────
    transform_and_load = BashOperator(
        task_id="transform_and_load",
        bash_command=SPARK_SUBMIT_CMD,
        env={
            "SPARK_HOME": "/opt/bitnami/spark",
            "PATH": "/opt/bitnami/spark/bin:/usr/local/bin:/usr/bin:/bin",
        },
    )

    # ── Task 3: Verify ScyllaDB ────────────────────────────────────────────
    verify_scylladb = PythonOperator(
        task_id="verify_scylladb",
        python_callable=verify_scylladb_load,
    )

    # ── End ────────────────────────────────────────────────────────────────
    pipeline_success = EmptyOperator(
        task_id="pipeline_success",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ── Task dependencies ──────────────────────────────────────────────────
    start >> check_delta_dq
    check_delta_dq >> [dq_fail, transform_and_load]
    transform_and_load >> verify_scylladb >> pipeline_success
    dq_fail >> pipeline_success
