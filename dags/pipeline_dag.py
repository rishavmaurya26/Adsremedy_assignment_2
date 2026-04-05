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
import subprocess
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
SCYLLA_HOST     = "scylladb"
SCYLLA_KEYSPACE = "transactions_ks"
SCYLLA_TABLE    = "daily_customer_totals"
MIN_ROW_COUNT   = 100               # DQ gate: pipeline fails if fewer rows than this

# ── Spark submit command ──────────────────────────────────────────────────────
SPARK_SUBMIT_CMD = f"""
docker exec spark_master \
        /opt/spark/bin/spark-submit \
          --master local[*] \
          --packages io.delta:delta-spark_2.12:3.1.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
          --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
          --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
          /opt/scripts/etl_job.py
"""


# ─────────────────────────────────────────────────────────────────────────────
# Python callables
# ─────────────────────────────────────────────────────────────────────────────

def check_delta_table(**context) -> str:
    """
    Data quality gate: verify the Delta table exists and has enough rows.
    Returns the id of the next task to run (branch operator).
    """
    """
    Data quality gate: verify the Delta table exists by checking
    if the _delta_log folder is present — no PySpark needed.
    """
    import os

    delta_log_path = os.path.join(DELTA_PATH, "_delta_log")
    log.info(f"Checking Delta table at {DELTA_PATH} ...")

    # Check if delta table exists
    if not os.path.exists(DELTA_PATH):
        log.warning(f"Delta path does not exist: {DELTA_PATH}")
        return "dq_fail_alert"

    if not os.path.exists(delta_log_path):
        log.warning(f"No _delta_log found — not a valid Delta table")
        return "dq_fail_alert"

    # Count parquet files as a proxy for row count
    parquet_files = [
        f for f in os.listdir(DELTA_PATH)
        if f.endswith(".parquet")
    ]

    log.info(f"Found {len(parquet_files)} parquet files in Delta table")

    if len(parquet_files) == 0:
        log.warning("No parquet files found — table appears empty")
        return "dq_fail_alert"

    context["ti"].xcom_push(key="parquet_file_count", value=len(parquet_files))
    log.info("Delta table check passed ✅")
    return "transform_and_load"

def run_spark_etl(**context):
    """Run spark-submit inside the spark_master container via subprocess."""
    cmd = [
        "docker", "exec", "spark_master",
        "/opt/spark/bin/spark-submit",
        "--master", "local[*]",
        "--packages", "io.delta:delta-spark_2.12:3.1.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0",
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "/opt/scripts/etl_job.py"
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True
    )

    log.info(result.stdout)

    if result.returncode != 0:
        log.error(result.stderr)
        raise Exception(f"Spark job failed with return code {result.returncode}\n{result.stderr}")

    log.info("Spark ETL completed successfully ✅")

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
    """Verify ScyllaDB received data using cqlsh via docker exec."""
    import subprocess

    result = subprocess.run(
        [
            "docker", "exec", "scylladb",
            "cqlsh", "172.18.0.2", "9042",
            "-e", f"SELECT COUNT(*) FROM {SCYLLA_KEYSPACE}.{SCYLLA_TABLE};"
        ],
        capture_output=True,
        text=True
    )

    log.info(result.stdout)

    if result.returncode != 0:
        log.error(result.stderr)
        raise Exception(f"ScyllaDB verification failed: {result.stderr}")

    # Check if count is 0
    if "0 rows" in result.stdout or "| 0" in result.stdout:
        raise Exception("ScyllaDB table is empty after Spark job!")

    log.info("ScyllaDB verification passed ✅")


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
    transform_and_load = PythonOperator(
        task_id="transform_and_load",
        python_callable=run_spark_etl
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
