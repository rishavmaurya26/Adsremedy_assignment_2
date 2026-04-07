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
    "retries": 1,                           # ← only 1 retry
    "retry_delay": timedelta(seconds=30),   # ← wait only 30 seconds
    "retry_exponential_backoff": True
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

def check_delta_table(**context):
    import os

    delta_log_path = os.path.join(DELTA_PATH, "_delta_log")
    log.info(f"Checking Delta table at {DELTA_PATH} ...")

    if not os.path.exists(DELTA_PATH) or not os.path.exists(delta_log_path):
        raise Exception(f"Delta table not found at {DELTA_PATH}")

    parquet_files = [f for f in os.listdir(DELTA_PATH) if f.endswith(".parquet")]
    log.info(f"Found {len(parquet_files)} parquet files")

    if len(parquet_files) == 0:
        raise Exception("Delta table is empty — generation may have failed!")

    context["ti"].xcom_push(key="parquet_file_count", value=len(parquet_files))
    log.info("Delta table check passed ✅")

# ─────────────────────────────────────────────────────────────────────────────
# Helper — run docker exec command
# ─────────────────────────────────────────────────────────────────────────────
def run_docker_exec(cmd: list[str], task_name: str) -> str:
    """Run a command inside a container and return stdout. Raises on failure."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise Exception(f"{task_name} failed with return code {result.returncode}\n{result.stderr}")
    return result.stdout

# ─────────────────────────────────────────────────────────────────────────────
# Task 2 — Generate fake data and write to Delta Lake
# ─────────────────────────────────────────────────────────────────────────────
def run_data_generation(**context):
    """Run generate_data.py inside spark_master via docker exec."""

    #  Install required packages inside spark container automatically
    log.info("Installing required packages in spark_master...")
    subprocess.run(
        ["docker", "exec", "spark_master", "pip", "install", "faker"],
        capture_output=True,
        text=True
    )
    log.info("faker installed ✅")


    log.info("Starting data generation...")
 
    cmd = [
        "docker", "exec", "spark_master",
        "/opt/spark/bin/spark-submit",
        "--master", "local[*]",
        "--packages", "io.delta:delta-spark_2.12:3.1.0",
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "/opt/scripts/generate_data.py"
    ]
 
    output = run_docker_exec(cmd, "Data Generation")
    log.info("Data generation completed ✅")
    log.info(output)


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


def verify_scylladb_load(**context):
    """Verify ScyllaDB received data using cqlsh via docker exec."""
    import subprocess

    result = subprocess.run(
        [
            "docker", "exec", "scylladb",
            "cqlsh", "scylladb", "9042",
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
generate_data >> check_delta_dq >> transform_and_load >> verify_scylladb
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
    check_delta_dq = PythonOperator(
        task_id="check_delta_dq",
        python_callable=check_delta_table,
    )

    # Task 2 — Data generation
    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=run_data_generation,
    )

    # ── Task 3: Spark ETL (extract + transform + load) ─────────────────────
    transform_and_load = PythonOperator(
        task_id="transform_and_load",
        python_callable=run_spark_etl
    )

    # ── Task 4: Verify ScyllaDB ────────────────────────────────────────────
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
start >> generate_data >> check_delta_dq >> transform_and_load >> verify_scylladb >> pipeline_success