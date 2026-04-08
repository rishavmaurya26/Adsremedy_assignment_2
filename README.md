# Data Engineering Assignment 2
## Delta Lake · Apache Spark · ScyllaDB · Apache Airflow

---

## Architecture

```
[Data Generation] ──► [Delta Lake (./data/delta-lake/)] ──► [Spark ETL Job]
                                                                    │
                              [Airflow DAG]                         ▼
                           (orchestrates all)              [ScyllaDB Table]
```

---



### Step 1 — Clone and create directory structure

```bash
git clone <your-repo-url>
cd de-assignment-2

# Create Delta Lake local directory (mounted into containers)
mkdir -p data/delta-lake/customer_transactions
mkdir -p logs plugins
```

### Step 2 — Start all services

```bash
docker compose up -d --build
```

Wait ~2 minutes for ScyllaDB and Airflow to fully start.

Check status:
```bash
docker compose ps
```

All services should show `healthy` or `running`.

### Step 3 — Initialize ScyllaDB schema

```bash
# Wait for ScyllaDB to be healthy first
docker compose exec scylladb cqlsh -f /init_scylla.cql
```

### Step 4 — Generate sample data and write to Delta Lake

```bash
docker compose exec spark-master bash -c "
  pip install faker delta-spark --quiet &&
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    /opt/spark/scripts/generate_data.py
"
```

### Step 5 — Trigger the Airflow DAG


Or trigger via CLI:
```bash
docker compose exec airflow-scheduler airflow dags trigger customer_transactions_etl
```

---

## Service URLs

| Service        | URL                        | Credentials       |
|----------------|----------------------------|-------------------|
| Airflow UI     | http://localhost:8081       | admin / admin     |
| Spark Master   | http://localhost:8080       | —                 |
| ScyllaDB CQL   | localhost:9042              | no auth (local)   |

---

## Verify Data in ScyllaDB

```bash
docker compose exec scylladb cqlsh

# Inside cqlsh:
USE transactions_ks;
SELECT * FROM daily_customer_totals LIMIT 10;
SELECT COUNT(*) FROM daily_customer_totals;
```

---

## Delta Lake Versioning

```bash
docker compose exec spark-master bash -c "
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --class org.apache.spark.sql.delta.DeltaHistory \
    /opt/spark/scripts/show_delta_history.py
"
```

Or interactively in PySpark:
```python
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "/opt/spark/delta-lake/customer_transactions")
dt.history().show(truncate=False)

# Time travel — read version 0 (original data)
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("/opt/spark/delta-lake/customer_transactions")
df_v0.count()
```

---

## Project Structure

```
de-assignment-2/
├── docker-compose.yml              # All services
├── README.md                       # This file
├── data/
│   └── delta-lake/                 # Mounted into Spark container
│       └── customer_transactions/  # Delta table files
├── scripts/
│   ├── generate_data.py            # Task 2 — data generation
│   └── init_scylla.cql             # Task 4 — ScyllaDB schema
├── spark_jobs/
│   └── etl_job.py                  # Task 3 — Spark ETL
├── dags/
│   └── pipeline_dag.py             # Task 5 — Airflow DAG
├── logs/                           # Airflow logs (auto-created)
└── plugins/                        # Airflow plugins (empty)
```

---

## Data Quality Rules Applied

| Rule              | Implementation                                      |
|-------------------|-----------------------------------------------------|
| Deduplication     | Window function on `transaction_id`, keep latest   |
| Invalid amounts   | Filter `amount <= 0` before aggregation             |
| Null check        | DQ gate in Airflow before running Spark job         |
| ScyllaDB verify   | Row count check after Spark load                   |

---



### Check all container logs
```bash
docker compose logs -f
```

---

## Stopping All Services

```bash
docker compose down           # Stop containers, keep volumes
docker compose down -v        # Stop containers AND delete all data
```

---

## Online Environment (GitHub Codespaces)

1. Push this repo to GitHub
2. Click **Code → Codespaces → Create codespace**
3. The Codespace gives you a 4-core/8GB cloud machine
4. Run `docker compose up -d` inside the terminal
5. Codespaces forwards ports automatically — click the link for each service

---

## Expected Output

After running the full pipeline, ScyllaDB will contain rows like:

```
 customer_id | transaction_date | daily_total
─────────────┼──────────────────┼─────────────
 C00001      | 2025-11-30       | 482.15
 C00001      | 2025-12-01       | 129.90
 C00042      | 2025-11-28       | 1205.00
```
