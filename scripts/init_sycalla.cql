-- ─────────────────────────────────────────────────────────────────────────
-- init_scylla.cql
-- ScyllaDB schema initialization for the Data Engineering Assignment
-- ─────────────────────────────────────────────────────────────────────────

-- Create keyspace with SimpleStrategy (single-node local setup)
CREATE KEYSPACE IF NOT EXISTS transactions_ks
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE transactions_ks;

-- ─────────────────────────────────────────────────────────────────────────
-- Table: daily_customer_totals
--
-- Partition key : customer_id   → all rows for a customer go to same node
-- Clustering key: transaction_date → rows are ordered by date within partition
--
-- Query patterns supported efficiently:
--   SELECT * FROM daily_customer_totals WHERE customer_id = 'C00001';
--   SELECT * FROM daily_customer_totals WHERE customer_id = 'C00001'
--     AND transaction_date >= '2025-01-01';
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_customer_totals (
    customer_id      TEXT,
    transaction_date DATE,
    daily_total      FLOAT,
    updated_at       TIMESTAMP,
    PRIMARY KEY (customer_id, transaction_date)
) WITH CLUSTERING ORDER BY (transaction_date DESC)
  AND comment = 'Aggregated daily transaction totals per customer';

-- ─────────────────────────────────────────────────────────────────────────
-- Verify schema
-- ─────────────────────────────────────────────────────────────────────────
DESCRIBE TABLE transactions_ks.daily_customer_totals;
