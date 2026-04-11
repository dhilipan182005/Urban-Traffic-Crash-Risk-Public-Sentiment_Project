# HDFS & Hive Optimization Strategy

This document outlines the recommendations and configuration changes to improve the performance, reliability, and storage footprint of the Chicago Traffic and YouTube Sentiment data pipelines.

---

## 1. HDFS Storage Optimization

### The Small File Problem
Hadoop processes data physically stored as files in HDFS. Having too many small files (such as frequent API ingestion creating small JSON dumps) leads to metadata overhead on the NameNode and inefficient MapReduce/Spark task parallelization.
**Recommendation:** Implement periodic "compaction" routines. 
- In Spark (used in your current `silver_chicago.py`), use `.coalesce(N)` before writing to parquets to enforce a specific number of larger partitions.
- For Hive tables, you can run `ALTER TABLE table_name PARTITION (part_col=X) CONCATENATE;` to compact ORC small files into larger ones.

### File Formats
Currently, Bronze ingest is JSON, and Silver PySpark outputs are Parquet. 
**Recommendation:** Ensure all Hive tables built on top of Silver/Gold data use **Parquet with Snappy compression**. Parquet's columnar nature limits I/O by only reading accessed columns, making Hive queries highly performant. Snappy provides a great balance of speed and compression ratio.

---

## 2. Hive Partitioning Strategy

Partitioning helps Hive prune directories it scans during query execution.
- **Chicago Data:** The Silver Chicago data currently saves with `partitionBy("year", "month")`. This is ideal. The Hive external tables should map directly to this scheme. Queries filtering by recent months will inherently be faster.
- **YouTube Data:** The YouTube data currently lacks a explicit temporal partition. It is recommended to partition the data by logical ingestion date or `publish_date` (e.g., `YYYY-MM-DD`) derived from the `published_at` field to speed up temporal queries.

---

## 3. Hive Performance Configuration (Non-Intrusive)

Add the following configurations in your Hive session (or globally in `hive-site.xml`) to drastically speed up query latencies without altering any business logic:

```sql
-- 1. Enable Tez Execution Engine (Replaces MapReduce for faster DAG resolution)
SET hive.execution.engine=tez;

-- 2. Enable Vectorized Query Execution (Processes data in batches of 1024 rows instead of row-by-row)
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;

-- 3. Enable Cost-Based Optimizer (CBO) (Uses statistics to form better query plans)
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;

-- 4. Enable Dynamic Partition Pruning
SET hive.optimize.ppd=true;
SET hive.optimize.ppd.storage=true;
```

---

## 4. Next Steps
- Execute the SQL file provided at `infrastructure/hive_optimized_ddl.sql` to lay down the new external tables over your existing data paths.
- Deploy the validation DAG `validation_dag.py` in Airflow to monitor data quality.
