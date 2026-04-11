-- ===========================================================================
-- HIVE OPTIMIZED DDLs (NON-DESTRUCTIVE)
-- These tables create an optimized layer pointing to the existing paths
-- and use efficient Parquet + Snappy storage features.
-- ===========================================================================

CREATE DATABASE IF NOT EXISTS data_pipeline;
USE data_pipeline;

-- 1. SILVER LAYER: CHICAGO DATA
-- Partitioned logically to map to the 'year' and 'month' Spark partitions.
CREATE EXTERNAL TABLE IF NOT EXISTS silver_chicago_optimized (
    crash_record_id STRING,
    crash_date TIMESTAMP,
    crash_hour INT,
    crash_day_of_week INT,
    severity STRING,
    injuries_total INT,
    injuries_fatal INT,
    street_name STRING,
    prim_contributory_cause STRING,
    posted_speed_limit INT,
    weather_condition STRING,
    lighting_condition STRING,
    roadway_surface_cond STRING
)
PARTITIONED BY (`year` INT, `month` INT)
STORED AS PARQUET
LOCATION '/user/hadoop/data/silver/chicago'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
);

-- Note: To make partitions visible after Spark writes them to disk, run:
-- MSCK REPAIR TABLE silver_chicago_optimized;


-- 2. SILVER LAYER: YOUTUBE DATA
-- Non-partitioned simple mapping (as currently processed). 
CREATE EXTERNAL TABLE IF NOT EXISTS silver_youtube_optimized (
    video_id STRING,
    title STRING,
    channel STRING,
    published_at TIMESTAMP,
    views BIGINT,
    likes BIGINT,
    comments BIGINT,
    engagement_score DOUBLE,
    is_viral BOOLEAN
)
STORED AS PARQUET
LOCATION '/user/hadoop/data/silver/youtube'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
);


-- 3. GOLD LAYER: CRASH SEVERITY SUMMARY
-- Using managed tables if you prefer Hive to own the lifecycle,
-- but sticking to EXTERNAL for safety and decoupling.
CREATE EXTERNAL TABLE IF NOT EXISTS gold_crash_severity_optimized (
    `year` INT,
    `month` INT,
    severity STRING,
    total_crashes BIGINT,
    avg_injuries DOUBLE
)
STORED AS PARQUET
LOCATION '/user/hadoop/data/gold/chicago/crash_severity_summary'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
);


-- 4. GOLD LAYER: VIRAL YOUTUBE CONTENT
CREATE EXTERNAL TABLE IF NOT EXISTS gold_youtube_viral_optimized (
    video_id STRING,
    title STRING,
    channel STRING,
    views BIGINT,
    likes BIGINT,
    comments BIGINT,
    engagement_score DOUBLE,
    is_viral BOOLEAN
)
STORED AS PARQUET
LOCATION '/user/hadoop/data/gold/youtube/viral_content'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
);
