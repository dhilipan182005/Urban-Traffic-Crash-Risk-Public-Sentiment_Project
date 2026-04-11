-- ===========================================================================
-- HIVE VALIDATION & VERIFICATION SCRIPT
-- Run this in Hive, Beeline, or Spark SQL to manually verify data quality.
-- ===========================================================================

USE data_pipeline;

-- 1. Refresh Partitions
-- Since Chicago data is partitioned by Year/Month, we must tell Hive to
-- scan the HDFS folders and load any new partitions into memory.
MSCK REPAIR TABLE silver_chicago_optimized;

-- 2. Chicago Row Count Check
-- Verifies the data loaded successfully (Should show 116,000+ rows)
SELECT 
    'Chicago Silver Data' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT crash_record_id) AS unique_crashes
FROM silver_chicago_optimized;

-- 3. Youtube Missing Data Check
-- Checks for critical nulls. We expect 0 null video_ids. 
SELECT 
    'YouTube Silver Data' AS table_name,
    COUNT(*) as total_videos,
    SUM(CASE WHEN video_id IS NULL THEN 1 ELSE 0 END) AS missing_video_ids,
    SUM(CASE WHEN engagement_score IS NULL THEN 1 ELSE 0 END) AS missing_scores
FROM silver_youtube_optimized;

-- 4. Chicago Freshness Check
-- Identifies the most recent month of data we ingested
SELECT 
    year, 
    month, 
    COUNT(*) as crashes_this_month 
FROM silver_chicago_optimized 
GROUP BY year, month 
ORDER BY year DESC, month DESC 
LIMIT 5;
