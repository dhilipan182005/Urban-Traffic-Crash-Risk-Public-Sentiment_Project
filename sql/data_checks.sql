-- ============================================================
--  MySQL DATA CHECKING QUERIES
--  Database : data_pipeline
--  Host     : localhost   Port: 3307
--  User     : root        Password: 182005kavi
--  Tables   : chicago_risk_gold, chicago_monthly_trends,
--             youtube_channel_gold, youtube_top_videos
-- ============================================================
-- Connect first:
--   mysql -h 127.0.0.1 -P 3307 -u root -p182005kavi data_pipeline
-- ============================================================

USE data_pipeline;


-- ------------------------------------------------------------
-- SECTION 1: ROW COUNT — All Tables
-- ------------------------------------------------------------

SELECT 'chicago_risk_gold'     AS table_name, COUNT(*) AS total_rows FROM chicago_risk_gold
UNION ALL
SELECT 'chicago_monthly_trends',               COUNT(*)              FROM chicago_monthly_trends
UNION ALL
SELECT 'youtube_channel_gold',                 COUNT(*)              FROM youtube_channel_gold
UNION ALL
SELECT 'youtube_top_videos',                   COUNT(*)              FROM youtube_top_videos;


-- ------------------------------------------------------------
-- SECTION 2: NULL CHECKS
-- ------------------------------------------------------------

-- 2.1 Chicago Risk Gold — null check
SELECT
    COUNT(*)                                                    AS total_rows,
    SUM(CASE WHEN location      IS NULL THEN 1 ELSE 0 END)      AS null_location,
    SUM(CASE WHEN time_of_day   IS NULL THEN 1 ELSE 0 END)      AS null_time_of_day,
    SUM(CASE WHEN severity      IS NULL THEN 1 ELSE 0 END)      AS null_severity,
    SUM(CASE WHEN total_crashes IS NULL THEN 1 ELSE 0 END)      AS null_total_crashes,
    SUM(CASE WHEN total_injuries IS NULL THEN 1 ELSE 0 END)     AS null_total_injuries
FROM chicago_risk_gold;

-- 2.2 Chicago Monthly Trends — null check
SELECT
    COUNT(*)                                                    AS total_rows,
    SUM(CASE WHEN year          IS NULL THEN 1 ELSE 0 END)      AS null_year,
    SUM(CASE WHEN month         IS NULL THEN 1 ELSE 0 END)      AS null_month,
    SUM(CASE WHEN total_crashes IS NULL THEN 1 ELSE 0 END)      AS null_total_crashes,
    SUM(CASE WHEN total_injuries IS NULL THEN 1 ELSE 0 END)     AS null_total_injuries
FROM chicago_monthly_trends;

-- 2.3 YouTube Channel Gold — null check
SELECT
    COUNT(*)                                                    AS total_rows,
    SUM(CASE WHEN channel           IS NULL THEN 1 ELSE 0 END)  AS null_channel,
    SUM(CASE WHEN total_views       IS NULL THEN 1 ELSE 0 END)  AS null_total_views,
    SUM(CASE WHEN avg_engagement    IS NULL THEN 1 ELSE 0 END)  AS null_avg_engagement,
    SUM(CASE WHEN viral_video_count IS NULL THEN 1 ELSE 0 END)  AS null_viral_count,
    SUM(CASE WHEN video_count       IS NULL THEN 1 ELSE 0 END)  AS null_video_count
FROM youtube_channel_gold;

-- 2.4 YouTube Top Videos — null check
SELECT
    COUNT(*)                                                    AS total_rows,
    SUM(CASE WHEN video_id        IS NULL THEN 1 ELSE 0 END)    AS null_video_id,
    SUM(CASE WHEN title           IS NULL THEN 1 ELSE 0 END)    AS null_title,
    SUM(CASE WHEN engagement_score IS NULL THEN 1 ELSE 0 END)   AS null_engagement,
    SUM(CASE WHEN is_viral        IS NULL THEN 1 ELSE 0 END)    AS null_is_viral
FROM youtube_top_videos;


-- ------------------------------------------------------------
-- SECTION 3: CHICAGO CRASH ANALYSIS
-- ------------------------------------------------------------

-- 3.1 Overall Chicago summary
SELECT
    SUM(total_crashes)             AS grand_total_crashes,
    SUM(total_injuries)            AS grand_total_injuries,
    ROUND(AVG(total_injuries), 2)  AS avg_injuries_per_group,
    MAX(total_crashes)             AS max_crashes_in_one_group,
    MIN(total_crashes)             AS min_crashes_in_one_group
FROM chicago_risk_gold;

-- 3.2 Crashes by severity
SELECT
    severity,
    COUNT(*)            AS location_groups,
    SUM(total_crashes)  AS total_crashes,
    SUM(total_injuries) AS total_injuries,
    ROUND(AVG(total_crashes), 1) AS avg_crashes_per_group
FROM chicago_risk_gold
GROUP BY severity
ORDER BY total_crashes DESC;

-- 3.3 Crashes by time of day (DAY vs NIGHT)
SELECT
    time_of_day,
    SUM(total_crashes)  AS total_crashes,
    SUM(total_injuries) AS total_injuries
FROM chicago_risk_gold
GROUP BY time_of_day
ORDER BY total_crashes DESC;

-- 3.4 Monthly crash trend (ALL months)
SELECT
    year,
    month,
    total_crashes,
    total_injuries,
    ROUND(total_injuries / total_crashes, 2) AS injury_rate
FROM chicago_monthly_trends
ORDER BY year DESC, month DESC;

-- 3.5 *** LAST 2 MONTHS — Total crashes & injuries ***
SELECT
    SUM(total_crashes)             AS total_cases_last_2_months,
    SUM(total_injuries)            AS total_injuries_last_2_months,
    ROUND(SUM(total_injuries) / SUM(total_crashes), 2) AS injury_rate
FROM chicago_monthly_trends
WHERE
    (year = YEAR(CURDATE()) AND month >= MONTH(CURDATE()) - 1)
    OR (year = YEAR(CURDATE()) - 1 AND month = 12 AND MONTH(CURDATE()) = 1);

-- 3.6 Last 2 months — breakdown by month
SELECT
    year,
    month,
    total_crashes,
    total_injuries
FROM chicago_monthly_trends
WHERE
    (year = YEAR(CURDATE()) AND month >= MONTH(CURDATE()) - 1)
    OR (year = YEAR(CURDATE()) - 1 AND month = 12 AND MONTH(CURDATE()) = 1)
ORDER BY year DESC, month DESC;

-- 3.7 Top 10 most dangerous locations
SELECT
    location,
    time_of_day,
    severity,
    total_crashes,
    total_injuries
FROM chicago_risk_gold
ORDER BY total_crashes DESC
LIMIT 10;

-- 3.8 Top 10 deadliest locations (by injuries)
SELECT
    location,
    severity,
    total_crashes,
    total_injuries,
    ROUND(total_injuries / total_crashes, 2) AS injury_rate
FROM chicago_risk_gold
ORDER BY total_injuries DESC
LIMIT 10;

-- 3.9 HIGH severity crash hotspots
SELECT
    location,
    time_of_day,
    total_crashes,
    total_injuries
FROM chicago_risk_gold
WHERE severity = 'HIGH'
ORDER BY total_injuries DESC
LIMIT 10;

-- 3.10 Year-over-year crash comparison
SELECT
    year,
    SUM(total_crashes)  AS yearly_crashes,
    SUM(total_injuries) AS yearly_injuries
FROM chicago_monthly_trends
GROUP BY year
ORDER BY year DESC;


-- ------------------------------------------------------------
-- SECTION 4: YOUTUBE ANALYSIS
-- ------------------------------------------------------------

-- 4.1 YouTube overall summary
SELECT
    SUM(video_count)               AS total_videos,
    SUM(total_views)               AS total_views,
    ROUND(AVG(avg_engagement), 4)  AS avg_engagement_score,
    SUM(viral_video_count)         AS total_viral_videos
FROM youtube_channel_gold;

-- 4.2 Top 10 channels by total views
SELECT
    channel,
    total_views,
    video_count,
    viral_video_count,
    ROUND(avg_engagement, 4) AS avg_engagement
FROM youtube_channel_gold
ORDER BY total_views DESC
LIMIT 10;

-- 4.3 Top 10 most engaging channels
SELECT
    channel,
    ROUND(avg_engagement, 4) AS avg_engagement,
    video_count,
    viral_video_count
FROM youtube_channel_gold
ORDER BY avg_engagement DESC
LIMIT 10;

-- 4.4 Channels with most viral videos
SELECT
    channel,
    viral_video_count,
    video_count,
    total_views,
    ROUND((viral_video_count / video_count) * 100, 1) AS viral_pct
FROM youtube_channel_gold
WHERE viral_video_count > 0
ORDER BY viral_video_count DESC
LIMIT 10;

-- 4.5 Top 10 viral videos
SELECT
    video_id,
    title,
    ROUND(engagement_score, 4) AS engagement_score,
    is_viral
FROM youtube_top_videos
WHERE is_viral = 1
ORDER BY engagement_score DESC
LIMIT 10;

-- 4.6 Top 10 most engaging videos (all)
SELECT
    video_id,
    title,
    ROUND(engagement_score, 4) AS engagement_score,
    is_viral
FROM youtube_top_videos
ORDER BY engagement_score DESC
LIMIT 10;

-- 4.7 Viral vs non-viral count
SELECT
    is_viral,
    COUNT(*)                       AS video_count,
    ROUND(AVG(engagement_score), 4) AS avg_engagement
FROM youtube_top_videos
GROUP BY is_viral;


-- ------------------------------------------------------------
-- SECTION 5: DATA QUALITY SANITY CHECKS
-- ------------------------------------------------------------

-- 5.1 Any zero-crash rows? (should be 0)
SELECT COUNT(*) AS zero_crash_rows
FROM chicago_risk_gold
WHERE total_crashes = 0;

-- 5.2 Any negative injuries? (should be 0)
SELECT COUNT(*) AS negative_injury_rows
FROM chicago_risk_gold
WHERE total_injuries < 0;

-- 5.3 Any future year in monthly trends? (should be 0)
SELECT COUNT(*) AS future_year_rows
FROM chicago_monthly_trends
WHERE year > YEAR(CURDATE());

-- 5.4 Any invalid months (not 1-12)?
SELECT COUNT(*) AS invalid_month_rows
FROM chicago_monthly_trends
WHERE month < 1 OR month > 12;

-- 5.5 Duplicate channel names in gold? (should be 0)
SELECT channel, COUNT(*) AS cnt
FROM youtube_channel_gold
GROUP BY channel
HAVING COUNT(*) > 1;

-- 5.6 Videos with zero engagement but marked viral? (data anomaly)
SELECT COUNT(*) AS anomaly_rows
FROM youtube_top_videos
WHERE engagement_score = 0 AND is_viral = 1;


-- ------------------------------------------------------------
-- SECTION 6: QUICK SUMMARY DASHBOARD
-- ------------------------------------------------------------

SELECT
    'Total Crash Groups'                            AS metric,
    CAST(COUNT(*) AS CHAR)                          AS value
FROM chicago_risk_gold
UNION ALL
SELECT 'Grand Total Crashes',    CAST(SUM(total_crashes)  AS CHAR) FROM chicago_risk_gold
UNION ALL
SELECT 'Grand Total Injuries',   CAST(SUM(total_injuries) AS CHAR) FROM chicago_risk_gold
UNION ALL
SELECT 'Total Monthly Records',  CAST(COUNT(*) AS CHAR)            FROM chicago_monthly_trends
UNION ALL
SELECT 'Total YouTube Channels', CAST(COUNT(*) AS CHAR)            FROM youtube_channel_gold
UNION ALL
SELECT 'Total YouTube Videos',   CAST(SUM(video_count) AS CHAR)    FROM youtube_channel_gold
UNION ALL
SELECT 'Total View Count',       CAST(SUM(total_views) AS CHAR)    FROM youtube_channel_gold
UNION ALL
SELECT 'Total Viral Videos',     CAST(SUM(viral_video_count) AS CHAR) FROM youtube_channel_gold;
