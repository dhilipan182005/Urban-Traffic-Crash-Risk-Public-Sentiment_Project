CREATE DATABASE IF NOT EXISTS data_pipeline;

USE data_pipeline;

-- SILVER TABLES

CREATE TABLE IF NOT EXISTS youtube_data (
    video_id VARCHAR(255) PRIMARY KEY,
    title TEXT,
    views BIGINT,
    likes BIGINT,
    comments BIGINT,
    engagement_score DOUBLE,
    is_viral BOOLEAN
);

CREATE TABLE IF NOT EXISTS chicago_data (
    crash_record_id VARCHAR(255) PRIMARY KEY,
    crash_date TIMESTAMP,
    injuries_total INT,
    year INT,
    month INT,
    severity VARCHAR(50)
);

-- GOLD TABLES — Chicago Crash Analytics

CREATE TABLE IF NOT EXISTS gold_crash_severity_summary (
    year INT,
    month INT,
    severity VARCHAR(50),
    total_crashes BIGINT,
    avg_injuries DOUBLE
);

CREATE TABLE IF NOT EXISTS gold_crash_time_analysis (
    crash_hour VARCHAR(10),
    crash_day_of_week VARCHAR(10),
    total_crashes BIGINT,
    avg_injuries DOUBLE,
    total_fatalities BIGINT
);

CREATE TABLE IF NOT EXISTS gold_crash_hotspots (
    street_name VARCHAR(255),
    total_crashes BIGINT,
    avg_injuries DOUBLE,
    total_fatalities BIGINT
);

CREATE TABLE IF NOT EXISTS gold_crash_causes (
    prim_contributory_cause VARCHAR(255),
    total_crashes BIGINT,
    avg_injuries DOUBLE,
    total_fatalities BIGINT
);

-- GOLD TABLES — YouTube Analytics

CREATE TABLE IF NOT EXISTS gold_channel_summary (
    channel VARCHAR(255),
    total_videos BIGINT,
    total_views BIGINT,
    total_likes BIGINT,
    total_comments BIGINT,
    avg_engagement_score DOUBLE
);

CREATE TABLE IF NOT EXISTS gold_viral_content (
    video_id VARCHAR(255),
    title TEXT,
    channel VARCHAR(255),
    views BIGINT,
    likes BIGINT,
    comments BIGINT,
    engagement_score DOUBLE,
    is_viral BOOLEAN
);

CREATE TABLE IF NOT EXISTS gold_publishing_trends (
    publish_date DATE,
    videos_published BIGINT,
    total_views BIGINT,
    avg_engagement DOUBLE
);
