CREATE DATABASE IF NOT EXISTS traffic_db;
CREATE EXTERNAL TABLE IF NOT EXISTS traffic_db.chicago_bronze (
  json_data STRING
)
STORED AS TEXTFILE
LOCATION '/user/hadoop/bronze/chicago';


CREATE EXTERNAL TABLE IF NOT EXISTS traffic_db.youtube_bronze (
  json_data STRING
)
STORED AS TEXTFILE
LOCATION '/user/hadoop/bronze/youtube';

CREATE EXTERNAL TABLE IF NOT EXISTS traffic_db.chicago_silver (
  crash_record_id STRING,
  crash_date      STRING,
  location        STRING,
  severity        STRING,
  time_of_day     STRING,
  injuries_total  INT,
  year            INT,
  month           INT
)
STORED AS PARQUET
LOCATION '/user/hadoop/silver/chicago';

CREATE EXTERNAL TABLE IF NOT EXISTS traffic_db.youtube_silver (
  video_id         STRING,
  title            STRING,
  channel          STRING,
  views            BIGINT,
  likes            BIGINT,
  published_at     STRING,
  engagement_score DOUBLE,
  is_viral         BOOLEAN
)
STORED AS PARQUET
LOCATION '/user/hadoop/silver/youtube';

CREATE EXTERNAL TABLE IF NOT EXISTS traffic_db.chicago_risk_gold (
  location      STRING,
  time_of_day   STRING,
  severity      STRING,
  total_crashes BIGINT,
  total_injuries BIGINT
)
STORED AS PARQUET
LOCATION '/user/hadoop/gold/chicago/location_risk';

CREATE EXTERNAL TABLE IF NOT EXISTS traffic_db.youtube_channel_gold (
  channel           STRING,
  total_views       BIGINT,
  avg_engagement    DOUBLE,
  viral_video_count BIGINT,
  video_count       BIGINT
)
STORED AS PARQUET
LOCATION '/user/hadoop/gold/youtube/channel_stats';
