import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = "/workspace"
DATA_ROOT = "/workspace/data"

BRONZE_CHICAGO = "/workspace/data/bronze/chicago"
BRONZE_YOUTUBE = "/workspace/data/bronze/youtube"
SILVER_CHICAGO = "/workspace/data/silver/chicago"
SILVER_YOUTUBE = "/workspace/data/silver/youtube"
GOLD_CHICAGO = "/workspace/data/gold/chicago"
GOLD_YOUTUBE = "/workspace/data/gold/youtube"

HDFS_BRONZE_CHICAGO = "hdfs://namenode:9000/user/hadoop/bronze/chicago"
HDFS_BRONZE_YOUTUBE = "hdfs://namenode:9000/user/hadoop/bronze/youtube"
HDFS_SILVER_CHICAGO = "hdfs://namenode:9000/user/hadoop/silver/chicago"
HDFS_SILVER_YOUTUBE = "hdfs://namenode:9000/user/hadoop/silver/youtube"
HDFS_GOLD_CHICAGO = "hdfs://namenode:9000/user/hadoop/gold/chicago"
HDFS_GOLD_YOUTUBE = "hdfs://namenode:9000/user/hadoop/gold/youtube"




CHICAGO_API_URL = "https://data.cityofchicago.org/resource/85ca-t3if.json"
CHICAGO_API_TOKEN = os.getenv("API_TOKEN")

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
YOUTUBE_BASE_URL = "https://www.googleapis.com/youtube/v3"

LIMIT = 1000
MAX_RETRIES = 5
BACKOFF_FACTOR = 2
SLEEP_TIME = 5

STATE_FILE = "/workspace/metadata/state.json"
LOG_FILE = "/workspace/logs/pipeline.log"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"