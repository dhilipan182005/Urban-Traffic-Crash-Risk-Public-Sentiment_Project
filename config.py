import os

# -------- CHICAGO --------
CHICAGO_API_URL = "https://data.cityofchicago.org/resource/85ca-t3if.json"
LIMIT = 1000
SLEEP_TIME = 1
MAX_RETRY = 5

BRONZE_CHICAGO_PATH = "bronze/chicago_api/"
CHECKPOINT_FILE = "ingestion/chicago/checkpoint.txt"

# -------- YOUTUBE --------
YOUTUBE_API_KEY = "YOUR_API_KEY"
YOUTUBE_BASE_URL = "https://www.googleapis.com/youtube/v3"
BRONZE_YOUTUBE_PATH = "bronze/youtube/"