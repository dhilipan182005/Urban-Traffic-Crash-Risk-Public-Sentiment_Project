from dotenv import load_dotenv
import os

load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

if not YOUTUBE_API_KEY:
    raise ValueError("Missing API key")

# -------- CHICAGO --------
CHICAGO_API_URL = "https://data.cityofchicago.org/resource/85ca-t3if.json"
LIMIT = 1000
SLEEP_TIME = 1
MAX_RETRY = 5

BRONZE_CHICAGO_PATH = "bronze/chicago_api/"
CHECKPOINT_FILE = "ingestion/chicago/checkpoint.txt"

# -------- YOUTUBE --------

YOUTUBE_BASE_URL = "https://www.googleapis.com/youtube/v3"
BRONZE_YOUTUBE_PATH = "bronze/youtube/"