import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base Path Configuration
# If running in Docker, use /workspace, otherwise use current directory
BASE_DIR = os.getenv("BASE_DIR", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_ROOT = os.path.join(BASE_DIR, "data")

# Layer Paths
BRONZE_CHICAGO = os.path.join(DATA_ROOT, "bronze", "chicago")
BRONZE_YOUTUBE = os.path.join(DATA_ROOT, "bronze", "youtube")

SILVER_CHICAGO = os.path.join(DATA_ROOT, "silver", "chicago")
SILVER_YOUTUBE = os.path.join(DATA_ROOT, "silver", "youtube")

GOLD_CHICAGO = os.path.join(DATA_ROOT, "gold", "chicago")
GOLD_YOUTUBE = os.path.join(DATA_ROOT, "gold", "youtube")

# API Configuration
CHICAGO_API_URL = "https://data.cityofchicago.org/resource/85ca-t3if.json"
CHICAGO_API_TOKEN = os.getenv("API_TOKEN")

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
YOUTUBE_BASE_URL = "https://www.googleapis.com/youtube/v3"

# Ingestion Settings
LIMIT = 1000
MAX_RETRIES = 5
BACKOFF_FACTOR = 2
SLEEP_TIME = 5

# Metadata
STATE_FILE = os.path.join(BASE_DIR, "metadata", "state.json")
LOG_FILE = os.path.join(BASE_DIR, "logs", "app.log")