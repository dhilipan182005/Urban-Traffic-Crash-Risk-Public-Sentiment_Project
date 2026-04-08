BASE_PATH = "/workspace/data"

BRONZE_CHICAGO = f"{BASE_PATH}/bronze/chicago"
SILVER_CHICAGO = f"{BASE_PATH}/silver/chicago"
GOLD_PATH = f"{BASE_PATH}/gold"

import os
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
YOUTUBE_BASE_URL = "https://www.googleapis.com/youtube/v3"