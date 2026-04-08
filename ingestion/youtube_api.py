import os
import json
import requests
import time
from datetime import datetime

from config.config import YOUTUBE_API_KEY, YOUTUBE_BASE_URL
from utils.utils import log_info, log_error

BRONZE_YOUTUBE = "/workspace/data/bronze/youtube"
os.makedirs(BRONZE_YOUTUBE, exist_ok=True)


def get_video_details(video_id):

   def get_comments(video_id):

    url = f"{YOUTUBE_BASE_URL}/commentThreads"

    params = {
        "part": "snippet",
        "videoId": video_id,
        "maxResults": 50,
        "key": YOUTUBE_API_KEY
    }

    res = requests.get(url, params=params)

    if res.status_code == 200:
        return res.json()

    return None


def save_json(data, video_id):

    path = f"{BRONZE_YOUTUBE}/{video_id}.json"

    with open(path, "w") as f:
        json.dump(data, f)

    log_info(f"Saved {video_id}")


def ingest_youtube(video_ids):

    for vid in video_ids:

        log_info(f"Processing {vid}")

        data = get_video_details(vid)

        if data is None:
            continue

        # 🔥 Add metadata (IMPORTANT)
        data["metadata"] = {
            "source": "youtube_api",
            "video_id": vid,
            "ingestion_time": str(datetime.now())
        }

        save_json(data, vid)

        time.sleep(1)  # avoid rate limit


if __name__ == "__main__":

    video_ids = [
        "dQw4w9WgXcQ",
        "3JZ_D3ELwOQ"
    ]

    ingest_youtube(video_ids)