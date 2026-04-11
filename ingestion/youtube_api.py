import os
import json
import time
import yaml
from datetime import datetime, timezone
from config.config import (
    YOUTUBE_API_KEY, YOUTUBE_BASE_URL, BRONZE_YOUTUBE, 
    STATE_FILE, BASE_DIR, LIMIT
)
from utils.utils import log_info, log_error, log_debug
from utils.api_handler import request_with_retry

# Ensure directories exist
os.makedirs(BRONZE_YOUTUBE, exist_ok=True)
os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

# Load YAML config
CONFIG_YAML = os.path.join(BASE_DIR, "config", "config.yaml")
with open(CONFIG_YAML) as f:
    cfg_yaml = yaml.safe_load(f)

def load_state():
    """Load the timestamp of the last successful run."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception as e:
            log_error(f"Failed to load state file: {e}")
    return {"last_run": "2024-01-01T00:00:00Z"}

def save_state(ts):
    """Save the current timestamp as the last run time."""
    with open(STATE_FILE, "w") as f:
        json.dump({"last_run": ts}, f)

def ingest():
    """Search and ingest YouTube video metadata based on keywords."""
    state = load_state()
    last_run = state["last_run"]
    
    # Ensure last_run is in correct UTC format for YouTube
    if not last_run.endswith('Z'):
        last_run += 'Z'

    batch_timestamp = int(time.time())
    log_info(f"Starting YouTube Ingestion (since {last_run})...")

    for keyword in cfg_yaml.get("keywords", []):
        log_info(f"Processing keyword: {keyword}")
        next_page = None
        item_count = 0

        while True:
            params = {
                "part": "snippet",
                "q": keyword,
                "type": "video",
                "maxResults": cfg_yaml.get("max_results", 50),
                "key": YOUTUBE_API_KEY,
                "publishedAfter": last_run
            }

            if next_page:
                params["pageToken"] = next_page

            data = request_with_retry(f"{YOUTUBE_BASE_URL}/search", params=params)

            if not data or "items" not in data:
                log_info(f"No more data for keyword '{keyword}'.")
                break

            items = data["items"]
            if not items:
                break

            # --- Fetch video statistics ---
            video_ids = [item["id"]["videoId"] for item in items if "id" in item and "videoId" in item["id"]]
            
            if video_ids:
                videos_params = {
                    "part": "statistics",
                    "id": ",".join(video_ids),
                    "key": YOUTUBE_API_KEY
                }
                videos_data = request_with_retry(f"{YOUTUBE_BASE_URL}/videos", params=videos_params)
                
                if videos_data and "items" in videos_data:
                    videos_dict = {v["id"]: v for v in videos_data["items"]}
                    for item in items:
                        vid = item.get("id", {}).get("videoId")
                        if vid and vid in videos_dict:
                            item["statistics"] = videos_dict[vid].get("statistics", {})
            # ------------------------------

            output = {
                "data": items,
                "metadata": {
                    "batch_id": f"yt_{batch_timestamp}",
                    "keyword": keyword,
                    "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "source": "youtube_v3_search"
                }
            }

            file_name = f"yt_{batch_timestamp}_{keyword.replace(' ', '_')}.json"
            file_path = os.path.join(BRONZE_YOUTUBE, file_name)

            with open(file_path, "w") as f:
                json.dump(output, f)

            item_count += len(items)
            log_info(f"Saved {len(items)} records to {file_path}")

            next_page = data.get("nextPageToken")
            if not next_page:
                break

        log_info(f"Finished keyword '{keyword}': Total {item_count} records processed.")

    # Save current UTC time as last run
    new_state_ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    save_state(new_state_ts)
    log_info("YouTube Ingestion Finished.")

if __name__ == "__main__":
    if not YOUTUBE_API_KEY:
        log_error("YOUTUBE_API_KEY not found in environment variables.")
    else:
        ingest()