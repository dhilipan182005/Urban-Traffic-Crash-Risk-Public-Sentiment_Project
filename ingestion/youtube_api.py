import os
import sys
import json
import time
from datetime import datetime, timezone
import yaml
from config.config import (
    YOUTUBE_API_KEY, 
    YOUTUBE_BASE_URL, 
    TIMESTAMP_FORMAT,
    BRONZE_YOUTUBE as OUTPUT_PATH
)
from utils.utils import log_info, log_success, log_error, request_with_retry

os.makedirs(OUTPUT_PATH, exist_ok=True)

def get_video_details(video_id):
    url = f"{YOUTUBE_BASE_URL}/videos"
    params = {
        "part": "snippet,statistics",
        "id": video_id,
        "key": YOUTUBE_API_KEY
    }
    return request_with_retry(url, params=params)

def get_comments(video_id, max_results_cap=None):
    url = f"{YOUTUBE_BASE_URL}/commentThreads"
    params = {
        "part": "snippet",
        "videoId": video_id,
        "maxResults": 100,
        "key": YOUTUBE_API_KEY
    }
    
    all_comments = []
    next_page = None
    
    while True:
        if next_page:
            params["pageToken"] = next_page
            
        data = request_with_retry(url, params=params)
        if not data:
            break
            
        all_comments.extend(data.get("items", []))
        next_page = data.get("nextPageToken")
        
        if max_results_cap and len(all_comments) >= max_results_cap:
            all_comments = all_comments[:max_results_cap]
            break
            
        if not next_page:
            break
        time.sleep(1)
        
    return all_comments

def ingest():
    log_info("YouTube API Ingestion - Running")
    success_count = 0

    try:
        with open("/workspace/config/config.yaml") as f:
            cfg = yaml.safe_load(f)
    except Exception as e:
        log_error(f"Failed to load config.yaml: {e}")
        return

    video_ids = cfg.get("video_ids", [])
    max_results_cap = cfg.get("max_results", 100)
    
    for vid in video_ids:
        log_info(f"Processing details for: {vid}")
        
        details = get_video_details(vid)
        if not details or not details.get("items"):
            log_error(f"Failed to fetch details for: {vid}")
            continue
            
        comments = get_comments(vid, max_results_cap=max_results_cap)
        
        output = {
            "data": details["items"],
            "comments": comments,
            "metadata": {
                "video_id": vid,
                "timestamp": datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT),
                "source": "youtube_v3_api_details"
            }
        }

        file_name = f"{OUTPUT_PATH}/{vid}_details.json"
        with open(file_name, "w") as f:
            json.dump(output, f)

        log_info(f"Saved details and {len(comments)} comments to {file_name}")
        success_count += 1

    if success_count == 0:
        log_error("Failed to fetch any YouTube data. Check network or API key.")
        sys.exit(1)

    log_success("YouTube API Ingestion - Completed")

if __name__ == "__main__":
    try:
        ingest()
    except Exception as e:
        log_error(f"Critical failure in YouTube ingestion: {str(e)}")
        sys.exit(1)