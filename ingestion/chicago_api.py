import os
import json
import time
from datetime import datetime
from config.config import *
from ingestion.api_client import fetch_api_data
from utils.utils import log_info, log_error

os.makedirs(BRONZE_CHICAGO, exist_ok=True)

def get_last_offset():
    if not os.path.exists(CHECKPOINT_FILE):
        return 0
    with open(CHECKPOINT_FILE, "r") as f:
        return int(f.read().strip())

def save_checkpoint(offset):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(offset))

def save_json(data, batch_id):
    path = f"{BRONZE_CHICAGO}/batch_{batch_id}.json"
    with open(path, "w") as f:
        json.dump(data, f)
    log_info(f"Saved batch {batch_id}")

def ingest():

    offset = get_last_offset()
    batch_id = offset // LIMIT

    while True:

        log_info(f"Fetching offset {offset}")

        data = fetch_api_data(offset)

        if not data:
            log_info("No more data")
            break

        # metadata
        enriched = {
            "data": data,
            "metadata": {
                "source": "chicago_api",
                "ingestion_time": str(datetime.now()),
                "batch_id": batch_id
            }
        }

        save_json(enriched, batch_id)

        offset += LIMIT
        batch_id += 1
        save_checkpoint(offset)

        time.sleep(SLEEP_TIME)

if __name__ == "__main__":
    ingest()