import os
import sys
import json
import time
from datetime import datetime, timezone
from config.config import BRONZE_CHICAGO, CHICAGO_API_URL, LIMIT, TIMESTAMP_FORMAT, SLEEP_TIME, STATE_FILE
from utils.utils import log_info, log_success, log_error, request_with_retry

LOCK_FILE = "/workspace/metadata/chicago_ingestion.lock"

def acquire_lock():
    if os.path.exists(LOCK_FILE):
        log_error("Ingestion already running (lock file exists).")
        return False
    os.makedirs(os.path.dirname(LOCK_FILE), exist_ok=True)
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))
    return True

def release_lock():
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)

def get_last_offset():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
                return state.get("chicago_offset", 0)
        except Exception as e:
            log_error(f"Error reading state: {e}")
    return 0

def save_checkpoint(offset):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    state = {}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
        except:
            pass
    
    state["chicago_offset"] = offset
    state["last_run"] = datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT)
    
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=4)

def ingest():
    log_info("Chicago API Ingestion - Running")
    success_count = 0
    
    os.makedirs(BRONZE_CHICAGO, exist_ok=True)

    offset = get_last_offset()
    batch = offset // LIMIT

    log_info(f"Resuming Chicago ingestion from offset {offset} (Batch {batch})")

    while True:
        params = {
            "$limit": LIMIT,
            "$offset": offset
        }

        log_info(f"Fetching offset {offset} from Chicago API...")
        data = request_with_retry(CHICAGO_API_URL, params=params)

        if not data or len(data) == 0:
            log_info("Reached end of data stream or request failed.")
            break

        output = {
            "data": data,
            "metadata": {
                "batch": batch,
                "offset": offset,
                "timestamp": datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT),
                "source": "chicago_data_portal"
            }
        }

        file_path = f"{BRONZE_CHICAGO}/batch_{batch}.json"
        with open(file_path, "w") as f:
            json.dump(output, f)

        log_info(f"Successfully saved batch {batch} to {file_path} ({len(data)} records)")
        success_count += 1

        offset += LIMIT
        batch += 1
        save_checkpoint(offset)
        
        time.sleep(SLEEP_TIME)

    if success_count == 0 and offset == 0:
        log_error("Failed to fetch any Chicago data on initial run. Check network.")
        sys.exit(1)

    log_success("Chicago API Ingestion - Completed")

if __name__ == "__main__":
    if not acquire_lock():
        sys.exit(0)
    try:
        ingest()
    except Exception as e:
        log_error(f"Critical failure in Chicago ingestion: {str(e)}")
        sys.exit(1)
    finally:
        release_lock()