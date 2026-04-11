import os
import json
from datetime import datetime, timezone
from config.config import CHICAGO_API_URL, CHICAGO_API_TOKEN, BRONZE_CHICAGO, LIMIT
from utils.utils import log_info, log_error
from utils.api_handler import request_with_retry

# Ensure bronze directory exists
os.makedirs(BRONZE_CHICAGO, exist_ok=True)

def ingest():
    """Ingest crash data from Chicago API in batches."""
    offset = 0
    batch = 0
    headers = {"X-App-Token": CHICAGO_API_TOKEN} if CHICAGO_API_TOKEN else {}

    log_info("Starting Chicago API Ingestion...")

    while True:
        params = {
            "$limit": LIMIT,
            "$offset": offset
        }

        data = request_with_retry(CHICAGO_API_URL, params=params, headers=headers)

        if not data:
            log_info("No more data from Chicago API or persistent error occurred.")
            break

        # Socrata API might return empty list when offset exceeds total records
        if len(data) == 0:
            log_info("Empty response received. Ingestion complete.")
            break

        # Production-quality metadata with UTC 'Z' format
        output = {
            "data": data,
            "metadata": {
                "batch_id": batch,
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "source": "chicago_data_portal"
            }
        }

        file_path = os.path.join(BRONZE_CHICAGO, f"batch_{batch}.json")
        with open(file_path, "w") as f:
            json.dump(output, f)

        log_info(f"Successfully saved batch {batch} ({len(data)} records) to {file_path}")

        offset += LIMIT
        batch += 1

    log_info("Chicago API Ingestion Finished.")

if __name__ == "__main__":
    ingest()