import os
import json
import time
from ingestion.chicago.api_client import fetch_api_data
from config import LIMIT, SLEEP_TIME, BRONZE_CHICAGO_PATH, CHECKPOINT_FILE


os.makedirs(BRONZE_CHICAGO_PATH, exist_ok=True)


def get_last_offset():
    if not os.path.exists(CHECKPOINT_FILE):
        return 0

    with open(CHECKPOINT_FILE, "r") as f:
        return int(f.read().strip())


def save_checkpoint(offset):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(offset))


def save_json(data, batch_id):
    file_path = f"{BRONZE_CHICAGO_PATH}/batch_{batch_id}.json"

    with open(file_path, "w") as f:
        json.dump(data, f)

    print(f"Saved batch {batch_id}")


def ingest_data():

    offset = get_last_offset()
    batch_id = offset // LIMIT

    print(f"Starting from offset {offset}")

    while True:

        print(f"Fetching offset: {offset}")

        data = fetch_api_data(offset)

        if data is None:
            print("API failed. Stopping.")
            break

        if len(data) == 0:
            print("No more data.")
            break

        save_json(data, batch_id)

        offset += LIMIT
        batch_id += 1

        save_checkpoint(offset)

        time.sleep(SLEEP_TIME)


if __name__ == "__main__":
    ingest_data()