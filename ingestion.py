import os
import json
import time
from api_client import fetch_api_data
from config.config import LIMIT, SLEEP_TIME, BRONZE_PATH, CHECKPOINT_FILE

# create bronze folder
os.makedirs(BRONZE_PATH, exist_ok=True)


def get_last_offset():
    if not os.path.exists(CHECKPOINT_FILE):
        return 0

    with open(CHECKPOINT_FILE, "r") as f:
        return int(f.read().strip())


def save_checkpoint(offset):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(offset))


def save_json(data, batch_id):
    file_path = f"{BRONZE_PATH}/batch_{batch_id}.json"

    with open(file_path, "w") as f:
        json.dump(data, f)

    print("Saved:", file_path)


def ingest_data():

    offset = get_last_offset()
    batch_id = offset // LIMIT

    while True:

        print("Fetching offset:", offset)

        data = fetch_api_data(offset)

        if not data:
            print("No more data.")
            break

        save_json(data, batch_id)

        offset += LIMIT
        batch_id += 1

        save_checkpoint(offset)

        time.sleep(SLEEP_TIME)


if __name__ == "__main__":
    ingest_data()