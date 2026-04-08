import requests
import time
from config import CHICAGO_API_URL, LIMIT, MAX_RETRY

def fetch_api_data(offset):

    params = {
        "$limit": LIMIT,
        "$offset": offset
    }

    retry = 0

    while retry < MAX_RETRY:
        try:
            response = requests.get(CHICAGO_API_URL, params=params, timeout=30)

            if response.status_code == 200:
                return response.json()

            print("API Error:", response.status_code)

        except Exception as e:
            print("Request failed:", e)

        retry += 1
        time.sleep(2)

    return None