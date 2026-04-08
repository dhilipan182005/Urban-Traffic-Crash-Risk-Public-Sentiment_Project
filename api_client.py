import requests
import time
from config.config import API_URL, API_TOKEN, LIMIT, MAX_RETRY


def fetch_api_data(offset):
    """
    Fetch batch data from API using offset
    """

    headers = {
        "X-App-Token": API_TOKEN
    }

    params = {
        "$limit": LIMIT,
        "$offset": offset
    }

    retry = 0

    while retry < MAX_RETRY:

        try:

            response = requests.get(API_URL, headers=headers, params=params)

            if response.status_code == 200:

                return response.json()

            else:

                print("API Error:", response.status_code)

        except Exception as e:

            print("Request failed:", e)

        retry += 1

        time.sleep(5)

    return None