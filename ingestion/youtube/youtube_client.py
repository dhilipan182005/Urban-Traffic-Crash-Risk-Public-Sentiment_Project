import requests
import time
from config import YOUTUBE_API_KEY, YOUTUBE_BASE_URL


def get_video_details(video_id):

    url = f"{YOUTUBE_BASE_URL}/videos"

    params = {
        "part": "snippet,statistics",
        "id": video_id,
        "key": YOUTUBE_API_KEY
    }

    res = requests.get(url, params=params)

    if res.status_code == 200:
        return res.json()

    print("Error fetching video:", res.status_code)
    return None


def get_comments(video_id):

    url = f"{YOUTUBE_BASE_URL}/commentThreads"

    params = {
        "part": "snippet",
        "videoId": video_id,
        "maxResults": 100,
        "key": YOUTUBE_API_KEY
    }

    all_comments = []

    while True:

        res = requests.get(url, params=params)

        if res.status_code != 200:
            print("Error fetching comments:", res.status_code)
            break

        data = res.json()
        all_comments.extend(data.get("items", []))

        if "nextPageToken" not in data:
            break

        params["pageToken"] = data["nextPageToken"]
        time.sleep(1)

    return all_comments