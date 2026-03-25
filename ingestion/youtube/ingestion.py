import os
import json
from ingestion.youtube.youtube_client import get_video_details, get_comments
from config import BRONZE_YOUTUBE_PATH


os.makedirs(BRONZE_YOUTUBE_PATH, exist_ok=True)


def build_video_url(video_id):
    return f"https://www.youtube.com/watch?v={video_id}"


def build_comment_url(video_id, comment_id):
    return f"https://www.youtube.com/watch?v={video_id}&lc={comment_id}"


def save_json(data, filename):
    path = os.path.join(BRONZE_YOUTUBE_PATH, filename)

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

    print("Saved:", filename)


def enrich_video_data(video_id, details, comments):

    video_url = build_video_url(video_id)

    enriched = {
        "video_id": video_id,
        "video_url": video_url,
        "details": details,
        "comments": []
    }

    for c in comments:
        try:
            comment_data = c["snippet"]["topLevelComment"]
            comment_id = comment_data["id"]

            enriched["comments"].append({
                "comment_id": comment_id,
                "comment_url": build_comment_url(video_id, comment_id),
                "text": comment_data["snippet"]["textDisplay"],
                "author": comment_data["snippet"]["authorDisplayName"]
            })

        except Exception:
            continue

    return enriched


def ingest_youtube(video_ids):

    for vid in video_ids:

        print("Processing:", vid)

        details = get_video_details(vid)
        comments = get_comments(vid)

        if details is None:
            continue

        enriched_data = enrich_video_data(vid, details, comments)

        save_json(enriched_data, f"{vid}_full.json")


if __name__ == "__main__":

    video_ids = [
        "dQw4w9WgXcQ",
        "3JZ_D3ELwOQ"
    ]

    ingest_youtube(video_ids)