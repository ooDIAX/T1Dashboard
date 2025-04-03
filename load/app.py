import os
import requests
import json
from flask import Flask, jsonify
from google.cloud import storage, bigquery
from datetime import datetime

app = Flask(__name__)

player_to_puuid = {
    "어리고싶다#KR1": "hzXrhB_kOowcOct4ChX4Y2w1csXrDKz5Op6_Lw20YVJTN39e26nVBBNXekYwjQ9YNy17pW_NDWth5A",
    "T1 Oner#T1GO": "hmFB1blGrOe9_K5FOLfHbTdaUJ-YEjhlIjFQcIXaTVGuNVyWlmtYN9oJvWwRWgD4dxc7M4zi3eEJJg",
    "Hide on bush#KR1": "DiP-XjK39x5hWFu0YmhAsKJvRGuoHdxExF80LKs1LJbzgHxHpGGBN7Q_KnROkCDIUYAJNQOboxxySA",
    "T1 Gumayusi#KR1": "D48co_DlSFYK9vq35gkelb0cbIltyNwDvFyp3F-NTq0thD_cl3zhjn8N3LFtf56TzjTgOBsuEtMxYw",
    "역천괴#Ker10": "kIhU-x7lh-nL7MiokBkRnJu_k-YWFndW1rM9mHEuvUmgKeRvFiQJwmcsp_8YUbQTFMpCbHxxs2jspg"
}

# Configuration
GCS_BUCKET_NAME = "t1dashboard"
BQ_DATASET = "riot_data"
BQ_TABLE = "match_stats"
PROJECT_ID = "Qvegriala"

# Initialize clients
storage_client = storage.Client()
bq_client = bigquery.Client()

def get_latest_gcs_file(bucket_name):
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix="t1_riot_data_"))
    if not blobs:
        raise Exception("No files found in GCS bucket")
    # Sort by creation time and get the latest
    latest_blob = sorted(blobs, key=lambda x: x.time_created, reverse=True)[0]
    return latest_blob

def transform_match_data(player_riot_id, match_data):
    # Extract relevant stats for each match
    rows = []
    for match in match_data:
        # Find the player's participant data
        participant = next(
            p for p in match["info"]["participants"] if p["puuid"] == player_to_puuid[player_riot_id]
        )
        row = {
            "player_riot_id": player_riot_id,
            "match_id": match["metadata"]["matchId"],
            "game_timestamp": datetime.fromtimestamp(match["info"]["gameCreation"] / 1000).isoformat(),
            "champion": participant["championName"],
            "kills": participant["kills"],
            "deaths": participant["deaths"],
            "assists": participant["assists"],
            "win": participant["win"],
            "game_duration": match["info"]["gameDuration"]
        }
        rows.append(row)
    return rows

# def get_puuid(riot_id):
#     # This is a simplified lookup; in production, cache this or fetch from Riot API
#     game_name, tag_line = riot_id.split("#")
#     url = f"https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
#     # headers = {"X-Riot-Token": os.environ.get("RIOT_API_KEY")}
#     headers = {"X-Riot-Token": RIOT_API_KEY}
#     response = requests.get(url, headers=headers)
#     if response.status_code == 200:
#         return response.json()["puuid"]
#     raise Exception(f"Failed to get PUUID for {riot_id}")

@app.route("/load-to-bigquery", methods=["GET"])
def load_to_bigquery():
    return jsonify({"message": f"vwov???"}), 200
    try:
        # Get the latest file from GCS
        latest_blob = get_latest_gcs_file(GCS_BUCKET_NAME)
        data = json.loads(latest_blob.download_as_string())

        # Transform data for all players
        rows_to_insert = []
        for player_riot_id, matches in data.items():
            rows_to_insert.extend(transform_match_data(player_riot_id, matches))

        # Load into BigQuery
        table_ref = bq_client.dataset(BQ_DATASET).table(BQ_TABLE)
        errors = bq_client.insert_rows_json(table_ref, rows_to_insert)

        if errors:
            raise Exception(f"Errors loading data into BigQuery: {errors}")
        
        return jsonify({"message": f"Loaded {len(rows_to_insert)} rows into BigQuery"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))