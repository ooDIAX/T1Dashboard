import os
import requests
import json
import time
from flask import Flask, jsonify
from google.cloud import storage
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
RIOT_API_KEY = "RGAPI-edec9f74-1786-47a6-b8ec-453314cd0406"
GCS_BUCKET_NAME = "t1dashboard"
REGION = "asia"

# T1's active 2025 roster with Riot IDs (gameName#tagLine)
PLAYERS = [
    "어리고싶다#KR1",  # Choi "Doran" Hyeon-joon
    "T1 Oner#T1GO",   # Mun "Oner" Hyeon-jun
    "Hide on bush#KR1",  # Lee "Faker" Sang-hyeok
    "T1 Gumayusi#KR1",  # Lee "Gumayusi" Min-hyeong
    "역천괴#Ker10"   # Ryu "Keria" Min-seok
]

# Initialize GCS client
storage_client = storage.Client()

def get_last_matches(puuid, count=10):
    url = f"https://{REGION}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?count={count}"
    headers = {"X-Riot-Token": RIOT_API_KEY}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to get matches for PUUID {puuid}: {response.status_code}")

def get_match_details(match_id):
    url = f"https://{REGION}.api.riotgames.com/lol/match/v5/matches/{match_id}"
    headers = {"X-Riot-Token": RIOT_API_KEY}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to get match details for {match_id}: {response.status_code}")

def upload_to_gcs(data, destination_blob_name):
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(json.dumps(data), content_type="application/json")
    return f"gs://{GCS_BUCKET_NAME}/{destination_blob_name}"

@app.route("/fetch-stats", methods=["GET"])
def fetch_stats():
    try:
        all_player_data = {}
        for player in PLAYERS:
            # Get PUUID using Riot ID
            puuid = player_to_puuid[player]
            match_ids = get_last_matches(puuid)

            # Get match details
            matches = [get_match_details(match_id) for match_id in match_ids]
            all_player_data[player] = matches

        # Generate a unique filename with timestamp
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"t1_riot_data_{timestamp}.json"

        # Upload to GCS
        gcs_uri = upload_to_gcs(all_player_data, filename)
        return jsonify({"message": "T1 roster data fetched and uploaded", "gcs_uri": gcs_uri}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))