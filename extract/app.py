import os
import requests
from flask import Flask, jsonify
from google.cloud import storage
from datetime import datetime

app = Flask(__name__)

# Configuration
RIOT_API_KEY = "RGAPI-9839e078-e41b-4548-8726-188eb40ca4ae"  # Replace with your Riot API key
GCS_BUCKET_NAME = "t1dashboard"  # Replace with your GCS bucket name
REGION = "asia"  # Riot API region (adjust if targeting KR server)

# T1's active 2025 roster with Riot IDs (gameName#tagLine)
PLAYERS = [
    "Doran#KR1",  # Choi "Doran" Hyeon-joon
    "Oner#KR1",   # Mun "Oner" Hyeon-jun
    "Faker#KR1",  # Lee "Faker" Sang-hyeok
    "Gumayusi#KR1",  # Lee "Gumayusi" Min-hyeong
    "Keria#KR1"   # Ryu "Keria" Min-seok
]

# Initialize GCS client
storage_client = storage.Client()

def get_puuid(riot_id):
    game_name, tag_line = riot_id.split("#")
    url = f"https://{REGION}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
    headers = {"X-Riot-Token": RIOT_API_KEY}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()["puuid"]
    else:
        raise Exception(f"Failed to get PUUID for {riot_id}: {response.status_code}")

def get_last_matches(puuid, count=5):
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
    blob.upload_from_string(str(data), content_type="application/json")
    return f"gs://{GCS_BUCKET_NAME}/{destination_blob_name}"

@app.route("/fetch-stats", methods=["GET"])
def fetch_stats():
    return jsonify({"test": "works"}), 200
    try:
        all_player_data = {}
        for player in PLAYERS:
            # Get PUUID using Riot ID
            puuid = get_puuid(player)
            # Get last 5 match IDs
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