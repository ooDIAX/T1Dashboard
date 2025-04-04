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
PROJECT_ID = "qvegriala"

# Initialize clients
storage_client = storage.Client()
bq_client = bigquery.Client()

def get_latest_gcs_file(bucket_name):
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix="t1_riot_data_"))
    if not blobs:
        raise Exception("No files found in GCS bucket")
    latest_blob = sorted(blobs, key=lambda x: x.time_created, reverse=True)[0]
    return latest_blob

def transform_match_data(player_riot_id, match_data):
    rows = []
    for match in match_data:
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

def get_existing_match_ids():
    """Query BigQuery to get all existing match IDs"""
    query = f"""
        SELECT match_id
        FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
    """
    query_job = bq_client.query(query)
    results = query_job.result()
    return set(row.match_id for row in results)

@app.route("/load-to-bigquery", methods=["GET"])
def load_to_bigquery():
    try:
        # Get existing match IDs from BigQuery
        existing_match_ids = get_existing_match_ids()

        # Get the latest file from GCS
        latest_blob = get_latest_gcs_file(GCS_BUCKET_NAME)
        data = json.loads(latest_blob.download_as_string())

        # Transform data for all players and filter out existing matches
        rows_to_insert = []
        for player_riot_id, matches in data.items():
            player_rows = transform_match_data(player_riot_id, matches)
            # Only include rows with match_ids that don't exist in BigQuery
            new_rows = [row for row in player_rows if row["match_id"] not in existing_match_ids]
            rows_to_insert.extend(new_rows)

        if not rows_to_insert:
            return jsonify({"message": "No new rows to load into BigQuery"}), 200

        # Load new rows into BigQuery
        table_ref = bq_client.dataset(BQ_DATASET).table(BQ_TABLE)
        errors = bq_client.insert_rows_json(table_ref, rows_to_insert)

        if errors:
            raise Exception(f"Errors loading data into BigQuery: {errors}")
        
        return jsonify({
            "message": f"Loaded {len(rows_to_insert)} new rows into BigQuery",
            "skipped": len(player_rows) - len(new_rows) if 'player_rows' in locals() else 0
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))