import os
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
STAGING_TABLE = "match_stats_staging"
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

def create_staging_table():
    """Create or recreate the staging table"""
    schema = [
        bigquery.SchemaField("player_riot_id", "STRING"),
        bigquery.SchemaField("match_id", "STRING"),
        bigquery.SchemaField("game_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("champion", "STRING"),
        bigquery.SchemaField("kills", "INTEGER"),
        bigquery.SchemaField("deaths", "INTEGER"),
        bigquery.SchemaField("assists", "INTEGER"),
        bigquery.SchemaField("win", "BOOLEAN"),
        bigquery.SchemaField("game_duration", "INTEGER")
    ]
    table_ref = bq_client.dataset(BQ_DATASET).table(STAGING_TABLE)
    table = bigquery.Table(table_ref, schema=schema)
    bq_client.delete_table(table_ref, not_found_ok=True)  # Delete if exists
    bq_client.create_table(table)

@app.route("/load-to-bigquery", methods=["GET"])
def load_to_bigquery():
    try:
        # Get the latest file from GCS
        latest_blob = get_latest_gcs_file(GCS_BUCKET_NAME)
        data = json.loads(latest_blob.download_as_string())

        # Transform data for all players
        rows_to_insert = []
        for player_riot_id, matches in data.items():
            rows_to_insert.extend(transform_match_data(player_riot_id, matches))

        if not rows_to_insert:
            return jsonify({"message": "No new rows to load"}), 200

        # Create staging table and load new data
        create_staging_table()
        staging_table_ref = bq_client.dataset(BQ_DATASET).table(STAGING_TABLE)
        errors = bq_client.insert_rows_json(staging_table_ref, rows_to_insert)
        if errors:
            raise Exception(f"Errors loading data into staging table: {errors}")

        # Merge staging table into main table
        merge_query = f"""
            MERGE `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}` T
            USING `{PROJECT_ID}.{BQ_DATASET}.{STAGING_TABLE}` S
            ON T.match_id = S.match_id
            WHEN NOT MATCHED THEN
                INSERT (player_riot_id, match_id, game_timestamp, champion, kills, deaths, assists, win, game_duration)
                VALUES (player_riot_id, match_id, game_timestamp, champion, kills, deaths, assists, win, game_duration)
        """
        query_job = bq_client.query(merge_query)
        query_job.result()  # Wait for the query to complete

        return jsonify({"message": f"Processed {len(rows_to_insert)} rows, merged into BigQuery"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))