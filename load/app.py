import os
import requests
import json
import logging
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

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        logger.warning("No files found in GCS bucket")
        raise Exception("No files found in GCS bucket")
    latest_blob = sorted(blobs, key=lambda x: x.time_created, reverse=True)[0]
    logger.info(f"Found latest file: {latest_blob.name}")
    return latest_blob

def transform_match_data(player_riot_id, match_data):
    rows = []
    for match in match_data:
        participant = next(
            (p for p in match["info"]["participants"] if p["puuid"] == player_to_puuid[player_riot_id]),
            None
        )
        if participant is None:
            logger.warning(f"No participant data for {player_riot_id} in match {match['metadata']['matchId']}")
            continue
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

@app.route("/load-to-bigquery", methods=["GET"])
def load_to_bigquery():
    try:
        # Get the latest file from GCS
        latest_blob = get_latest_gcs_file(GCS_BUCKET_NAME)
        data = json.loads(latest_blob.download_as_string())
        logger.info(f"Loaded data from GCS with {len(data)} players")

        # Transform data for all players
        rows_to_insert = []
        for player_riot_id, matches in data.items():
            transformed_rows = transform_match_data(player_riot_id, matches)
            rows_to_insert.extend(transformed_rows)
        logger.info(f"Transformed {len(rows_to_insert)} rows")

        if not rows_to_insert:
            return jsonify({"message": "No rows to load into BigQuery"}), 200

        # Write transformed data to a temporary GCS file
        temp_gcs_path = f"temp/transformed_data_{int(datetime.utcnow().timestamp())}.json"
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(temp_gcs_path)
        blob.upload_from_string(json.dumps(rows_to_insert), content_type="application/json")
        gcs_uri = f"gs://{GCS_BUCKET_NAME}/{temp_gcs_path}"

        # Load into a temporary table using batch load
        temp_table = f"{PROJECT_ID}.{BQ_DATASET}.temp_match_stats_{int(datetime.utcnow().timestamp())}"
        table_ref = bq_client.dataset(BQ_DATASET).table(temp_table.split('.')[-1])
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
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        load_job.result()  # Wait for the load to complete
        logger.info(f"Loaded {len(rows_to_insert)} rows into temporary table")

        # Perform MERGE operation
        merge_query = f"""
        MERGE `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}` T
        USING `{temp_table}` S
        ON T.match_id = S.match_id AND T.player_riot_id = S.player_riot_id
        WHEN MATCHED THEN
            UPDATE SET
                game_timestamp = S.game_timestamp,
                champion = S.champion,
                kills = S.kills,
                deaths = S.deaths,
                assists = S.assists,
                win = S.win,
                game_duration = S.game_duration
        WHEN NOT MATCHED THEN
            INSERT (player_riot_id, match_id, game_timestamp, champion, kills, deaths, assists, win, game_duration)
            VALUES (player_riot_id, match_id, game_timestamp, champion, kills, deaths, assists, win, game_duration)
        """
        query_job = bq_client.query(merge_query)
        query_job.result()
        logger.info(f"Merged {len(rows_to_insert)} rows into BigQuery")

        # Clean up temporary resources
        bq_client.delete_table(table_ref, not_found_ok=True)
        bucket.blob(temp_gcs_path).delete()

        return jsonify({"message": f"Merged {len(rows_to_insert)} rows into BigQuery"}), 200
    except Exception as e:
        logger.error(f"Error in load-to-bigquery: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))