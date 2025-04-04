# T1 Roster Dashboard Updater

## Project Overview
This project retrieves information about the current T1 roster and updates a dashboard for visualization.

## How It Works
The system consists of two Cloud Run functions:

1. **Extract Function**  
   - Calls an API to fetch roster data.  
   - Loads the data into a data lake.  

2. **Load Function**  
   - Reads the data from the data lake.
   - Transforms data according to BigQuery schema.
   - Loads the processed data into BigQuery.  

Once the data is in BigQuery, it is visualized using **Looker Studio**.

## Scheduling & Execution
- **Pub/Sub is used for scheduling.**  
- There are **two Pub/Sub topics**, each with a subscriber:  
  1. The **first topic** runs on a schedule (`0 */4 * * *`, every 4 hours).  
     - It triggers the extract function in Cloud Run.  
  2. Once the extract function completes, it **publishes a message to the second topic**.  
     - The second topic then triggers the load function to update BigQuery.  

This ensures that the dashboard is updated with the latest T1 roster data automatically.
