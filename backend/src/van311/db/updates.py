from datetime import datetime

from van311.db.core import get_db_connection, upsert_page_data
from van311.ingestion.fetch_requests import fetch_requests

UPDATE_INTERVAL_SECONDS = 1800


def update_service_requests():
    try:
        print("--- Starting scheduled update run ---")
        with get_db_connection() as con:
            requests_data = fetch_requests()
            upsert_page_data(con, requests_data)

            print(
                f"[{datetime.now()}] Successfully updated database with {len(requests_data)} records."
            )

            con.commit()
        print(f"Update finished. Sleeping for {UPDATE_INTERVAL_SECONDS} seconds.")
    except Exception as e:
        print(f"CRITICAL ERROR during update: {e}.")


def update_metrics():
    pass


if __name__ == "__main__":
    update_service_requests()
