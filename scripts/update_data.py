from datetime import datetime
from time import sleep

from van311.api import fetch_requests
from van311.database import get_db_connection, upsert_page_data

UPDATE_INTERVAL_SECONDS = 1800


def update_service_requests():
    while True:
        print("--- Starting scheduled update run ---")

        try:
            with get_db_connection() as con:
                requests_data = fetch_requests()
                upsert_page_data(con, requests_data)

                print(
                    f"[{datetime.now()}] Successfully updated database with {len(requests_data)} records."
                )

            print(f"Update finished. Sleeping for {UPDATE_INTERVAL_SECONDS} seconds.")
            sleep(UPDATE_INTERVAL_SECONDS)

        except Exception as e:
            print(f"CRITICAL ERROR during update: {e}. Retrying in 5 minutes.")
            sleep(300)
            continue


if __name__ == "__main__":
    update_service_requests()
