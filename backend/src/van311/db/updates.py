from datetime import datetime

from ..ingestion.fetch_requests import fetch_requests
from .core import get_db_connection, upsert_page_data

GRACE_TIME_SECONDS = 10
UPDATE_INTERVAL_MINUTES = 3


def update_service_requests():
    try:
        print("------ Starting scheduled update run ------")
        with get_db_connection() as con:
            requests_data = fetch_requests()
            upsert_page_data(con, requests_data)

            print(
                f"[{datetime.now()}] Successfully updated database with {len(requests_data)} records.\n"
            )

            con.commit()
        print(
            f"------ Finished update run. Sleeping for {UPDATE_INTERVAL_MINUTES} minutes  ------"
        )
    except Exception as e:
        print(
            f"CRITICAL ERROR during update: {e}.\n ------ Retrying in {GRACE_TIME_SECONDS} ------"
        )


if __name__ == "__main__":
    update_service_requests()
