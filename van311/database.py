import json
import sqlite3
from dataclasses import astuple

import requests

from van311.models import ServiceRequest


def get_db_connection(db_name="../vancouver.db"):
    con = sqlite3.connect(db_name)
    con.row_factory = sqlite3.Row
    return con


def fetch_latest_requests(limit: int = 5, offset: int = 0):
    BASE_URL = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/3-1-1-service-requests/records"

    params = {
        "order_by": "last_modified_timestamp DESC",
        "limit": limit,
        "offset": offset,
    }

    try:
        r = requests.get(BASE_URL, params)
        r.raise_for_status()
        data = json.loads(r.text)
        return data["results"]

    except requests.exceptions.RequestException as e:
        print(f"Error during API fetch: {e}")
        return []


def upsert_service_requests(con, requests_data: list):
    cur = con.cursor()

    for request in requests_data:
        req = ServiceRequest.dict_to_service_request(request)

        cur.execute(
            """INSERT INTO service_requests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET status = EXCLUDED.status, closure_reason = EXCLUDED.closure_reason,
               close_ts = EXCLUDED.close_ts, modified_ts = EXCLUDED.modified_ts""",
            astuple(req),
        )
        con.commit()


con = get_db_connection()
data = fetch_latest_requests()
cur = con.cursor()
cur.execute(
    """CREATE TABLE IF NOT EXISTS service_requests(department TEXT, issue_type TEXT,
    status TEXT, closure_reason TEXT, open_ts TEXT, close_ts TEXT, modified_ts TEXT,
    address TEXT, local_area TEXT, channel TEXT, lat TEXT, long TEXT, id TEXT PRIMARY KEY)"""
)
upsert_service_requests(con, data)
