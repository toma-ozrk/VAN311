import sqlite3
from dataclasses import astuple

from van311.api import fetch_requests
from van311.models import ServiceRequest


def get_db_connection(db_name="../vancouver.db"):
    con = sqlite3.connect(db_name)
    con.row_factory = sqlite3.Row
    return con


def upsert_page_data(con, requests_data):
    for request in requests_data:
        req = ServiceRequest.dict_to_service_request(request)

        con.execute(
            """INSERT INTO service_requests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET status = EXCLUDED.status, closure_reason = EXCLUDED.closure_reason,
               close_ts = EXCLUDED.close_ts, modified_ts = EXCLUDED.modified_ts""",
            astuple(req),
        )


def upsert_service_requests(con, requests_data: list):
    upsert_page_data(con, requests_data)
    con.commit()
    con.close()
