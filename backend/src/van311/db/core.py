import sqlite3
from dataclasses import astuple
from pathlib import Path

from ..ingestion.fetch_requests import fetch_requests
from ..models.service_request import ServiceRequest


def create_db_table(con):
    cur = con.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS service_requests(department TEXT, issue_type TEXT,
        status TEXT, closure_reason TEXT, open_ts TEXT, close_ts TEXT, modified_ts TEXT,
        address TEXT, local_area TEXT, channel TEXT, lat TEXT, long TEXT, time_to_resolve INT,
        time_to_update REAL, id TEXT PRIMARY KEY)"""
    )


def _seed_helper(con, ts, pbar):
    data = fetch_requests(timestamp=ts)

    if len(data) == 0:
        return ""

    upsert_page_data(con, data, seeding=True)
    pbar.update(100)

    return data[-1]["service_request_open_timestamp"]


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DB_PATH = PROJECT_ROOT / "data" / "vancouver.db"


def get_db_connection(db_path=DB_PATH, db_path_string=""):
    con = sqlite3.connect(db_path if not db_path_string else db_path_string, timeout=30)
    con.row_factory = sqlite3.Row
    return con


def upsert_page_data(con, requests_data, seeding: bool = False):
    for request in requests_data:
        req = ServiceRequest.dict_to_service_request(request)
        if seeding:
            req.time_to_update = None

        con.execute(
            """INSERT INTO service_requests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET status = EXCLUDED.status, closure_reason = EXCLUDED.closure_reason,
               close_ts = EXCLUDED.close_ts, modified_ts = EXCLUDED.modified_ts""",
            astuple(req),
        )


if __name__ == "__main__":
    con = get_db_connection()
