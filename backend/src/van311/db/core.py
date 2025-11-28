import sqlite3
from dataclasses import astuple
from pathlib import Path
from time import sleep

from van311.ingestion.fetch_requests import fetch_requests
from van311.models.service_request import ServiceRequest


def create_db_table(con):
    cur = con.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS service_requests(department TEXT, issue_type TEXT,
        status TEXT, closure_reason TEXT, open_ts TEXT, close_ts TEXT, modified_ts TEXT,
        address TEXT, local_area TEXT, channel TEXT, lat TEXT, long TEXT, time_to_resolve INT,
        time_to_update REAL, id TEXT PRIMARY KEY)"""
    )


def _seed_month(con, month, year, pbar):
    SLEEP_INTERVAL = 1.8

    j = 0
    while True:
        offset = j * 100
        data = fetch_requests(offs=offset, month=month, year=year, seeding=True)

        upsert_page_data(con, data, seeding=True)

        sleep(SLEEP_INTERVAL)
        pbar.update(100)
        if len(data) < 100:
            break

        j += 1
        # con.commit() # for partial seeding testing only


PROJECT_ROOT = Path(__file__).resolve().parents[4]
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
