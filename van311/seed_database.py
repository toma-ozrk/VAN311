import json
from dataclasses import astuple
from datetime import date
from time import sleep

import requests
from dateutil.relativedelta import relativedelta
from tqdm import tqdm

from van311.database import get_db_connection
from van311.models import ServiceRequest


def create_db_table(con):
    cur = con.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS service_requests(department TEXT, issue_type TEXT,
        status TEXT, closure_reason TEXT, open_ts TEXT, close_ts TEXT, modified_ts TEXT,
        address TEXT, local_area TEXT, channel TEXT, lat TEXT, long TEXT, id TEXT PRIMARY KEY)"""
    )


def subtract_months_from_today(n):
    dt = date.today() - relativedelta(months=n)
    month = dt.month
    year = dt.year
    return (month, year)


def fetch_seeding_requests(offset, month, year):
    BASE_URL = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/3-1-1-service-requests/records"

    params = {
        "order_by": "service_request_open_timestamp ASC",
        "limit": 100,
        "offset": offset,
        "refine": f'service_request_open_timestamp:"{year}/{month}"',
    }

    try:
        r = requests.get(BASE_URL, params)
        r.raise_for_status()
        data = json.loads(r.text)
        return data["results"]

    except requests.exceptions.RequestException as e:
        print(f"Error during API fetch: {e}")
        return []


def seed_database(con):
    pbar = tqdm(total=59000, desc="😋 Seeding Database")

    for i in reversed(range(0, 3)):
        cur = con.cursor()
        month, year = subtract_months_from_today(i)

        j = 0
        while True:
            offset = j * 100
            data = fetch_seeding_requests(offset, 8, 2024)

            for request in data:
                req = ServiceRequest.dict_to_service_request(request)

                cur.execute(
                    """INSERT INTO service_requests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET status = EXCLUDED.status, closure_reason = EXCLUDED.closure_reason,
                    close_ts = EXCLUDED.close_ts, modified_ts = EXCLUDED.modified_ts""",
                    astuple(req),
                )
                con.commit()

            sleep(1.8)
            pbar.update(100)
            if len(data) < 100:
                break

            j += 1

    pbar.close()
    print("Task completed!")


con = get_db_connection()
create_db_table(con)
seed_database(con)
