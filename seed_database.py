import hashlib
import json
import sqlite3
from dataclasses import astuple
from datetime import date
from time import sleep

import requests
from dateutil.relativedelta import relativedelta
from tqdm import tqdm

import models

con = sqlite3.connect("vancouver.db")
cur = con.cursor()

cur.execute(
    """CREATE TABLE IF NOT EXISTS service_requests(department TEXT, issue_type TEXT,
    status TEXT, closure_reason TEXT, open_ts TEXT, close_ts TEXT, modified_ts TEXT,
    address TEXT, local_area TEXT, channel TEXT, lat TEXT, long TEXT, id TEXT PRIMARY KEY)"""
)

# progress bar :D
pbar = tqdm(total=590, desc="😋 Seeding Database")

for i in reversed(range(0, 3)):
    dt = date.today() - relativedelta(months=i)
    month = dt.month
    year = dt.year

    j = 0
    while True:
        offset = j * 100
        r = requests.get(
            f"https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/3-1-1-service-requests/records?order_by=service_request_open_timestamp%20ASC&limit=100&offset={offset}&refine=service_request_open_timestamp%3A%22{year}%2F{month}%22"
        )

        data = json.loads(r.text)

        for request in data["results"]:
            # id hashing for each service request
            key = (
                request["department"]
                + request["service_request_type"]
                + request["service_request_open_timestamp"]
            )
            unique_id = (hashlib.sha256(key.encode("utf-8"))).hexdigest()

            # parsing
            req = models.ServiceRequest(
                department=request["department"],
                issue_type=request["service_request_type"],
                status=request["status"],
                closure_reason=request["closure_reason"],
                open_ts=request["service_request_open_timestamp"],
                close_ts=request["service_request_close_date"],
                last_modified=request["last_modified_timestamp"],
                address=request["address"],
                local_area=request["local_area"],
                channel=request["channel"],
                lat=request["latitude"],
                lon=request["longitude"],
                id=unique_id,
            )  # Ignoring geom parameter from API

            cur.execute(
                """INSERT INTO service_requests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET status = EXCLUDED.status, closure_reason = EXCLUDED.closure_reason,
                close_ts = EXCLUDED.close_ts, modified_ts = EXCLUDED.modified_ts""",
                astuple(req),
            )
            con.commit()

        sleep(2)
        pbar.update(1)
        if len(data["results"]) < 100:
            break

        j += 1

pbar.close()
print("Task completed!")
