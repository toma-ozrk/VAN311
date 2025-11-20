import json
import sqlite3
from dataclasses import astuple

import requests

import models

r = requests.get(
    "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/3-1-1-service-requests/records?order_by=service_request_open_timestamp%20DESC&limit=5"
)

data = json.loads(r.text)

con = sqlite3.connect("vancouver.db")
cur = con.cursor()

cur.execute(
    """CREATE TABLE IF NOT EXISTS service_requests(department TEXT, issue_type TEXT,
    status TEXT, closure_reason TEXT, open_ts TEXT, close_ts TEXT, modified_ts TEXT,
    address TEXT, local_area TEXT, channel TEXT, lat TEXT, long TEXT)"""
)

for request in data["results"]:
    values = list(request.values())
    req = models.ServiceRequest(*(values[:-1]))
    # print(astuple(req))
    # break
    cur.execute(
        """INSERT INTO service_requests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        astuple(req),
    )
    con.commit()
