import hashlib
import json
import sqlite3
from dataclasses import astuple

import requests

import models

r = requests.get(
    "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/3-1-1-service-requests/records?order_by=last_modified_timestamp%20DESC&limit=4&offset=0"
)

data = json.loads(r.text)

con = sqlite3.connect("vancouver.db")
cur = con.cursor()

cur.execute(
    """CREATE TABLE IF NOT EXISTS service_requests(department TEXT, issue_type TEXT,
    status TEXT, closure_reason TEXT, open_ts TEXT, close_ts TEXT, modified_ts TEXT,
    address TEXT, local_area TEXT, channel TEXT, lat TEXT, long TEXT, id TEXT PRIMARY KEY)"""
)

for request in data["results"]:
    # id hashing for each service request
    key = (
        request["department"]
        + request["service_request_type"]
        + request["service_request_open_timestamp"]
    )
    id = hashlib.sha256(key.encode("utf-8"))

    # parsing
    values = list(request.values())
    values.insert(-1, id.hexdigest())
    req = models.ServiceRequest(*(values[:-1]))  # Ignoring geom parameter from API
    cur.execute(
        """INSERT INTO service_requests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET status = EXCLUDED.status, closure_reason = EXCLUDED.closure_reason,
           close_ts = EXCLUDED.close_ts, modified_ts = EXCLUDED.modified_ts""",
        astuple(req),
    )
    con.commit()
