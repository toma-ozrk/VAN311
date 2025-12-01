import json

import requests


def fetch_requests(limit: int = 50, timestamp=""):
    BASE_URL = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/3-1-1-service-requests/records"

    if timestamp:
        params = {
            "order_by": "service_request_open_timestamp ASC",
            "limit": 100,
            "where": f"service_request_open_timestamp > '{timestamp}'",
        }
    else:
        params = {
            "order_by": "last_modified_timestamp DESC",
            "limit": limit,
        }

    try:
        r = requests.get(BASE_URL, params)
        r.raise_for_status()
        data = json.loads(r.text)
        return data["results"]

    except requests.exceptions.RequestException as e:
        print(f"Error during API fetch: {e}")
        raise
        return []
