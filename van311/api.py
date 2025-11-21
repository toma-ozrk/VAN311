import json

import requests


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
