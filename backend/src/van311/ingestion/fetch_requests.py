import json

import requests


def fetch_requests(
    limit: int = 50,
    offs: int = 0,
    year: int = 0,
    month: int = 0,
    seeding: bool = False,
):
    BASE_URL = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/3-1-1-service-requests/records"

    if seeding:
        params = {
            "order_by": "service_request_open_timestamp ASC",
            "limit": 100,
            "offset": offs,
            "refine": f'service_request_open_timestamp:"{year}/{month}"',
        }
    else:
        params = {
            "order_by": "last_modified_timestamp DESC",
            "limit": limit,
            "offset": offs,
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
