import json
from unittest.mock import Mock, patch

from van311.api import fetch_requests

MOCK_SUCCESS_DATA = {
    "results": [
        {"request_id": 1},
        {"status": "Open"},
        {"request_id": 2},
        {"status": "Closed"},
    ]
}


@patch("van311.api.requests.get")
def test_fetch_requests(mock_get):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = json.dumps(MOCK_SUCCESS_DATA)

    mock_get.return_value = mock_response

    results = fetch_requests(limit=2)

    mock_get.assert_called_once_with(
        "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/3-1-1-service-requests/records",
        {
            "order_by": "last_modified_timestamp DESC",
            "limit": 2,
            "offset": 0,
        },
    )

    assert results == MOCK_SUCCESS_DATA["results"]
    assert len(results) == 4


@patch("van311.api.requests.get")
def test_fetch_requests_seeding(mock_get):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = json.dumps(MOCK_SUCCESS_DATA)

    mock_get.return_value = mock_response

    results = fetch_requests(year=2023, month=9, seeding=True)

    mock_get.assert_called_once_with(
        "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/3-1-1-service-requests/records",
        {
            "order_by": "service_request_open_timestamp ASC",
            "limit": 100,
            "offset": 0,
            "refine": 'service_request_open_timestamp:"2023/9"',
        },
    )

    assert results == MOCK_SUCCESS_DATA["results"]
    assert len(results) == 4
