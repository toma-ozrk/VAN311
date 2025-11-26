import sqlite3
from unittest.mock import Mock, patch

import pytest

from van311.db.core import (
    _seed_month,
    create_db_table,
    get_db_connection,
    upsert_page_data,
)
from van311.models.service_request import ServiceRequest


def test_get_db_connection():
    con = get_db_connection(db_path_string=":memory:")
    assert isinstance(con, sqlite3.Connection)

    try:
        con.execute("CREATE TABLE test_table(id INTEGER PRIMARY KEY, value TEXT)")
        con.execute("INSERT INTO test_table (value) VALUES('coolvalue')")
        con.commit()

        data = con.execute("SELECT value FROM test_table WHERE id = 1")
        row = data.fetchone()
        assert row is not None and row["value"] == "coolvalue"

    except sqlite3.Error as e:
        pytest.fail(f"Database operation failed: {e}")

    finally:
        con.close()


MOCK_SUCCESS_DATA = [
    {
        "department": "DBL - Property Use Inspections",
        "service_request_type": "Noise on Private Property Case",
        "status": "Close",
        "closure_reason": "Assigned to inspector",
        "service_request_open_timestamp": "2025-08-25T15:03:00+00:00",
        "service_request_close_date": "2025-08-27",
        "last_modified_timestamp": "2025-08-27T18:56:08+00:00",
        "address": None,
        "local_area": "Marpole",
        "channel": "WEB",
        "latitude": None,
        "longitude": None,
        "time_to_resolve": 2,
        "time_to_update": 2.2,
        "id": "7df03dffc88c54dd2f0748dc30148b77a35b64f430b27e4362a02e9027b5bcc7",
    }
]


@patch("van311.db.core.ServiceRequest")
def test_upsert_page_data(mock_request):

    input_data = [
        {"id": "1", "value": "1"},
        {"id": "2", "value": "2"},
        {"id": "3", "value": "3"},
        {"id": "4", "value": "4"},
        {"id": "5", "value": "5"},
        {"id": "6", "value": "6"},
        {"id": "7", "value": "7"},
        {"id": "8", "value": "8"},
        {"id": "9", "value": "9"},
        {"id": "10", "value": "10"},
        {"id": "11", "value": "11"},
        {"id": "12", "value": "12"},
        {"id": "13", "value": "13"},
    ]

    service_request_params = (
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "10",
        "11",
        "12",
        13,
        14,
        "15",
    )

    mock_con = Mock()
    mock_request.dict_to_service_request.return_value = ServiceRequest(
        *service_request_params
    )

    upsert_page_data(mock_con, input_data)

    assert mock_request.dict_to_service_request.call_count == 13
    assert mock_con.execute.call_count == 13

    call_args, _ = mock_con.execute.call_args
    actual_sql = call_args[0]
    actual_parameters = call_args[1]

    assert actual_sql.startswith("INSERT INTO service_requests"), (
        "SQL should insert into service requests"
    )
    assert actual_parameters == service_request_params


def test_create_db_table():
    mock_con = Mock()
    mock_cur = Mock()

    mock_con.cursor.return_value = mock_cur

    create_db_table(mock_con)
    mock_cur.execute.assert_called_once()

    call_args, call_kwargs = mock_cur.execute.call_args
    assert call_args[0].startswith("CREATE TABLE IF NOT EXISTS service_requests")


requests_100 = [{f"{x}": f"{x}"} for x in range(0, 100)]
requests_0 = [{}]


@patch("van311.db.core.fetch_requests")
@patch("van311.db.core.upsert_page_data")
def test_seed_month(mock_page_data, mock_requests):
    mock_con = Mock()
    mock_page_data.return_value = None
    mock_pbar = Mock()

    mock_requests.side_effect = [requests_100, requests_100, requests_100, requests_0]
    _seed_month(mock_con, 5, 2023, mock_pbar)

    assert mock_requests.call_count == 4
    assert mock_page_data.call_count == 4


@patch("van311.db.core.fetch_requests")
@patch("van311.db.core.upsert_page_data")
def test_seed_month_no_results(mock_page_data, mock_requests):
    mock_con = Mock()
    mock_page_data.return_value = None
    mock_pbar = Mock()

    mock_requests.side_effect = [requests_0]
    _seed_month(mock_con, 5, 2023, mock_pbar)

    mock_requests.assert_called_once()
    mock_page_data.assert_called_once()
