import sqlite3
from unittest.mock import Mock, patch

import pytest

from van311.database import get_db_connection, upsert_service_requests


def test_get_db_connection():
    con = get_db_connection(":memory:")
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
        "id": "7df03dffc88c54dd2f0748dc30148b77a35b64f430b27e4362a02e9027b5bcc7",
    }
]


@patch("van311.database.get_db_connection")
def test_upsert_service_requests(mock_get_conn):
    mock_conn = Mock()
    mock_cursor = Mock()

    mock_conn.cursor.return_value = mock_cursor

    upsert_service_requests(mock_conn, MOCK_SUCCESS_DATA)

    mock_conn.execute.assert_called_once()

    call_args, call_kwargs = mock_conn.execute.call_args
    actual_sql = call_args[0]
    actual_params = call_args[1]

    assert actual_sql.startswith("""INSERT INTO service_requests""")
    assert actual_params == tuple(MOCK_SUCCESS_DATA[0].values())

    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()
