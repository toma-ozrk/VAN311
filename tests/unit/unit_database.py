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


MOCK_SUCCESS_DATA = [{"request_id": "1"}, {"value": "coolvalue"}]
EXPECTED_SQL = """INSERT INTO service_requests VALUES (?, ?)
   ON CONFLICT(id) DO UPDATE SET value = EXCLUDED.value"""


@patch("van311.database.get_db_connection")
def test_upsert_service_requests(mock_get_conn):
    mock_conn = Mock()
    mock_cursor = Mock()

    mock_conn.cursor.return_value = mock_cursor

    upsert_service_requests(mock_conn, MOCK_SUCCESS_DATA)

    mock_conn.assert_called_once()
    mock_cursor.execute.assert_called_once()

    call_args, call_kwargs = mock_cursor.execute.call_args
    actual_sql = call_args[0]
    actual_params = call_args[1]

    assert actual_sql.starts_with("""INSERT INTO service_requests""")
    assert actual_params == ("1", "coolvalue")

    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()
