import sqlite3

import pytest

from van311.database import (
    fetch_latest_requests,
    get_db_connection,
    upsert_service_requests,
)


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
