import sqlite3
from unittest.mock import MagicMock

import pytest

from van311.db.aggregation import create_db_metrics_table
from van311.db.core import create_db_table


@pytest.fixture
def database_mocks():
    mock_db_con = MagicMock(name="db_connection")
    mock_pbar_yielded = MagicMock(name="pbar_instance")
    mock_tqdm_context_manager = MagicMock(name="tqdm_context_manager")

    mock_db_con.__enter__.return_value = mock_db_con
    mock_tqdm_context_manager.__enter__.return_value = mock_pbar_yielded

    return {
        "db_con": mock_db_con,
        "pbar_instance": mock_pbar_yielded,
        "tqdm_manager": mock_tqdm_context_manager,
    }


@pytest.fixture(scope="function")
def db_metrics_connection():
    inserted_rows = [
        # la           it      ttr ttu close_ts
        ("Kitsilano", "Pothole", 4, 2, 1),
        ("Kitsilano", "Graffiti", 4, 2, 1),
        ("Downtown", "Pothole", 2, 1, 1),
        ("Downtown", "Graffiti", None, 2, None),
        (None, "Other", 1, 1, 1),
    ]

    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row

    con.execute(
        """CREATE TABLE service_requests(local_area TEXT, issue_type TEXT,
        time_to_resolve INT, time_to_update REAL, close_ts INT)"""
    )

    create_db_metrics_table(con)

    for row in inserted_rows:
        con.execute("""INSERT INTO service_requests VALUES (?, ?, ?, ?, ?)""", row)

    con.commit()
    yield con

    con.close()


@pytest.fixture(scope="function")
def db_router_query_connection():
    inserted_rows = [
        # la           it               name      val calc_at
        ("Kitsilano", "Pothole", "time_to_resolve", 2, "abc"),
        ("Kitsilano", "CITYWIDE", "time_to_update", 3, "abc"),
        ("CITYWIDE", "Pothole", "time_to_resolve", 2, "abc"),
        ("CITYWIDE", "CITYWIDE", "time_to_update", 3, "abc"),
        ("UNKNOWN_AREA", "UNKNOWN_ISSUE", "null_volume", 8, "abc"),
    ]

    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row

    con.execute(
        """CREATE TABLE metric_aggregates(local_area TEXT, issue_type TEXT,
        metric_name TEXT, metric_value REAL, calculated_at TEXT)"""
    )

    create_db_metrics_table(con)

    for row in inserted_rows:
        con.execute("""INSERT INTO metric_aggregates VALUES (?, ?, ?, ?, ?)""", row)

    con.commit()
    yield con

    con.close()


@pytest.fixture(scope="function")
def db_router_data_connection():
    row = (
        "DBL - Property Use Inspections",
        "Noise on Private Property Case",
        "Open",
        "Assigned to inspector",
        "2025-08-25T15:03:00+00:00",
        None,
        "2025-08-27T18:56:08+00:00",
        None,
        "Marpole",
        "WEB",
        None,
        None,
        2,
        2.2,
        "7df03dffc88c54dd2f0748dc30148b77a35b64f430b27e4362a02e9027b5bcc7",
    )

    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row

    con.execute(
        """CREATE TABLE IF NOT EXISTS service_requests(department TEXT, issue_type TEXT,
        status TEXT, closure_reason TEXT, open_ts TEXT, close_ts TEXT, modified_ts TEXT,
        address TEXT, local_area TEXT, channel TEXT, lat TEXT, long TEXT, time_to_resolve INT,
        time_to_update REAL, id TEXT PRIMARY KEY)"""
    )

    create_db_table(con)

    con.execute(
        """INSERT INTO service_requests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        row,
    )

    con.commit()
    yield con

    con.close()
