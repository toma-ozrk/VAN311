import sqlite3
from unittest.mock import MagicMock

import pytest

from van311.db.metrics import create_db_metrics_table


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
