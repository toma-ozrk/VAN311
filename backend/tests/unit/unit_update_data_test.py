from unittest.mock import call, patch

from van311.db.updates import update_service_requests


@patch("van311.db.updates.upsert_page_data")
@patch("van311.db.updates.fetch_requests")
@patch("van311.db.updates.get_db_connection")
def test_update_service_requests(
    mock_db_con,
    mock_requests,
    mock_upsert,
    database_mocks,
):
    db_con = database_mocks["db_con"]
    mock_db_con.return_value = db_con

    SAMPLE_REQUESTS = {
        "request1": "coolrequest",
        "request2": "uncoolrequest",
    }

    mock_requests.return_value = SAMPLE_REQUESTS
    mock_db_con.return_value = db_con

    update_service_requests()

    mock_db_con.assert_called_once()
    mock_requests.assert_called_once()
    mock_upsert.assert_called_once_with(db_con, SAMPLE_REQUESTS)
    db_con.commit.assert_called_once()
