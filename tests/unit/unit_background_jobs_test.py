from unittest.mock import call, patch

from scripts.data_gathering.seed_db import seed_database
from scripts.data_gathering.update_data import update_service_requests


@patch("scripts.data_gathering.seed_db.tqdm")
@patch("scripts.data_gathering.seed_db.get_db_connection")
def test_seed_database_success(mock_get_con, mock_tqdm, database_mocks):
    db_con = database_mocks["db_con"]
    pbar_yielded = database_mocks["pbar_instance"]
    tqdm_manager = database_mocks["tqdm_manager"]

    mock_get_con.return_value = db_con
    mock_tqdm.return_value = tqdm_manager

    with (
        patch("scripts.data_gathering.seed_db.create_db_table") as mock_create_table,
        patch(
            "scripts.data_gathering.seed_db.subtract_months_from_today"
        ) as mock_month_operation,
        patch("scripts.data_gathering.seed_db._seed_month") as mock_seed_month,
    ):
        mock_month_operation.side_effect = [(7, 2025), (8, 2025), (9, 2025)]

        seed_database()

        mock_get_con.assert_called_once()
        mock_tqdm.assert_called_once_with(total=59000, desc="😋 Seeding Database")

        mock_create_table.assert_called_once_with(db_con)

        EXPECTED_CALLS = [
            call(db_con, 7, 2025, pbar_yielded),
            call(db_con, 8, 2025, pbar_yielded),
            call(db_con, 9, 2025, pbar_yielded),
        ]

        mock_seed_month.assert_has_calls(EXPECTED_CALLS)

        db_con.commit.assert_called_once()
        db_con.__exit__.assert_called_once()
        tqdm_manager.__exit__.assert_called_once()


@patch("builtins.print")
@patch("scripts.data_gathering.seed_db.tqdm")
@patch("scripts.data_gathering.seed_db.get_db_connection")
def test_seed_database_db_connection_failiure(
    mock_get_con, mock_tqdm, mock_print, database_mocks
):
    db_con = database_mocks["db_con"]
    tqdm_manager = database_mocks["tqdm_manager"]

    mock_get_con.return_value = Exception("Connection failiure")
    mock_tqdm.return_value = tqdm_manager

    with (
        patch("scripts.data_gathering.seed_db.create_db_table") as mock_create_table,
        patch(
            "scripts.data_gathering.seed_db.subtract_months_from_today"
        ) as mock_month_operation,
        patch("scripts.data_gathering.seed_db._seed_month") as mock_seed_month,
    ):
        mock_month_operation.side_effect = [(7, 2025), (8, 2025), (9, 2025)]

        seed_database()

        mock_get_con.assert_called_once()
        mock_tqdm.assert_not_called()
        mock_create_table.assert_not_called()
        mock_seed_month.assert_not_called()
        mock_month_operation.assert_not_called()
        db_con.commit.assert_not_called()

        mock_print.assert_called_once()
        actual_call_arg = mock_print.call_args[0][0]
        assert "CRITICAL ERROR during seeding" in actual_call_arg


@patch("builtins.print")
@patch("scripts.data_gathering.seed_db.tqdm")
@patch("scripts.data_gathering.seed_db.get_db_connection")
def test_seed_database_db_creation_failiure(
    mock_get_con, mock_tqdm, mock_print, database_mocks
):
    db_con = database_mocks["db_con"]
    tqdm_manager = database_mocks["tqdm_manager"]

    mock_get_con.return_value = db_con
    mock_tqdm.return_value = tqdm_manager

    with (
        patch("scripts.data_gathering.seed_db.create_db_table") as mock_create_table,
        patch(
            "scripts.data_gathering.seed_db.subtract_months_from_today"
        ) as mock_month_operation,
        patch("scripts.data_gathering.seed_db._seed_month") as mock_seed_month,
    ):
        mock_create_table.side_effect = Exception("Database creation failiure")
        mock_month_operation.side_effect = [(7, 2025), (8, 2025), (9, 2025)]

        seed_database()

        mock_get_con.assert_called_once()
        mock_tqdm.assert_called_once()
        mock_create_table.assert_called_once()
        mock_month_operation.assert_not_called()
        mock_seed_month.assert_not_called()
        db_con.commit.assert_not_called()

        mock_print.assert_called_once()
        actual_call_arg = mock_print.call_args[0][0]
        assert "CRITICAL ERROR during seeding" in actual_call_arg


@patch("builtins.print")
@patch("scripts.data_gathering.seed_db.tqdm")
@patch("scripts.data_gathering.seed_db.get_db_connection")
def test_seed_database_failiure_mid_seed(
    mock_get_con, mock_tqdm, mock_print, database_mocks
):
    db_con = database_mocks["db_con"]
    tqdm_manager = database_mocks["tqdm_manager"]

    mock_get_con.return_value = db_con
    mock_tqdm.return_value = tqdm_manager

    with (
        patch("scripts.data_gathering.seed_db.create_db_table") as mock_create_table,
        patch(
            "scripts.data_gathering.seed_db.subtract_months_from_today"
        ) as mock_month_operation,
        patch("scripts.data_gathering.seed_db._seed_month") as mock_seed_month,
    ):
        mock_seed_month.side_effect = [None, Exception("Seeding failiure mid cycle")]
        mock_month_operation.side_effect = [(7, 2025), (8, 2025), (9, 2025)]

        seed_database()

        mock_get_con.assert_called_once()
        mock_tqdm.assert_called_once()
        mock_create_table.assert_called_once()
        assert mock_seed_month.call_count == 2
        db_con.commit.assert_not_called()

        mock_print.assert_called_once()
        actual_call_arg = mock_print.call_args[0][0]
        assert "CRITICAL ERROR during seeding" in actual_call_arg


@patch("scripts.data_gathering.update_data.upsert_page_data")
@patch("scripts.data_gathering.update_data.fetch_requests")
@patch("scripts.data_gathering.update_data.get_db_connection")
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
