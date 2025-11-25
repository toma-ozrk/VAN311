from unittest.mock import Mock, call, patch

from van311.seed_database import _seed_month, create_db_table, seed_database


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


@patch("van311.seed_database.fetch_requests")
@patch("van311.seed_database.upsert_page_data")
def test_seed_month(mock_page_data, mock_requests):
    mock_con = Mock()
    mock_page_data.return_value = None
    mock_pbar = Mock()

    mock_requests.side_effect = [requests_100, requests_100, requests_100, requests_0]
    _seed_month(mock_con, 5, 2023, mock_pbar)

    mock_con.commit.assert_called_once()
    assert mock_requests.call_count == 4
    assert mock_page_data.call_count == 4


@patch("van311.seed_database.fetch_requests")
@patch("van311.seed_database.upsert_page_data")
def test_seed_month_no_results(mock_page_data, mock_requests):
    mock_con = Mock()
    mock_page_data.return_value = None
    mock_pbar = Mock()

    mock_requests.side_effect = [requests_0]
    _seed_month(mock_con, 5, 2023, mock_pbar)

    mock_con.commit.assert_called_once()
    mock_requests.assert_called_once()
    mock_page_data.assert_called_once()


@patch("van311.seed_database.tqdm")
@patch("van311.seed_database.subtract_months_from_today")
@patch("van311.seed_database._seed_month")
def test_seed_database(mock_seed_month, mock_month_operation, mock_pbar):
    mock_con = Mock()
    mock_month_operation.side_effect = [(11, 2025), (10, 2025), (9, 2025)]

    mock_pbar_instance = Mock()
    mock_pbar.return_value = mock_pbar_instance

    seed_database(mock_con)
    mock_pbar.assert_called_once()
    assert mock_seed_month.call_count == 3
    assert mock_month_operation.call_count == 3

    expected_calls = [
        call(mock_con, 11, 2025, mock_pbar_instance),
        call(mock_con, 10, 2025, mock_pbar_instance),
        call(mock_con, 9, 2025, mock_pbar_instance),
    ]

    mock_seed_month.assert_has_calls(expected_calls)
    mock_con.close.assert_called_once()
    mock_pbar_instance.close.assert_called_once()
