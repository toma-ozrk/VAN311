from unittest.mock import Mock, patch

from van311.seed_database import _seed_month, create_db_table, seed_database


def test_create_db_table():
    mock_con = Mock()
    mock_cur = Mock()

    mock_con.cursor.return_value = mock_cur

    create_db_table(mock_con)
    mock_cur.execute.assert_called_once()

    call_args, call_kwargs = mock_cur.execute.call_args
    assert call_args[0].startswith("CREATE TABLE IF NOT EXISTS service_requests")
