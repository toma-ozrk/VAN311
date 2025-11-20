from unittest.mock import MagicMock

import pytest


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
