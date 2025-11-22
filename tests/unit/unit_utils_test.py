from datetime import date
from unittest.mock import patch

from van311.utils import subtract_months_from_today


@patch("van311.utils.date")
def test_subtract_months_from_today_rollover(mock_date):
    mock_date.today.return_value = date(2025, 1, 15)

    month, year = subtract_months_from_today(1)

    assert month == 12, "Should roll back to December"
    assert year == 2024, "Should roll back to 2024"


@patch("van311.utils.date")
def test_subtract_months_from_today_large_n(mock_date):
    mock_date.today.return_value = date(2025, 5, 15)

    month, year = subtract_months_from_today(13)

    assert month == 4, "Should be April"
    assert year == 2024, "Should be 2024"
