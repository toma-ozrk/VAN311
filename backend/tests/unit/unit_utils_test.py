from datetime import date, datetime
from unittest.mock import patch, MagicMock

from van311.utils.dates import subtract_months_from_today, calculate_day_difference, calculate_timestamp_difference, timestamp_to_date_obj


# ------ TEST SUBTRACT_MONTHS_FROM_TODAY ------

@patch("van311.utils.dates.date")
def test_subtract_months_from_today_rollover(mock_date):
    mock_date.today.return_value = date(2025, 1, 15)

    month, year = subtract_months_from_today(1)

    assert month == 12, "Should roll back to December"
    assert year == 2024, "Should roll back to 2024"


@patch("van311.utils.dates.date")
def test_subtract_months_from_today_large_n(mock_date):
    mock_date.today.return_value = date(2025, 5, 15)

    month, year = subtract_months_from_today(13)

    assert month == 4, "Should be April"
    assert year == 2024, "Should be 2024"


# ------ TEST CALCULATE_DAY_DIFFERENCE ------

def test_calculate_day_difference_basic():
    OPEN_TIMESTAMP = "2025-08-25T15:03:00+00:00"
    CLOSE_DATE = "2025-08-27"

    days_elapsed = calculate_day_difference(OPEN_TIMESTAMP, CLOSE_DATE)
    assert days_elapsed == 2

def test_calculate_day_difference_same_day():
    OPEN_TIMESTAMP = "2025-08-25T15:03:00+00:00"
    CLOSE_DATE = "2025-08-25"

    days_elapsed = calculate_day_difference(OPEN_TIMESTAMP, CLOSE_DATE)
    assert days_elapsed == 0

def test_calculate_day_difference_previous_day():

    OPEN_TIMESTAMP = "2025-08-25T15:03:00+00:00"
    CLOSE_DATE = "2025-08-24"

    days_elapsed = calculate_day_difference(OPEN_TIMESTAMP, CLOSE_DATE)
    assert days_elapsed == 0 # should return 0 due to api logging inconsistencies


# ------ TEST TIMESTAMP_TO_DATE_OBJ ------

def test_timestamp_to_test_obj_basic():
    TEST_TIMESTAMP = "2025-08-25T15:03:00+00:00"
    response = timestamp_to_date_obj(TEST_TIMESTAMP)

    assert response == datetime(year=2025, month=8, day=25, hour=15, minute=3, second=0)


# ------ TEST CALCULATE_TIMESTAMP_DIFFERENCE ------

def test_calculate_timestamp_difference_basic():
    OPEN_TIMESTAMP = "2025-09-01T00:42:00+00:00"
    MOD_DATE = "2025-09-05T16:35:08+00:00"

    days_elapsed = calculate_timestamp_difference(OPEN_TIMESTAMP, MOD_DATE)

    assert days_elapsed == 4.66

def test_calculate_timestamp_difference_same_timestamp():
    OPEN_TIMESTAMP = "2025-09-01T00:42:00+00:00"
    MOD_DATE = "2025-09-01T00:42:00+00:00"

    days_elapsed = calculate_timestamp_difference(OPEN_TIMESTAMP, MOD_DATE)

    assert days_elapsed == 0

def test_calculate_timestamp_difference_negative():
    OPEN_TIMESTAMP = "2025-09-05T16:35:08+00:00"
    MOD_DATE = "2025-09-01T00:42:00+00:00"

    days_elapsed = calculate_timestamp_difference(OPEN_TIMESTAMP, MOD_DATE)

    assert days_elapsed == -4.66
