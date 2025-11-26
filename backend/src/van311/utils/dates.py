from datetime import date

from dateutil.relativedelta import relativedelta


def subtract_months_from_today(n):
    dt = date.today() - relativedelta(months=n)
    month = dt.month
    year = dt.year
    return (month, year)
