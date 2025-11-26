from datetime import date, datetime, time

from dateutil.relativedelta import relativedelta


def subtract_months_from_today(n):
    dt = date.today() - relativedelta(months=n)
    month = dt.month
    year = dt.year
    return (month, year)

def calculate_day_difference(open_t: str, close_t: str):
    open_t_date = date.fromisoformat(open_t.split("T")[0])
    close_t_date = date.fromisoformat(close_t)
    days_elapsed = (close_t_date - open_t_date).days

    return 0 if days_elapsed < 0 else days_elapsed

def timestamp_to_date_obj(ts):
    time_split = ts.split("T")
    ts_date = date.fromisoformat(time_split[0])
    ts_time = time.fromisoformat(time_split[1].split("+")[0])
    return datetime.combine(ts_date, ts_time)

def calculate_timestamp_difference(open_t: str, mod_t: str):
    open_time = timestamp_to_date_obj(open_t)
    mod_time = timestamp_to_date_obj(mod_t)

    time_difference = (mod_time - open_time)
    days_elapsed: float = time_difference.days + time_difference.seconds / 86400

    return float(days_elapsed.__format__(".2f"))

if __name__ == "__main__":
    pass
