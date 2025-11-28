from datetime import datetime

from tqdm import tqdm

from .core import get_db_connection

FLAG_CITYWIDE = "CITYWIDE"
FLAG_UNKNOWN_AREA = "UNKNOWN_AREA"
FLAG_UNKNOWN_ISSUE = "UNKNOWN_ISSUE"
METRICS_INTERVAL_MINUTES = 20
GRACE_TIME_SECONDS = 10


# ------ HELPER FUNCTIONS ------


def create_db_metrics_table(con):
    con.execute(
        """CREATE TABLE IF NOT EXISTS metric_aggregates(metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
        local_area TEXT, issue_type TEXT, metric_name TEXT, metric_value REAL, calculated_at TEXT)"""
    )


def drop_db_metrics_table(con):
    con.execute("""DROP TABLE IF EXISTS metric_aggregates""")


def sql_execute(con, input):
    con.execute(
        """INSERT INTO metric_aggregates (local_area, issue_type, metric_name, metric_value,
        calculated_at) VALUES (?, ?, ?, ?, ?)""",
        input,
    )


def insert_row(con, row, metric_name: str, metric_key: str):
    input = (
        row["local_area"],
        row["issue_type"],
        metric_name,
        row[metric_key],
        str(datetime.now()),
    )
    sql_execute(con, input)


def insert_row_issue(con, row, metric_name: str, metric_key: str):
    input = (
        FLAG_CITYWIDE,
        row["issue_type"],
        metric_name,
        row[metric_key],
        str(datetime.now()),
    )
    sql_execute(con, input)


def insert_row_neighbourhood(con, row, metric_name: str, metric_key: str):
    input = (
        row["local_area"],
        FLAG_CITYWIDE,
        metric_name,
        row[metric_key],
        str(datetime.now()),
    )
    sql_execute(con, input)


def insert_row_citywide(con, metric_name: str, value):
    input = (FLAG_CITYWIDE, FLAG_CITYWIDE, metric_name, value, str(datetime.now()))
    sql_execute(con, input)


def insert_row_null(con, metric_name: str, value):
    input = (
        FLAG_UNKNOWN_AREA,
        FLAG_UNKNOWN_ISSUE,
        metric_name,
        value,
        str(datetime.now()),
    )
    sql_execute(con, input)


# ------ METRICS BY NEIGHBOURHOOD AND ISSUE TYPE ------


def get_ni_average_resolution_time(con):
    rows = con.execute(
        """SELECT local_area, issue_type, AVG(time_to_resolve) as avg_ttr FROM service_requests
        WHERE time_to_resolve IS NOT NULL AND local_area IS NOT NULL
        AND issue_type IS NOT NULL GROUP BY local_area, issue_type"""
    )

    for row in rows:
        insert_row(con, row, "ni_avg_ttr", "avg_ttr")


def get_ni_average_update_time(con):
    rows = con.execute(
        """SELECT local_area, issue_type, AVG(time_to_update) as avg_ttu FROM service_requests
        WHERE time_to_update IS NOT NULL AND local_area IS NOT NULL
        AND issue_type IS NOT NULL GROUP BY local_area, issue_type"""
    )

    for row in rows:
        insert_row(con, row, "ni_avg_ttu", "avg_ttu")


def get_ni_volume(con):
    rows = con.execute(
        """SELECT local_area, issue_type, COUNT(*) as count FROM service_requests
        WHERE local_area IS NOT NULL AND issue_type IS NOT NULL
        GROUP BY local_area, issue_type"""
    )

    for row in rows:
        insert_row(con, row, "ni_volume", "count")


def get_ni_open_requests(con):
    rows = con.execute(
        """SELECT local_area, issue_type, COUNT(*) as count FROM service_requests
        WHERE close_ts IS NULL AND local_area IS NOT NULL
        AND issue_type IS NOT NULL GROUP BY local_area, issue_type"""
    )

    for row in rows:
        insert_row(con, row, "ni_open_requests", "count")


# ------ METRICS BY ISSUE ------


def get_issue_average_resolution_time(con):
    rows = con.execute(
        """SELECT issue_type, AVG(time_to_resolve) as avg_ttr FROM service_requests
        WHERE time_to_resolve IS NOT NULL AND issue_type IS NOT NULL GROUP BY issue_type"""
    )

    for row in rows:
        insert_row_issue(con, row, "issue_avg_ttr", "avg_ttr")


def get_issue_average_update_time(con):
    rows = con.execute(
        """SELECT issue_type, AVG(time_to_update) as avg_ttu FROM service_requests
        WHERE time_to_update IS NOT NULL AND issue_type IS NOT NULL GROUP BY issue_type"""
    )

    for row in rows:
        insert_row_issue(con, row, "issue_avg_ttu", "avg_ttu")


def get_issue_volume(con):
    rows = con.execute(
        """SELECT issue_type, COUNT(*) as count FROM service_requests
        WHERE issue_type IS NOT NULL GROUP BY issue_type"""
    )

    for row in rows:
        insert_row_issue(con, row, "issue_volume", "count")


def get_issue_open_requests(con):
    rows = con.execute(
        """SELECT issue_type, COUNT(*) as count FROM service_requests
        WHERE issue_type IS NOT NULL AND close_ts IS NULL GROUP BY issue_type"""
    )

    for row in rows:
        insert_row_issue(con, row, "issue_open_requests", "count")


# ------ METRICS BY NEIGHBOURHOOD ------


def get_neighbourhood_average_resolution_time(con):
    rows = con.execute(
        """SELECT local_area, AVG(time_to_resolve) as avg_ttr FROM service_requests
        WHERE time_to_resolve IS NOT NULL AND local_area IS NOT NULL GROUP BY local_area"""
    )

    for row in rows:
        insert_row_neighbourhood(con, row, "neighbourhood_avg_ttr", "avg_ttr")


def get_neighbourhood_average_update_time(con):
    rows = con.execute(
        """SELECT local_area, AVG(time_to_update) as avg_ttu FROM service_requests
        WHERE time_to_update IS NOT NULL AND local_area IS NOT NULL GROUP BY local_area"""
    )

    for row in rows:
        insert_row_neighbourhood(con, row, "neighbourhood_avg_ttu", "avg_ttu")


def get_neighbourhood_volume(con):
    rows = con.execute(
        """SELECT local_area, COUNT(*) as count FROM service_requests
        WHERE local_area IS NOT NULL GROUP BY local_area"""
    )

    for row in rows:
        insert_row_neighbourhood(con, row, "neighbourhood_volume", "count")


def get_neighbourhood_open_requests(con):
    rows = con.execute(
        """SELECT local_area, COUNT(*) as count FROM service_requests
        WHERE close_ts IS NULL AND local_area IS NOT NULL GROUP BY local_area"""
    )

    for row in rows:
        insert_row_neighbourhood(con, row, "neighbourhood_open_requests", "count")


# ------ CITY WIDE METRICS ------


def get_citywide_average_resolution_time(con):
    query = con.execute(
        """SELECT AVG(time_to_resolve) as avg_ttr FROM service_requests
        WHERE time_to_resolve IS NOT NULL"""
    )

    average = query.fetchone()["avg_ttr"]
    insert_row_citywide(con, "citywide_avg_ttr", average)


def get_citywide_average_update_time(con):
    query = con.execute(
        """SELECT AVG(time_to_update) as avg_ttu FROM service_requests
        WHERE time_to_update IS NOT NULL"""
    )

    average = query.fetchone()["avg_ttu"]
    insert_row_citywide(con, "citywide_avg_ttu", average)


def get_citywide_volume(con):
    query = con.execute("""SELECT COUNT(*) as count FROM service_requests""")

    count = query.fetchone()["count"]
    insert_row_citywide(con, "citywide_volume", count)


def get_citywide_open_requests(con):
    query = con.execute(
        """SELECT COUNT(*) as count FROM service_requests
        WHERE close_ts IS NULL"""
    )

    count = query.fetchone()["count"]
    insert_row_citywide(con, "citywide_open_requests", count)


# ------ NULL METRICS ------


def get_null_neighbourhood_volume(con):
    query = con.execute(
        """SELECT COUNT(*) as count FROM service_requests WHERE local_area IS NULL"""
    )

    count = query.fetchone()["count"]
    insert_row_null(con, "null_neighbourhood_volume", count)


# ------ CALCULATE METRICS ------


def calculate_null_metrics(con, pbar):
    get_null_neighbourhood_volume(con)
    pbar.update(1)


def calculate_city_wide_metrics(con, pbar):
    get_citywide_average_resolution_time(con)
    pbar.update(1)
    get_citywide_average_update_time(con)
    pbar.update(1)
    get_citywide_volume(con)
    pbar.update(1)
    get_citywide_open_requests(con)
    pbar.update(1)


def calculate_issue_metrics(con, pbar):
    get_issue_average_resolution_time(con)
    pbar.update(1)
    get_issue_average_update_time(con)
    pbar.update(1)
    get_issue_volume(con)
    pbar.update(1)
    get_issue_open_requests(con)
    pbar.update(1)


def calculate_neighbourhood_metrics(con, pbar):
    get_neighbourhood_average_resolution_time(con)
    pbar.update(1)
    get_neighbourhood_average_update_time(con)
    pbar.update(1)
    get_neighbourhood_volume(con)
    pbar.update(1)
    get_neighbourhood_open_requests(con)
    pbar.update(1)


def calculate_neighbourhood_issue_metrics(con, pbar):
    get_ni_average_resolution_time(con)
    pbar.update(1)
    get_ni_average_update_time(con)
    pbar.update(1)
    get_ni_volume(con)
    pbar.update(1)
    get_ni_open_requests(con)
    pbar.update(1)


def calculate_metrics():
    try:
        with (
            get_db_connection() as con,
            tqdm(total=17, desc="😋 Scheduled metrics calculations") as pbar,
        ):
            drop_db_metrics_table(con)
            create_db_metrics_table(con)

            calculate_null_metrics(con, pbar)
            calculate_city_wide_metrics(con, pbar)
            calculate_neighbourhood_metrics(con, pbar)
            calculate_issue_metrics(con, pbar)
            calculate_neighbourhood_issue_metrics(con, pbar)

            con.commit()
            print(
                f"------ Finished calculating metrics. Sleeping for {METRICS_INTERVAL_MINUTES} minutes  ------"
            )

    except Exception as e:
        print(
            f"CRITICAL ERROR during update: {e}.\n ------ Retrying in {GRACE_TIME_SECONDS} ------"
        )


if __name__ == "__main__":
    calculate_metrics()
