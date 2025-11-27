from unittest.mock import Mock

from van311.db.metrics import (
    calculate_city_wide_metrics,
    get_citywide_average_resolution_time,
    get_citywide_average_update_time,
    get_citywide_open_requests,
    get_citywide_volume,
    get_issue_average_resolution_time,
    get_issue_average_update_time,
    get_issue_open_requests,
    get_issue_volume,
    get_neighbourhood_average_resolution_time,
    get_neighbourhood_average_update_time,
    get_neighbourhood_open_requests,
    get_neighbourhood_volume,
    get_ni_average_resolution_time,
    get_ni_average_update_time,
    get_ni_open_requests,
    get_ni_volume,
    get_null_neighbourhood_volume,
)

# ------ CITYWIDE TESTS ------


def test_citywide_average_resolution_time(db_metrics_connection):
    con = db_metrics_connection
    get_citywide_average_resolution_time(con)

    data = con.execute("""SELECT metric_value FROM metric_aggregates""")
    row = data.fetchone()

    assert row[0] == 2.75


def test_citywide_average_update_time(db_metrics_connection):
    con = db_metrics_connection
    get_citywide_average_update_time(con)

    data = con.execute("""SELECT metric_value FROM metric_aggregates""")
    row = data.fetchone()

    assert row[0] == 1.6


def test_citywide_open_requests(db_metrics_connection):
    con = db_metrics_connection
    get_citywide_open_requests(con)

    data = con.execute("""SELECT metric_value FROM metric_aggregates""")
    row = data.fetchone()

    assert row[0] == 1


def test_citywide_volume(db_metrics_connection):
    con = db_metrics_connection
    get_citywide_volume(con)

    data = con.execute("""SELECT metric_value FROM metric_aggregates""")
    row = data.fetchone()

    assert row[0] == 5


# ------ ISSUE TEST ------


def test_issue_average_resolution_time(db_metrics_connection):
    con = db_metrics_connection
    get_issue_average_resolution_time(con)

    data = con.execute("""SELECT issue_type, metric_value FROM metric_aggregates""")
    rows = data.fetchall()
    metrics = {row["issue_type"]: row["metric_value"] for row in rows}

    assert len(metrics) == 3
    assert metrics["Graffiti"] == 4
    assert metrics["Other"] == 1
    assert metrics["Pothole"] == 3


def test_issue_average_update_time(db_metrics_connection):
    con = db_metrics_connection
    get_issue_average_update_time(con)

    data = con.execute("""SELECT issue_type, metric_value FROM metric_aggregates""")
    rows = data.fetchall()
    metrics = {row["issue_type"]: row["metric_value"] for row in rows}

    assert len(metrics) == 3
    assert metrics["Graffiti"] == 2
    assert metrics["Other"] == 1
    assert metrics["Pothole"] == 1.5


def test_issue_open_requests(db_metrics_connection):
    con = db_metrics_connection
    get_issue_open_requests(con)

    data = con.execute("""SELECT issue_type, metric_value FROM metric_aggregates""")
    rows = data.fetchall()
    metrics = {row["issue_type"]: row["metric_value"] for row in rows}

    assert len(metrics) == 1
    assert metrics["Graffiti"] == 1


def test_issue_volume(db_metrics_connection):
    con = db_metrics_connection
    get_issue_volume(con)

    data = con.execute("""SELECT issue_type, metric_value FROM metric_aggregates""")
    rows = data.fetchall()
    metrics = {row["issue_type"]: row["metric_value"] for row in rows}

    assert len(metrics) == 3
    assert metrics["Graffiti"] == 2
    assert metrics["Other"] == 1
    assert metrics["Pothole"] == 2


# ------ NEIGHBOURHOOD TEST ------


def test_neighbourhood_average_resolution_time(db_metrics_connection):
    con = db_metrics_connection
    get_neighbourhood_average_resolution_time(con)

    data = con.execute("""SELECT local_area, metric_value FROM metric_aggregates""")
    rows = data.fetchall()
    metrics = {row["local_area"]: row["metric_value"] for row in rows}

    assert len(metrics) == 2
    assert metrics["Kitsilano"] == 4
    assert metrics["Downtown"] == 2


def test_neighbourhood_average_update_time(db_metrics_connection):
    con = db_metrics_connection
    get_neighbourhood_average_update_time(con)

    data = con.execute("""SELECT local_area, metric_value FROM metric_aggregates""")
    rows = data.fetchall()
    metrics = {row["local_area"]: row["metric_value"] for row in rows}

    assert len(metrics) == 2
    assert metrics["Kitsilano"] == 2
    assert metrics["Downtown"] == 1.5


def test_neighbourhood_open_requests(db_metrics_connection):
    con = db_metrics_connection
    get_neighbourhood_open_requests(con)

    data = con.execute("""SELECT local_area, metric_value FROM metric_aggregates""")
    rows = data.fetchall()
    metrics = {row["local_area"]: row["metric_value"] for row in rows}

    assert len(metrics) == 1
    assert metrics["Downtown"] == 1


def test_neighbourhood_volume(db_metrics_connection):
    con = db_metrics_connection
    get_neighbourhood_volume(con)

    data = con.execute("""SELECT local_area, metric_value FROM metric_aggregates""")
    rows = data.fetchall()
    metrics = {row["local_area"]: row["metric_value"] for row in rows}

    assert len(metrics) == 2
    assert metrics["Kitsilano"] == 2
    assert metrics["Downtown"] == 2


# # ------ ISSUE NEIGHBOURHOOD TEST ------


def test_ni_average_resolution_time(db_metrics_connection):
    con = db_metrics_connection
    get_ni_average_resolution_time(con)

    data = con.execute(
        """SELECT local_area, issue_type, metric_value FROM metric_aggregates"""
    )
    rows = data.fetchall()
    metrics = {
        (row["local_area"], row["issue_type"]): row["metric_value"] for row in rows
    }

    assert len(metrics) == 3
    assert metrics[("Kitsilano", "Graffiti")] == 4
    assert metrics[("Kitsilano", "Pothole")] == 4
    assert metrics[("Downtown", "Pothole")] == 2


def test_ni_average_update_time(db_metrics_connection):
    con = db_metrics_connection
    get_ni_average_update_time(con)

    data = con.execute(
        """SELECT local_area, issue_type, metric_value FROM metric_aggregates"""
    )
    rows = data.fetchall()
    metrics = {
        (row["local_area"], row["issue_type"]): row["metric_value"] for row in rows
    }

    assert len(metrics) == 4
    assert metrics[("Kitsilano", "Graffiti")] == 2
    assert metrics[("Kitsilano", "Pothole")] == 2
    assert metrics[("Downtown", "Graffiti")] == 2
    assert metrics[("Downtown", "Pothole")] == 1


def test_ni_open_requests(db_metrics_connection):
    con = db_metrics_connection
    get_ni_open_requests(con)

    data = con.execute(
        """SELECT local_area, issue_type, metric_value FROM metric_aggregates"""
    )
    rows = data.fetchall()
    metrics = {
        (row["local_area"], row["issue_type"]): row["metric_value"] for row in rows
    }

    assert len(metrics) == 1
    assert metrics[("Downtown", "Graffiti")] == 1


def test_ni_volume(db_metrics_connection):
    con = db_metrics_connection
    get_ni_volume(con)

    data = con.execute(
        """SELECT local_area, issue_type, metric_value FROM metric_aggregates"""
    )
    rows = data.fetchall()
    metrics = {
        (row["local_area"], row["issue_type"]): row["metric_value"] for row in rows
    }

    assert len(metrics) == 4
    assert metrics[("Kitsilano", "Graffiti")] == 1
    assert metrics[("Kitsilano", "Pothole")] == 1
    assert metrics[("Downtown", "Graffiti")] == 1
    assert metrics[("Downtown", "Pothole")] == 1


# # ------ NULL TEST ------


def test_null_neighbourhood_volume(db_metrics_connection):
    con = db_metrics_connection
    get_null_neighbourhood_volume(con)

    data = con.execute("""SELECT metric_value FROM metric_aggregates""")
    row = data.fetchone()

    assert row[0] == 1
