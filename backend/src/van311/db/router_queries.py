from van311.db.core import get_db_connection
from van311.models.api_models import (
    AreaIssueMetricItem,
    AreaMetricItem,
    BaseMetricItem,
    DataOpenItem,
    IssueMetricItem,
)


def retrieve_citywide(con):
    data = con.execute(
        """SELECT metric_name, metric_value FROM metric_aggregates WHERE local_area = 'CITYWIDE' AND issue_type = 'CITYWIDE'"""
    )
    rows = data.fetchall()
    metric = [BaseMetricItem(**row) for row in rows]
    return metric


def retrieve_area(con, area):
    data = con.execute(
        f"""SELECT issue_type, metric_name, metric_value FROM metric_aggregates WHERE local_area = '{area}' AND issue_type = 'CITYWIDE'"""
    )
    rows = data.fetchall()
    metric = [AreaMetricItem(**row) for row in rows]
    return metric


def retrieve_issue(con, issue):
    data = con.execute(
        f"""SELECT local_area, metric_name, metric_value FROM metric_aggregates WHERE issue_type = '{issue}' AND local_area = 'CITYWIDE'"""
    )
    rows = data.fetchall()
    metric = [IssueMetricItem(**row) for row in rows]
    return metric


def retrieve_area_issue(con, area, issue):
    data = con.execute(
        f"""SELECT local_area, issue_type, metric_name, metric_value FROM metric_aggregates WHERE local_area = '{area}' AND issue_type = '{issue}'"""
    )
    rows = data.fetchall()
    metric = [AreaIssueMetricItem(**row) for row in rows]
    return metric


def retrieve_issue_list(con):
    data = con.execute(
        """SELECT issue_type FROM metric_aggregates WHERE issue_type IS NOT NULL GROUP BY issue_type"""
    )
    rows = data.fetchall()
    i_list = [row[0] for row in rows]
    return i_list


def retrieve_area_list(con):
    data = con.execute(
        """SELECT local_area FROM metric_aggregates WHERE local_area IS NOT NULL GROUP BY local_area"""
    )
    rows = data.fetchall()
    a_list = [row[0] for row in rows]
    return a_list


def retrieve_null(con):
    query = con.execute(
        """SELECT metric_value FROM metric_aggregates WHERE local_area = 'UNKNOWN_AREA'"""
    )
    data = query.fetchone()
    return int(data[0])


def retrieve_open(con):
    query = con.execute(
        """SELECT department, local_area, issue_type, open_ts, modified_ts, address, channel,
        long, lat, time_to_update FROM service_requests WHERE close_ts IS NULL"""
    )
    rows = query.fetchall()
    # data = [DataOpenItem(**row) for row in rows]
    # return data
    return [DataOpenItem(**row) for row in rows]


if __name__ == "__main__":
    con = get_db_connection()
    print(retrieve_open(con))
