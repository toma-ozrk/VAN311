from van311.db.router_queries import (
    retrieve_area,
    retrieve_area_issue,
    retrieve_area_list,
    retrieve_citywide,
    retrieve_issue,
    retrieve_issue_list,
    retrieve_null,
    retrieve_open,
)


def test_retrieve_citywide(db_router_query_connection):
    x = retrieve_citywide(db_router_query_connection)
    assert len(x) == 1
    assert x[0].metric_name == "time_to_update"
    assert x[0].metric_value == 3


def test_retrieve_area(db_router_query_connection):
    x = retrieve_area(db_router_query_connection, "Kitsilano")
    assert len(x) == 1
    assert x[0].metric_name == "time_to_update"
    assert x[0].metric_value == 3


def test_retrieve_issue(db_router_query_connection):
    x = retrieve_issue(db_router_query_connection, "Pothole")
    assert len(x) == 1
    assert x[0].metric_name == "time_to_resolve"
    assert x[0].metric_value == 2


def test_retrieve_area_issue(db_router_query_connection):
    x = retrieve_area_issue(db_router_query_connection, "Kitsilano", "Pothole")
    assert len(x) == 1
    assert x[0].metric_name == "time_to_resolve"
    assert x[0].metric_value == 2


def test_retrieve_null(db_router_query_connection):
    x = retrieve_null(db_router_query_connection)
    assert x == 8


def test_retrieve_open(db_router_data_connection):
    x = retrieve_open(db_router_data_connection)
    assert len(x) == 1
    assert x[0].issue_type == "Noise on Private Property Case"


def test_retrieve_area_list(db_router_query_connection):
    x = retrieve_area_list(db_router_query_connection)
    assert len(x) == 3
    assert set(x) == {"Kitsilano", "UNKNOWN_AREA", "CITYWIDE"}


def test_retrieve_issue_list(db_router_query_connection):
    x = retrieve_issue_list(db_router_query_connection)
    assert len(x) == 3
    assert set(x) == {"Pothole", "UNKNOWN_ISSUE", "CITYWIDE"}
