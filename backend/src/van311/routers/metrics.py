from fastapi import APIRouter

from van311.db.core import get_db_connection
from van311.db.router_queries import (
    retrieve_area,
    retrieve_area_issue,
    retrieve_area_list,
    retrieve_citywide,
    retrieve_issue,
    retrieve_issue_list,
    retrieve_null,
)
from van311.models.api_models import (
    AreaIssueResponse,
    AreaResponse,
    CitywideResponse,
    IssueResponse,
    ListResponse,
    NullResponse,
)

SUCCESS_MESSAGE = "Metrics successfully retrieved"

router = APIRouter()


@router.get("/metrics/")
def metrics():
    return {"Welcome to the metrics page!"}


@router.get("/metrics/citywide", response_model=CitywideResponse)
def get_metrics_citywide():
    con = get_db_connection()
    return CitywideResponse(message=SUCCESS_MESSAGE, data=retrieve_citywide(con))


@router.get("/metrics/areas", response_model=ListResponse)
def list_metrics_neighbourhoods():
    con = get_db_connection()
    return ListResponse(message=SUCCESS_MESSAGE, data=retrieve_area_list(con))


@router.get("/metrics/areas/{local_area}", response_model=AreaResponse)
def get_metrics_neighbourhood(local_area: str):
    con = get_db_connection()
    return AreaResponse(message=SUCCESS_MESSAGE, data=retrieve_area(con, local_area))


@router.get("/metrics/issues", response_model=ListResponse)
def list_metrics_issues():
    con = get_db_connection()
    return ListResponse(message=SUCCESS_MESSAGE, data=retrieve_issue_list(con))


@router.get("/metrics/issues/{issue_type}", response_model=IssueResponse)
def get_metrics_issue(issue_type: str):
    con = get_db_connection()
    return IssueResponse(message=SUCCESS_MESSAGE, data=retrieve_issue(con, issue_type))


@router.get(
    "/metrics/areas/{local_area}/issues/{issue_type}", response_model=AreaIssueResponse
)
def get_metrics_neighbourhood_issue(local_area: str, issue_type: str):
    con = get_db_connection()
    return AreaIssueResponse(
        message=SUCCESS_MESSAGE, data=retrieve_area_issue(con, local_area, issue_type)
    )


@router.get("/metrics/null", response_model=NullResponse)
def metrics_null():
    con = get_db_connection()
    return NullResponse(message=SUCCESS_MESSAGE, data=retrieve_null(con))
