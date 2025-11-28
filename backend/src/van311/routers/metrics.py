from fastapi import APIRouter

from ..db.core import get_db_connection
from ..db.router_queries import (
    retrieve_area,
    retrieve_area_issue,
    retrieve_areas_all,
    retrieve_citywide,
    retrieve_issue,
    retrieve_issues_all,
)
from ..models.api_models import (
    AreaIssueResponse,
    AreaResponse,
    BaseResponse,
    IssueResponse,
)

SUCCESS_MESSAGE = "Metrics successfully retrieved"

router = APIRouter()


@router.get("/metrics/")
def metrics():
    return {"Welcome to the metrics page!"}


@router.get("/metrics/citywide", response_model=BaseResponse)
def get_metrics_citywide():
    with get_db_connection() as con:
        return BaseResponse(message=SUCCESS_MESSAGE, data=retrieve_citywide(con))


@router.get("/metrics/areas", response_model=AreaResponse)
def get_metrics_neighbourhoods():
    with get_db_connection() as con:
        return AreaResponse(message=SUCCESS_MESSAGE, data=retrieve_areas_all(con))


@router.get("/metrics/areas/{local_area}", response_model=BaseResponse)
def get_metrics_neighbourhood(local_area: str):
    with get_db_connection() as con:
        return BaseResponse(
            message=SUCCESS_MESSAGE, data=retrieve_area(con, local_area)
        )


@router.get("/metrics/issues", response_model=IssueResponse)
def list_metrics_issues():
    with get_db_connection() as con:
        return IssueResponse(message=SUCCESS_MESSAGE, data=retrieve_issues_all(con))


@router.get("/metrics/issues/{issue_type}", response_model=BaseResponse)
def get_metrics_issue(issue_type: str):
    with get_db_connection() as con:
        return BaseResponse(
            message=SUCCESS_MESSAGE, data=retrieve_issue(con, issue_type)
        )


@router.get(
    "/metrics/areas/{local_area}/issues/{issue_type}", response_model=AreaIssueResponse
)
def get_metrics_neighbourhood_issue(local_area: str, issue_type: str):
    with get_db_connection() as con:
        return AreaIssueResponse(
            message=SUCCESS_MESSAGE,
            data=retrieve_area_issue(con, local_area, issue_type),
        )
