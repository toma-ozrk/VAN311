from fastapi import APIRouter

from ..db.core import get_db_connection
from ..db.router_queries import (
    retrieve_area_list,
    retrieve_issue_list,
    retrieve_null,
    retrieve_open,
)
from ..models.api_models import DataOpenResponse, ListResponse, NullResponse

SUCCESS_MESSAGE = "Successfully retrieved data"

router = APIRouter()


@router.get("/data/")
def data():
    return {"message": "data :D"}


@router.get("/data/areas", response_model=ListResponse)
def get_areas():
    with get_db_connection() as con:
        return ListResponse(message=SUCCESS_MESSAGE, data=retrieve_area_list(con))


@router.get("/data/issues", response_model=ListResponse)
def get_issues():
    with get_db_connection() as con:
        return ListResponse(message=SUCCESS_MESSAGE, data=retrieve_issue_list(con))


@router.get("/data/open-requests", response_model=DataOpenResponse)
def get_open_requests():
    with get_db_connection() as con:
        return DataOpenResponse(message=SUCCESS_MESSAGE, data=retrieve_open(con))


@router.get("/data/null", response_model=NullResponse)
def metrics_null():
    with get_db_connection() as con:
        return NullResponse(message=SUCCESS_MESSAGE, data=retrieve_null(con))
