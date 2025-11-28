from fastapi import APIRouter

from van311.db.core import get_db_connection
from van311.db.router_queries import retrieve_open
from van311.models.api_models import DataOpenResponse

SUCCESS_MESSAGE = "Successfully retrieved data"

router = APIRouter()


@router.get("/data/")
def data():
    return {"message": "data :D"}


@router.get("/data/open-requests", response_model=DataOpenResponse)
def get_open_requests():
    with get_db_connection() as con:
        return DataOpenResponse(message=SUCCESS_MESSAGE, data=retrieve_open(con))
