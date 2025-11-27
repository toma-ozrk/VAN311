from fastapi import APIRouter

router = APIRouter()


@router.get("/metrics/")
def metrics():
    return {"Welcome to the metrics page!"}
