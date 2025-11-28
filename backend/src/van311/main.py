from fastapi import FastAPI

from .routers.data import router as data_router
from .routers.metrics import router as metric_router

app = FastAPI()
app.include_router(metric_router)
app.include_router(data_router)


@app.get("/")
def read_root():
    return {"Hello": "World"}
