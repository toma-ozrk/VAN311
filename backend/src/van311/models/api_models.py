from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel

# ------ METRIC MODELS ------


class BaseMetricItem(BaseModel):
    metric_name: str
    metric_value: float


class AreaMetricItem(BaseMetricItem):
    issue_type: str


class IssueMetricItem(BaseMetricItem):
    local_area: str


class AreaIssueMetricItem(BaseMetricItem):
    local_area: str
    issue_type: str


class DataOpenItem(BaseModel):
    department: Optional[str]
    local_area: Optional[str]
    issue_type: Optional[str]
    open_ts: Optional[str]
    modified_ts: Optional[str]
    address: Optional[str]
    channel: Optional[str]
    long: Optional[str]
    lat: Optional[str]
    time_to_update: Optional[float]


# ------ RESPONSE MODELS ------


class CitywideResponse(BaseModel):
    message: str
    timestamp: str = str(datetime.now())
    data: List[BaseMetricItem]


class AreaResponse(BaseModel):
    message: str
    timestamp: str = str(datetime.now())
    data: List[AreaMetricItem]


class IssueResponse(BaseModel):
    message: str
    timestamp: str = str(datetime.now())
    data: List[IssueMetricItem]


class AreaIssueResponse(BaseModel):
    message: str
    timestamp: str = str(datetime.now())
    data: List[AreaIssueMetricItem]


class ListResponse(BaseModel):
    message: str
    timestamp: str = str(datetime.now())
    data: list


class NullResponse(BaseModel):
    message: str
    timestamp: str = str(datetime.now())
    data: int


class DataOpenResponse(BaseModel):
    message: str
    timestamp: str = str(datetime.now())
    data: List[DataOpenItem]
