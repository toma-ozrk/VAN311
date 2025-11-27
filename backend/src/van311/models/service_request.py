import hashlib
from dataclasses import dataclass
from typing import Optional

from van311.utils.dates import calculate_day_difference, calculate_timestamp_difference


@dataclass
class ServiceRequest:
    department: str
    issue_type: str
    status: str
    closure_reason: Optional[str]
    open_ts: str
    close_ts: str
    last_modified: str
    address: str
    local_area: str
    channel: str
    lat: str
    lon: str
    time_to_resolve: Optional[int]
    time_to_update: Optional[float]
    id: str

    @staticmethod
    def create_hash(request: dict) -> str:
        key = str(
            request.get("department", "")
            + request.get("service_request_type", "")
            + request.get("service_request_open_timestamp", "")
        )
        unique_id = (hashlib.sha256(key.encode("utf-8"))).hexdigest()
        return unique_id

    @classmethod
    def dict_to_service_request(cls, request: dict):
        unique_id = cls.create_hash(request)

        open_time: str = request.get("service_request_open_timestamp", "")
        close_time: str = request.get("service_request_close_date", "")
        mod_time: str = request.get("last_modified_timestamp", "")

        return cls(
            department=request.get("department", ""),
            issue_type=request.get("service_request_type", ""),
            status=request.get("status", ""),
            closure_reason=request.get("closure_reason", "") if close_time else None,
            open_ts=open_time,
            close_ts=close_time,
            last_modified=request.get("last_modified_timestamp", ""),
            address=request.get("address", ""),
            local_area=request.get("local_area", ""),
            channel=request.get("channel", ""),
            lat=request.get("latitude", ""),
            lon=request.get("longitude", ""),
            time_to_resolve=calculate_day_difference(open_time, close_time)
            if close_time
            else None,
            time_to_update=calculate_timestamp_difference(open_time, mod_time)
            if (open_time != mod_time)
            else None,
            id=unique_id,
        )
