import hashlib
from dataclasses import dataclass


@dataclass
class ServiceRequest:
    department: str
    issue_type: str
    status: str
    closure_reason: str
    open_ts: str
    close_ts: str
    last_modified: str
    address: str
    local_area: str
    channel: str
    lat: str
    lon: str
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

        return cls(
            department=request.get("department", ""),
            issue_type=request.get("service_request_type", ""),
            status=request.get("status", ""),
            closure_reason=request.get("closure_reason", ""),
            open_ts=request.get("service_request_open_timestamp", ""),
            close_ts=request.get("service_request_close_date", ""),
            last_modified=request.get("last_modified_timestamp", ""),
            address=request.get("address", ""),
            local_area=request.get("local_area", ""),
            channel=request.get("channel", ""),
            lat=request.get("latitude", ""),
            lon=request.get("longitude", ""),
            id=unique_id,
        )
