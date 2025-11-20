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
