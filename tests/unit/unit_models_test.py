from unittest.mock import Mock, patch

from van311.models import ServiceRequest


@patch("van311.models.ServiceRequest.create_hash")
def test_service_request_sanitation(mock_hash):
    input_data = {
        "department": "DBL - Property Use Inspections",
        "service_request_type": "Noise on Private Property Case",
        "status": "Close",
        "closure_reason": "Assigned to inspector",
        "service_request_open_timestamp": "2025-08-25T15:03:00+00:00",
        "service_request_close_date": "2025-08-27",
        "last_modified_timestamp": "2025-08-27T18:56:08+00:00",
        "address": None,
        "local_area": "Marpole",
        "channel": "WEB",
        "latitude": None,
        "longitude": None,
    }

    mock_hash.return_value = (
        "7df03dffc88c54dd2f0748dc30148b77a35b64f430b27e4362a02e9027b5bcc7"
    )

    req = ServiceRequest.dict_to_service_request(input_data)
    assert req
    assert req.department == "DBL - Property Use Inspections"
    assert req.status == "Close"
    assert req.id == mock_hash.return_value


def test_hash_consistency():
    input_data = {
        "department": "jkasdlgjdklsajfa",
        "service_request_type": "fdkasdfklsaka",
        "service_request_open_timestamp": "kfdaslkdjalskjfls",
    }

    hash1 = ServiceRequest.create_hash(input_data)
    hash2 = ServiceRequest.create_hash(input_data)

    assert hash1 == hash2, "Hash must be deterministic"
    assert hash1 == "ed082740a22f98027f902d90632ad83d67e475d1919577a7f3443d0eda898bca"


def test_hash_sensitivity():
    input_data1 = {
        "department": "jkasdlgjdklsajfa",
        "service_request_type": "fdkasdfklsaka",
        "service_request_open_timestamp": "kfdaslkdjalskjfls",
    }

    input_data2 = {
        "department": "jkasdlgjdklsajfa",
        "service_request_type": "fdkasdfklsaka",
        "service_request_open_timestamp": "kfdaslkdjalskjflt",  # changed last character
    }

    hash1 = ServiceRequest.create_hash(input_data1)
    hash2 = ServiceRequest.create_hash(input_data2)

    assert hash1 != hash2, "Minor change in input must provide different hash"
