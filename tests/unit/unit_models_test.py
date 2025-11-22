from van311.models import ServiceRequest


def test_service_request_sanitation():
    pass


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
