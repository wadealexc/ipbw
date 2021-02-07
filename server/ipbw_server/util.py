import json
from ipaddress import ip_address
from typing import List

from fastapi import HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader

API_HEADER = APIKeyHeader(name="Authorization", auto_error=False)

with open("keys.json") as f:
    API_KEYS = json.load(f)


def remove_port_from_ips(ip_list: List[str]) -> List[str]:
    ips = []
    for endpoint in ip_list:
        ip, _ = endpoint.rsplit(":", 1)
        ips.append(ip)
    return ips


def validate_api_key(api_header: str = Security(API_HEADER)):
    if api_header not in API_KEYS.values():
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="nuh-uh")


def remove_private_ips(ip_list: List[str]) -> List[str]:
    filtered_ips = []
    for ip in ip_list:
        try:
            parsed_ip = ip_address(ip)
            if parsed_ip.is_private:
                continue
            filtered_ips.append(ip)
        except ValueError:
            # invalid IPv4/IPv6 address
            continue
    return filtered_ips
