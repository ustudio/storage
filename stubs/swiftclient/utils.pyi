from typing import Optional


def generate_temp_url(
    path: str,
    seconds: int,
    key: str,
    method: str,
    absolute: bool = False,
    prefix: bool = False,
    iso8601: bool = False,
    ip_range: Optional[str] = None) -> str: ...
