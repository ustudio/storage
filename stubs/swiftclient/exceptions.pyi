from typing import Optional


class ClientException(Exception):
    do_not_retry: Optional[bool]
    http_status: int

    pass
