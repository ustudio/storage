from typing import Optional


class ClientException(Exception):
    # Since underlying library doesn't provide this attribute,
    # we're "mixing it in" dynamically later.
    do_not_retry: Optional[bool]
    http_status: int

    pass
