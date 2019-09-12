from typing import Any, BinaryIO, Dict, Generator, Iterable, List, Optional
from typing import overload, Tuple, Union

from keystoneauth1.session import Session


ObjectContents = Union[str, BinaryIO, Iterable[str]]


class Connection(object):

    def __init__(
        self,
        authurl: Optional[str] = None,
        user: Optional[str] = None,
        key: Optional[str] = None,
        retries: int = 5,
        preauthurl: Optional[str] = None,
        preauthtoken: Optional[str] = None,
        snet: bool = False,
        starting_backoff: int = 1,
        max_backoff: int = 64,
        tenant_name: Optional[str] = None,
        os_options: Optional[Dict[str, Any]] = None,
        auth_version: str = '1',
        cacert: Optional[str] = None,
        insecure: bool = False,
        cert: Optional[str] = None,
        cert_key: Optional[str] = None,
        ssl_compression: bool = True,
        retry_on_ratelimit: bool = False,
        timeout: Optional[int] = None,
        session: Optional[Session] = None,
        force_auth_retry: bool = False) -> None: ...

    @overload
    def get_object(
        self,
        container: str,
        obj: str,
        resp_chunk_size: None = None,
        query_string: Optional[str] = None,
        response_dict: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
        ) -> Tuple[Dict[str, str], bytes]: ...

    @overload
    def get_object(
        self,
        container: str,
        obj: str,
        resp_chunk_size: int,
        query_string: Optional[str] = None,
        response_dict: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
        ) -> Tuple[Dict[str, str], Generator[bytes, None, None]]: ...

    def put_object(
        self,
        container: str,
        obj: str,
        contents: Optional[ObjectContents],
        content_length: Optional[int] = None,
        etag: Optional[str] = None,
        chunk_size: Optional[int] = None,
        content_type: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        query_string: Optional[str] = None,
        response_dict: Optional[Dict[str, Any]] = None) -> str: ...

    def delete_object(
        self,
        container: str,
        obj: str,
        query_string: Optional[str] = None,
        response_dict: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None) -> None: ...

    def get_service_auth(self) -> Tuple[str, Dict[str, str]]: ...

    def get_auth(self) -> Tuple[str, str]: ...

    def get_container(
        self,
        container: str,
        marker: Optional[str] = None,
        limit: Optional[str] = None,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        end_marker: Optional[str] = None,
        path: Optional[str] = None,
        full_listing: bool = False,
        headers: Optional[Dict[str, str]] = None,
        query_string: Optional[str] = None
        ) -> Tuple[Dict[str, str], List[Dict[str, str]]]: ...

    def head_account(self, headers: Optional[Dict[str, str]] = None) -> Dict[str, str]: ...
