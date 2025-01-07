import botocore.config
import botocore.session

from typing import Optional, Union


class Session(object):

    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        region_name: Optional[str] = None,
        botocore_session: Optional[botocore.session.Session] = None,
        profile_name: Optional[str] = None) -> None: ...

    def client(
        self,
        service_name: str,
        region_name: Optional[str] = None,
        api_version: Optional[str] = None,
        use_ssl: bool = True,
        verify: Union[None, bool, str] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        config: Optional[botocore.config.Config] = None
        ) -> botocore.session.Session: ...
