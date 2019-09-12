from typing import Any, Optional
from botocore.session import Session


# these three are actually in s3transfer module, but they are just pure
# type stubs for now since we aren't using them...

class TransferConfig(object): ...

class OSUtils(object): ...

class TransferManager(object): ...


class S3Transfer(object):
    def __init__(
        self,
        client: Optional[Session] = None,
        config: Optional[TransferConfig] = None,
        osutil: Optional[OSUtils] = None,
        manager: Optional[TransferManager] = None) -> None: ...

    def download_file(
        self,
        bucket: str,
        key: str,
        filename: str,
        *args: Any,
        **kwargs: Any) -> None: ...

    def upload_file(
        self,
        filename: str,
        bucket: str,
        key: str,
        *args: Any,
        **kwargs: Any) -> None: ...
