from datetime import timedelta

from typing import BinaryIO, Optional


class Blob(object):

    name: str

    def download_to_filename(self, path: str) -> None: ...

    def download_to_file(self, fp: BinaryIO) -> None: ...

    def upload_from_filename(self, path: str) -> None: ...

    def upload_from_file(self, fp: BinaryIO) -> None: ...

    def delete(self) -> None: ...

    def generate_signed_url(
        self, expiration: Optional[timedelta] = None,
        response_disposition: Optional[str] = None) -> str: ...
