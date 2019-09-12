from google.cloud.storage.blob import Blob

from typing import Iterator


class Bucket(object):

    def blob(self, blob_name: str) -> Blob: ...

    def list_blobs(self, prefix: str) -> Iterator[Blob]: ...
