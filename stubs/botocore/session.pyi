# this is actually a very dynamic class instance with dynamic attributes
# performing underlying calls against the configured services, but we're
# stubbing it out here for S3 for simplicity's sake.


from io import BytesIO
from typing import BinaryIO, List, Optional
from mypy_extensions import TypedDict


ObjectResponse = TypedDict("ObjectResponse", {"Body": BytesIO})

ListObjectResponse = TypedDict(
    "ListObjectResponse", {
        "Key": str,
        "LastModified": str,
        "ETag": str,
        "Size": int,
        "StorageClass": str
    })

ListResponse = TypedDict(
    "ListResponse", {"Contents": List[ListObjectResponse]})

DeleteEntry = TypedDict("DeleteEntry", {"Key": Optional[str]})

DeleteEntries = TypedDict("DeleteEntries", {"Objects": List[DeleteEntry]})

ParamEntries = TypedDict("ParamEntries", {"Bucket": str, "Key": str})


class Session(object):

    def get_object(self, Bucket: str, Key: str) -> ObjectResponse: ...

    def list_objects(self, Bucket: str, Prefix: str) -> ListResponse: ...

    def download_file(self, Bucket: str, Key: str, filepath: str) -> None: ...

    def put_object(self, Bucket: str, Key: str, Body: BinaryIO) -> None: ...

    def upload_file(self, Filename: str, Bucket: str, Key: str) -> None: ...

    def delete_object(self, Bucket: str, Key: str) -> None: ...

    def delete_objects(self, Bucket: str, Delete: DeleteEntries) -> None: ...

    def generate_presigned_url(
        self,
        Permission: str,
        Params: ParamEntries,
        ExpiresIn: int) -> str: ...
