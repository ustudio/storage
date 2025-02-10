# this is actually a very dynamic class instance with dynamic attributes
# performing underlying calls against the configured services, but we're
# stubbing it out here for S3 for simplicity's sake.


from io import BytesIO
from typing import BinaryIO, Dict, List, Optional
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
    "ListResponse", {
        "Contents": List[ListObjectResponse],
        "IsTruncated": bool,
        "NextMarker": Optional[str]
    })

DeleteEntries = TypedDict(
    "DeleteEntries", {"Objects": List[Dict[str, Optional[str]]]})

ParamEntries = TypedDict("ParamEntries", {"Bucket": str, "Key": str})


class Session(object):

    def get_object(self, Bucket: str, Key: str) -> ObjectResponse: ...

    def list_objects(
        self,
        Bucket: str,
        Prefix: str,
        Delimiter: Optional[str] = None,
        Marker: Optional[str] = None
    ) -> ListResponse:
        ...

    def download_file(self, Bucket: str, Key: str, filepath: str) -> None: ...

    def put_object(
        self, Bucket: str, Key: str, Body: BinaryIO, ContentType: Optional[str]) -> None: ...

    def upload_file(self, Filename: str, Bucket: str, Key: str) -> None: ...

    def delete_object(self, Bucket: str, Key: str) -> ListResponse: ...

    def delete_objects(self, Bucket: str, Delete: DeleteEntries) -> None: ...

    def generate_presigned_url(
        self,
        Permission: str,
        Params: ParamEntries,
        ExpiresIn: int) -> str: ...
