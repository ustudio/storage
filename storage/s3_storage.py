import mimetypes
import os
from urllib.parse import parse_qs, unquote

import boto3.session
import boto3.s3.transfer
from botocore.exceptions import ClientError
from botocore.session import Session

from typing import BinaryIO, Dict, Optional

from storage import retry
from storage.storage import Storage, NotFoundError, register_storage_protocol, _LARGE_CHUNK
from storage.storage import get_optional_query_parameter
from storage.url_parser import remove_user_info


@register_storage_protocol("s3")
class S3Storage(Storage):

    def __init__(self, storage_uri: str) -> None:
        super(S3Storage, self).__init__(storage_uri)
        self._access_key = unquote(self._parsed_storage_uri.username)
        self._access_secret = unquote(self._parsed_storage_uri.password)
        self._bucket = self._parsed_storage_uri.hostname
        self._keyname = self._parsed_storage_uri.path.replace("/", "", 1)

    def _validate_parsed_uri(self) -> None:
        query = parse_qs(self._parsed_storage_uri.query)
        self._region = get_optional_query_parameter(query, "region")

    def _connect(self) -> Session:
        aws_session = boto3.session.Session(
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._access_secret,
            region_name=self._region)

        return aws_session.client("s3")

    def save_to_filename(self, file_path: str) -> None:
        client = self._connect()

        transfer = boto3.s3.transfer.S3Transfer(client)
        try:
            transfer.download_file(self._bucket, self._keyname, file_path)
        except ClientError as original_exc:
            if original_exc.response["Error"]["Code"] == "404":
                raise NotFoundError("No File Found") from original_exc
            raise original_exc

    def save_to_file(self, out_file: BinaryIO) -> None:
        client = self._connect()

        response = client.get_object(Bucket=self._bucket, Key=self._keyname)

        if "Body" not in response:
            raise NotFoundError("No File Found")

        while True:
            chunk = response["Body"].read(_LARGE_CHUNK)
            out_file.write(chunk)
            if not chunk:
                break

    def save_to_directory(self, directory_path: str) -> None:
        client = self._connect()
        directory_prefix = "{}/".format(self._keyname)
        dir_object = client.list_objects(Bucket=self._bucket, Prefix=directory_prefix)

        if "Contents" not in dir_object:
            raise NotFoundError("No Files Found")

        dir_contents = dir_object["Contents"]

        for obj in dir_contents:
            file_key = obj["Key"].replace(self._keyname, "", 1)

            if file_key and not file_key.endswith("/"):
                file_path = os.path.dirname(file_key)

                if not os.path.exists(directory_path + file_path):
                    os.makedirs(directory_path + file_path)

                try:
                    retry.attempt(
                        client.download_file, self._bucket, obj["Key"], directory_path + file_key)
                except ClientError as original_exc:
                    if original_exc.response["Error"]["Code"] == "404":
                        raise NotFoundError("No File Found") from original_exc
                    raise original_exc

    def load_from_filename(self, file_path: str) -> None:
        client = self._connect()

        extra_args = None
        content_type = mimetypes.guess_type(file_path)[0]
        if content_type is not None:
            extra_args = {"ContentType": content_type}

        transfer = boto3.s3.transfer.S3Transfer(client)
        transfer.upload_file(file_path, self._bucket, self._keyname, extra_args=extra_args)

    def load_from_file(self, in_file: BinaryIO) -> None:
        client = self._connect()

        extra_args: Dict[str, str] = {}

        content_type = mimetypes.guess_type(self._storage_uri)[0]
        if content_type is not None:
            extra_args["ContentType"] = content_type

        client.put_object(Bucket=self._bucket, Key=self._keyname, Body=in_file, **extra_args)

    def load_from_directory(self, source_directory: str) -> None:
        client = self._connect()

        for root, _, files in os.walk(source_directory):
            relative_path = root.replace(source_directory, self._keyname, 1)

            for filename in files:
                upload_path = os.path.join(relative_path, filename)
                extra_args = None
                content_type = mimetypes.guess_type(filename)[0]
                if content_type is not None:
                    extra_args = {"ContentType": content_type}
                retry.attempt(
                    client.upload_file, os.path.join(root, filename), self._bucket, upload_path,
                    ExtraArgs=extra_args)

    def delete(self) -> None:
        client = self._connect()
        response = client.delete_object(Bucket=self._bucket, Key=self._keyname)

        if "DeleteMarker" not in response:
            raise NotFoundError("No File Found")

    def delete_directory(self) -> None:
        client = self._connect()
        directory_prefix = "{}/".format(self._keyname)
        dir_object = client.list_objects(Bucket=self._bucket, Prefix=directory_prefix)

        if "Contents" not in dir_object:
            raise NotFoundError("No Files Found")

        object_keys = [{"Key": o.get("Key", None)} for o in dir_object["Contents"]]

        try:
            client.delete_objects(Bucket=self._bucket, Delete={"Objects": object_keys})
        except ClientError as original_exc:
            if original_exc.response["Error"]["Code"] == "404":
                raise NotFoundError("No File Found") from original_exc
            raise original_exc

    def get_download_url(self, seconds: int = 60, key: Optional[str] = None) -> str:
        client = self._connect()

        return client.generate_presigned_url(
            "get_object", Params={"Bucket": self._bucket, "Key": self._keyname}, ExpiresIn=seconds)

    def get_sanitized_uri(self) -> str:
        return remove_user_info(self._parsed_storage_uri)
