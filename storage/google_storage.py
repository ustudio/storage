import base64
import datetime
import json
import os

import google.cloud.storage.client
from google.cloud.storage.bucket import Bucket
from google.cloud.storage.blob import Blob
import google.oauth2.service_account

from typing import BinaryIO, Optional

from storage import retry
from storage.storage import Storage, register_storage_protocol


@register_storage_protocol("gs")
class GoogleStorage(Storage):

    def _get_bucket(self) -> Bucket:
        username = self._parsed_storage_uri.username
        credentials_data = json.loads(base64.urlsafe_b64decode(username))
        credentials = google.oauth2.service_account.Credentials.from_service_account_info(
            credentials_data)
        client = google.cloud.storage.client.Client(
            project=credentials_data["project_id"], credentials=credentials)
        return client.get_bucket(self._parsed_storage_uri.hostname)

    def _get_blob(self) -> Blob:
        bucket = self._get_bucket()
        blob = bucket.blob(self._parsed_storage_uri.path[1:])
        return blob

    def save_to_filename(self, file_path: str) -> None:
        blob = self._get_blob()
        blob.download_to_filename(file_path)

    def save_to_file(self, out_file: BinaryIO) -> None:
        blob = self._get_blob()
        blob.download_to_file(out_file)

    def load_from_filename(self, file_path: str) -> None:
        blob = self._get_blob()
        blob.upload_from_filename(file_path)

    def load_from_file(self, in_file: BinaryIO) -> None:
        blob = self._get_blob()
        blob.upload_from_file(in_file)

    def delete(self) -> None:
        blob = self._get_blob()
        blob.delete()

    def get_download_url(self, seconds: int = 60, key: Optional[str] = None) -> str:
        blob = self._get_blob()
        return blob.generate_signed_url(
            expiration=datetime.timedelta(seconds=seconds),
            response_disposition="attachment")

    def save_to_directory(self, directory_path: str) -> None:
        bucket = self._get_bucket()

        prefix = self._parsed_storage_uri.path[1:] + "/"
        for blob in bucket.list_blobs(prefix=prefix):
            relative_path = blob.name.replace(prefix, "", 1)
            local_file_path = os.path.join(directory_path, relative_path)
            local_directory = os.path.dirname(local_file_path)

            if not os.path.exists(local_directory):
                os.makedirs(local_directory)

            if not relative_path[-1] == "/":
                unversioned_blob = bucket.blob(blob.name)
                retry.attempt(unversioned_blob.download_to_filename, local_file_path)

    def load_from_directory(self, directory_path: str) -> None:
        bucket = self._get_bucket()

        prefix = self._parsed_storage_uri.path[1:]

        for root, _, files in os.walk(directory_path):
            remote_path = root.replace(directory_path, prefix, 1)

            for filename in files:
                blob = bucket.blob("/".join([remote_path, filename]))
                retry.attempt(blob.upload_from_filename, os.path.join(root, filename))

    def delete_directory(self) -> None:
        bucket = self._get_bucket()

        for blob in bucket.list_blobs(prefix=self._parsed_storage_uri.path[1:] + "/"):
            unversioned_blob = bucket.blob(blob.name)
            unversioned_blob.delete()
