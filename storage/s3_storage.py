import os
import urllib
import urlparse

import boto3
import boto3.s3.transfer

from . import retry
from .storage import Storage, register_storage_protocol, _LARGE_CHUNK


@register_storage_protocol("s3")
class S3Storage(Storage):
    def __init__(self, storage_uri):
        super(S3Storage, self).__init__(storage_uri)
        self._access_key = urllib.unquote(self._parsed_storage_uri.username)
        self._access_secret = urllib.unquote(self._parsed_storage_uri.password)
        self._bucket = self._parsed_storage_uri.hostname
        self._keyname = self._parsed_storage_uri.path.replace("/", "", 1)
        query = urlparse.parse_qs(self._parsed_storage_uri.query)
        self._region = query.get("region", [None])[0]

    def _connect(self):
        aws_session = boto3.session.Session(
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._access_secret,
            region_name=self._region)

        return aws_session.client("s3")

    def save_to_filename(self, file_path):
        client = self._connect()

        transfer = boto3.s3.transfer.S3Transfer(client)
        transfer.download_file(self._bucket, self._keyname, file_path)

    def save_to_file(self, out_file):
        client = self._connect()

        response = client.get_object(Bucket=self._bucket, Key=self._keyname)

        while True:
            chunk = response["Body"].read(_LARGE_CHUNK)
            out_file.write(chunk)
            if not chunk:
                break

    def save_to_directory(self, directory_path):
        client = self._connect()
        directory_prefix = "{}/".format(self._keyname)
        dir_object = client.list_objects(Bucket=self._bucket, Prefix=directory_prefix)
        dir_contents = dir_object["Contents"]

        for file in dir_contents:
            file_key = file["Key"].replace(self._keyname, "", 1)

            if file_key and not file_key.endswith("/"):
                file_path = os.path.dirname(file_key)

                if not os.path.exists(directory_path + file_path):
                    os.makedirs(directory_path + file_path)

                retry.attempt(
                    client.download_file,
                    self._bucket,
                    file["Key"],
                    directory_path + file_key)

    def load_from_filename(self, file_path):
        client = self._connect()

        transfer = boto3.s3.transfer.S3Transfer(client)
        transfer.upload_file(file_path, self._bucket, self._keyname)

    def load_from_file(self, in_file):
        client = self._connect()

        client.put_object(Bucket=self._bucket, Key=self._keyname, Body=in_file)

    def load_from_directory(self, source_directory):
        client = self._connect()

        for root, _, files in os.walk(source_directory):
            relative_path = root.replace(source_directory, self._keyname, 1)

            for file in files:
                upload_path = os.path.join(relative_path, file)
                retry.attempt(
                    client.upload_file,
                    os.path.join(root, file),
                    self._bucket,
                    upload_path)

    def delete(self):
        client = self._connect()
        client.delete_object(Bucket=self._bucket, Key=self._keyname)

    def delete_directory(self):
        client = self._connect()
        directory_prefix = "{}/".format(self._keyname)
        dir_object = client.list_objects(Bucket=self._bucket, Prefix=directory_prefix)
        object_keys = [{"Key": o.get("Key", None)} for o in dir_object["Contents"]]
        client.delete_objects(
            Bucket=self._bucket,
            Delete={
                "Objects": object_keys
            })

    def get_download_url(self, seconds=60, key=None):
        client = self._connect()

        return client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self._bucket, "Key": self._keyname},
            ExpiresIn=seconds
        )
