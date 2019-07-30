import os
from urllib.parse import parse_qsl, urljoin

from keystoneauth1 import session
from keystoneauth1.identity import v2
from keystoneauth1.exceptions.http import Forbidden, Unauthorized
import swiftclient
from swiftclient.exceptions import ClientException
from typing import BinaryIO

from . import retry
from .storage import _LARGE_CHUNK, DEFAULT_SWIFT_TIMEOUT, register_storage_protocol, Storage


class SwiftStorageError(Exception):

    def __init__(self, message: str, do_not_retry: bool = False) -> None:
        super().__init__(message)
        self.do_not_retry = do_not_retry


def retry_swift_operation(error_str, fn, *args, **kwargs):

    def wrap_swift_operations():
        try:
            return fn(*args, **kwargs)
        except Forbidden:
            raise SwiftStorageError(
                f"Keystone authorization returned forbidden access {error_str}",
                do_not_retry=True)
        except Unauthorized:
            raise SwiftStorageError(
                f"Keystine authorization return unauthorized access {error_str}",
                do_not_retry=True)
        except ClientException as swift_exception:
            raise SwiftStorageError(
                f"Unable to perform swift operation {error_str}: {swift_exception}",
                do_not_retry=True)

    try:
        return retry.attempt(wrap_swift_operations)
    except SwiftStorageError:
        raise
    except Exception as exc:
        raise SwiftStorageError(f"Failure retrieving object: {exc}")


@register_storage_protocol("swift")
class SwiftStorage(Storage):

    def get_connection(self):
        query = dict(parse_qsl(self._parsed_storage_uri.query))

        auth_endpoint = query.get("auth_endpoint")
        if auth_endpoint is None:
            raise SwiftStorageError(f"Required filed is missing: auth_endpoint")

        tenant_id = query.get("tenant_id")
        if tenant_id is None:
            raise SwiftStorageError(f"Required filed is missing: tenant_id")

        region_name = query.get("region")
        if region_name is None:
            raise SwiftStorageError(f"Required filed is missing: region_name")

        self.download_url_key = query.get("download_url_key")

        os_options = {
            "tenant_id": tenant_id,
            "region_name": region_name
        }

        auth, _ = self._parsed_storage_uri.netloc.split("@")
        user, key = auth.split(":", 1)

        if user == "":
            raise SwiftStorageError(f"Missing username")

        if key == "":
            raise SwiftStorageError(f"Missing API key")

        auth = v2.Password(
            auth_url=auth_endpoint, username=user, password=key, tenant_name=tenant_id)

        keystone_session = session.Session(auth=auth)

        connection = swiftclient.client.Connection(
            session=keystone_session, os_options=os_options, timeout=DEFAULT_SWIFT_TIMEOUT)
        return connection

    def get_container_and_object_names(self):
        _, container = self._parsed_storage_uri.netloc.split("@")
        object_name = self._parsed_storage_uri.path[1:]
        return container, object_name

    def _download_object_to_file(self, container, object_name, out_file):
        connection = self.get_connection()

        def get_object():
            out_file.seek(0)
            resp_headers, object_contents = connection.get_object(
                container, object_name, resp_chunk_size=_LARGE_CHUNK)
            # may need to truncate file if retrying...
            for object_content in object_contents:
                out_file.write(object_content)

        retry_swift_operation(
            f"Failed to retrieve Swift object {object_name} from container {container}",
            get_object)

    def _download_object_to_filename(self, container, object_name, filename):
        dir_name = os.path.dirname(filename)
        os.makedirs(dir_name, exist_ok=True)
        with open(filename, "wb") as out_file:
            self._download_object_to_file(container, object_name, out_file)

    def save_to_file(self, out_file: BinaryIO) -> None:
        container, object_name = self.get_container_and_object_names()
        self._download_object_to_file(container, object_name, out_file)

    def save_to_filename(self, file_path: str) -> None:
        container, object_name = self.get_container_and_object_names()
        self._download_object_to_filename(container, object_name, file_path)

    def load_from_file(self, in_file: BinaryIO) -> None:
        connection = self.get_connection()
        container, object_name = self.get_container_and_object_names()

        retry_swift_operation(
            f"Failed to store Swift object {object_name} in container {container}",
            connection.put_object, container, object_name, in_file)

    def load_from_filename(self, in_path: str) -> None:
        with open(in_path, "rb") as fp:
            self.load_from_file(fp)

    def delete(self) -> None:
        connection = self.get_connection()
        container, object_name = self.get_container_and_object_names()

        retry_swift_operation(
            f"Failed to store Swift object {object_name} in container {container}",
            connection.delete_object, container, object_name)

    def get_download_url(self, seconds=60, key=None) -> str:
        connection = self.get_connection()

        download_url_key = key or self.download_url_key

        if download_url_key is None:
            raise SwiftStorageError(
                "Missing required `download_url_key` for `get_download_url`.")

        host, _ = connection.get_service_auth()
        container, object_name = self.get_container_and_object_names()

        path = swiftclient.utils.generate_temp_url(
            f"/v1/AUTH_account/{container}/{object_name}",
            seconds=seconds, key=download_url_key, method="GET")

        return urljoin(host, path)

    def save_to_directory(self, directory_path):
        connection = self.get_connection()

        container, object_name = self.get_container_and_object_names()

        prefix = self._parsed_storage_uri.path[1:] + "/"

        def get_container():
            resp_headers, objects = connection.get_container(container, prefix=prefix)
            for container_object in objects:
                base_path = container_object["name"].split(prefix)[1]
                relative_path = os.path.sep.join(base_path.split("/"))
                file_path = os.path.join(directory_path, relative_path)
                object_path = container_object["name"]

                while object_path.startswith("/"):
                    object_path = object_path[1:]

                self._download_object_to_filename(container, object_path, file_path)

        retry_swift_operation(
            f"Failed to retrieve Swift object {object_name} from container {container}",
            get_container)
