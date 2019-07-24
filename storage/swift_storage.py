from urllib.parse import parse_qsl

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

    def save_to_file(self, out_file: BinaryIO) -> None:
        connection = self.get_connection()
        container, object_name = self.get_container_and_object_names()

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

    def save_to_filename(self, file_path: str) -> None:
        with open(file_path, "wb") as out_file:
            self.save_to_file(out_file)

    def load_from_file(self, in_file: BinaryIO) -> None:
        connection = self.get_connection()
        container, object_name = self.get_container_and_object_names()
        connection.put_object(container, object_name, in_file)

#    def load_from_file(self, in_file: BinaryIO) -> None:
#        connection = self.get_connection()
#        container, object_name = self.get_container_and_object_names()
#
#        def put_object():
#            in_file.seek(0)
#            resp_headers, object_contents = connection.put_object(
#                container, object_name, in_file, resp_chunk_size=_LARGE_CHUNK)
#
#        retry_swift_operation(
#            f"Failed to upload Swift object {object_name} to container {container}",
#            put_object)
