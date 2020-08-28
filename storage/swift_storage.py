import mimetypes
import os
from urllib.parse import parse_qs, parse_qsl, ParseResult, urlencode, urljoin, urlparse

from keystoneauth1 import session
from keystoneauth1.identity import v2
import swiftclient.client
from swiftclient.exceptions import ClientException
import swiftclient.utils
from typing import BinaryIO, Callable, cast, Dict, List, Optional, Tuple, Type, TypeVar

from storage import retry
from storage.storage import InvalidStorageUri, register_storage_protocol, Storage, NotFoundError
from storage.storage import get_optional_query_parameter, _LARGE_CHUNK, DEFAULT_SWIFT_TIMEOUT


def register_swift_protocol(
        scheme: str, auth_endpoint: str) -> Callable[[Type["SwiftStorage"]], Type["SwiftStorage"]]:
    def wrapper(cls: Type["SwiftStorage"]) -> Type["SwiftStorage"]:
        cls.auth_endpoint = auth_endpoint
        return cast(Type[SwiftStorage], register_storage_protocol(scheme)(cls))
    return wrapper


class SwiftStorageError(Exception):

    def __init__(self, message: str, do_not_retry: bool = False) -> None:
        super().__init__(message)
        self.do_not_retry = do_not_retry


T = TypeVar("T")


@register_storage_protocol("swift")
class SwiftStorage(Storage):

    download_url_key: Optional[str]

    def _validate_parsed_uri(self) -> None:
        query = parse_qs(self._parsed_storage_uri.query)

        auth_endpoint = get_optional_query_parameter(query, "auth_endpoint")
        if auth_endpoint is None:
            raise SwiftStorageError("Required field is missing: auth_endpoint")
        self.auth_endpoint = auth_endpoint

        region_name = get_optional_query_parameter(query, "region")
        if region_name is None:
            raise SwiftStorageError("Required field is missing: region_name")
        self.region_name = region_name

        tenant_id = get_optional_query_parameter(query, "tenant_id")
        if tenant_id is None:
            raise SwiftStorageError("Required field is missing: tenant_id")
        self.tenant_id = tenant_id

        self.download_url_key = get_optional_query_parameter(query, "download_url_key")

        if self._parsed_storage_uri.username == "":
            raise InvalidStorageUri("Missing username")
        if self._parsed_storage_uri.password == "":
            raise InvalidStorageUri("Missing API key")

    # cache get connections
    def get_connection(self) -> swiftclient.client.Connection:
        if not hasattr(self, "_connection"):
            os_options = {
                "tenant_id": self.tenant_id,
                "region_name": self.region_name
            }

            user = self._parsed_storage_uri.username
            key = self._parsed_storage_uri.password

            auth = v2.Password(
                auth_url=self.auth_endpoint, username=user, password=key,
                tenant_name=self.tenant_id)

            keystone_session = session.Session(auth=auth)

            connection = swiftclient.client.Connection(
                session=keystone_session, os_options=os_options, timeout=DEFAULT_SWIFT_TIMEOUT,
                retries=0)
            connection.get_auth()

            self._connection = connection
        return self._connection

    def get_container_and_object_names(self) -> Tuple[str, str]:
        _, container = self._parsed_storage_uri.netloc.split("@")
        object_name = self._parsed_storage_uri.path[1:]
        return container, object_name

    def _download_object_to_file(
            self, container: str, object_name: str, out_file: BinaryIO) -> None:
        connection = self.get_connection()

        # may need to truncate file if retrying...
        out_file.seek(0)

        try:
            resp_headers, object_contents = connection.get_object(
                container, object_name, resp_chunk_size=_LARGE_CHUNK)
        except ClientException as original_exc:
            if original_exc.http_status == 404:
                raise NotFoundError("No File Found") from original_exc
            raise original_exc

        for object_content in object_contents:
            out_file.write(object_content)

    def _download_object_to_filename(self, container: str, object_name: str, filename: str) -> None:
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

        mimetype = mimetypes.guess_type(object_name)[0] or "application/octet-stream"

        connection.put_object(container, object_name, in_file, content_type=mimetype)

    def load_from_filename(self, in_path: str) -> None:
        with open(in_path, "rb") as fp:
            self.load_from_file(fp)

    def delete(self) -> None:
        connection = self.get_connection()
        container, object_name = self.get_container_and_object_names()

        try:
            connection.delete_object(container, object_name)
        except ClientException as original_exc:
            if original_exc.http_status == 404:
                raise NotFoundError("No File Found") from original_exc
            raise original_exc

    def get_download_url(self, seconds: int = 60, key: Optional[str] = None) -> str:
        connection = self.get_connection()

        download_url_key = key or self.download_url_key

        if download_url_key is None:
            raise SwiftStorageError(
                "Missing required `download_url_key` for `get_download_url`.")

        host, _ = connection.get_service_auth()
        container, object_name = self.get_container_and_object_names()

        storage_url, _ = connection.get_auth()
        storage_path = urlparse(storage_url).path

        path = swiftclient.utils.generate_temp_url(
            f"{storage_path}/{container}/{object_name}",
            seconds=seconds, key=download_url_key, method="GET")

        return urljoin(host, path)

    def get_sanitized_uri(self) -> str:
        parsed_uri = self._parsed_storage_uri
        new_query = dict(parse_qsl(parsed_uri.query))

        if "download_url_key" in new_query:
            del new_query["download_url_key"]

        new_uri = ParseResult(
            parsed_uri.scheme, parsed_uri.hostname, parsed_uri.path, parsed_uri.params,
            urlencode(new_query), parsed_uri.fragment)

        return new_uri.geturl()

    def _find_storage_objects_with_prefix(
            self, container: str, prefix: str) -> List[Dict[str, str]]:
        connection = self.get_connection()
        try:
            _, container_objects = connection.get_container(container, prefix=prefix)
            if len(container_objects) == 0:
                raise NotFoundError("No Files Found")
            return container_objects
        except ClientException as original_exc:
            if original_exc.http_status == 404:
                raise NotFoundError("No File Found") from original_exc
            raise original_exc

    def save_to_directory(self, directory_path: str) -> None:
        container, object_name = self.get_container_and_object_names()

        prefix = self._parsed_storage_uri.path[1:] + "/"

        for container_object in self._find_storage_objects_with_prefix(container, prefix):
            if container_object["name"].endswith("/"):
                continue

            base_path = container_object["name"].split(prefix)[1]
            relative_path = os.path.sep.join(base_path.split("/"))
            file_path = os.path.join(directory_path, relative_path)
            object_path = container_object["name"]

            while object_path.startswith("/"):
                object_path = object_path[1:]

            dir_name = os.path.dirname(file_path)
            os.makedirs(dir_name, exist_ok=True)

            retry.attempt(self._download_object_to_filename, container, object_path, file_path)

    def load_from_directory(self, directory_path: str) -> None:
        connection = self.get_connection()
        container, object_name = self.get_container_and_object_names()

        prefix = self._parsed_storage_uri.path[1:]

        for root, _, files in os.walk(directory_path):
            base = root.split(directory_path, 1)[1]
            while base.startswith("/"):
                base = base[1:]
            while base.endswith("/"):
                base = base[:-1]
            for filename in files:
                local_path = os.path.join(root, filename)
                remote_path = "/".join(filter(lambda x: x != "", [prefix, base, filename]))

                mimetype = mimetypes.guess_type(remote_path)[0] or "application/octet-stream"

                with open(local_path, "rb") as fp:
                    retry.attempt(
                        connection.put_object, container, remote_path, fp, content_type=mimetype)

    def delete_directory(self) -> None:
        connection = self.get_connection()
        container, object_name = self.get_container_and_object_names()

        prefix = self._parsed_storage_uri.path[1:] + "/"

        for container_object in self._find_storage_objects_with_prefix(container, prefix):
            object_path = container_object["name"]

            while object_path.startswith("/"):
                object_path = object_path[1:]

            connection.delete_object(container, object_path)
