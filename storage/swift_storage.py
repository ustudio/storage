import mimetypes
from urllib.parse import parse_qs, urlparse

import swiftclient
from swiftclient.utils import generate_temp_url
from typing import BinaryIO, Optional, Tuple

from .storage import _LARGE_CHUNK, register_storage_protocol, Storage


@register_storage_protocol("swift")
class SwiftStorage(Storage):
    def _get_connection(self) -> swiftclient.client.Connection:
        auth, _ = self._parsed_storage_uri.netloc.split("@")
        username, password = auth.split(":", 1)

        query = parse_qs(self._parsed_storage_uri.query)
        auth_endpoint = query.get("auth_endpoint", [None])[0]

        # This is the only auth parameter that's saved for later
        self.download_url_key = query.get("download_url_key", [None])[0]
        self.tenant_id = query.get("tenant_id", [None])[0]

        connection = swiftclient.client.Connection(
            authurl=auth_endpoint, user=username, key=password)

        return connection

    def _get_stoage_info(self) -> Tuple[str]:
        _, container_name = self._parsed_storage_uri.netloc.split("@")
        object_name = self._parsed_storage_uri.path[1:]

        return container_name, object_name

    def _get_content_type(self, object_name: str) -> str:
        content_type, _ = mimetypes.guess_type(object_name)
        return content_type

    def _get_connection_path(self) -> str:
        connection = self._get_connection()

        return urlparse(connection.url).path

    def save_to_file(self, out_file: BinaryIO) -> None:
        connection = self._get_connection()
        container_name, object_name = self._get_stoage_info()

        for _, chunk in connection.get_object(
                container_name, object_name, resp_chunk_size=_LARGE_CHUNK):
            out_file.write(chunk)

    def save_to_filename(self, file_path: str) -> None:
        with open(file_path, "wb") as out_file:
            self.save_to_file(out_file)

    def load_from_file(self, in_file: BinaryIO) -> None:
        connection = self._get_connection()

        container_name, object_name = self._get_stoage_info()
        content_type = self._get_content_type(object_name)

        connection.put_object(
            container_name, object_name, contents=in_file, content_type=content_type)

    def load_from_filename(self, file_path: str) -> None:
        with open(file_path, "rb") as in_file:
            self.load_from_file(in_file)

    def delete(self) -> None:
        connection = self._get_connection()

        container_name, object_name = self._get_stoage_info()

        connection.delete_object(container_name, object_name)

    def get_download_url(self, seconds: int = 60, key: Optional[str] = None) -> str:
        path = self._get_connection_path()

        container_name, object_name = self._get_stoage_info()

        resource = f"{path}/{container_name}/{object_name}"

        return generate_temp_url(resource, seconds, self.download_url_key, "GET")
