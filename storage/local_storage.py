import distutils.dir_util
from distutils.errors import DistutilsFileError
import os
import shutil
from urllib.parse import parse_qs

from typing import BinaryIO, Optional

from storage.storage import get_optional_query_parameter, Storage, register_storage_protocol
from storage.storage import _generate_download_url_from_base, NotFoundError
from storage.url_parser import remove_user_info


@register_storage_protocol("file")
class LocalStorage(Storage):
    """LocalStorage is a local file storage object.

    The URI for working with local file storage has the following format:

      file:///some/path/to/a/file.txt?[download_url_base=<URL-ENCODED-URL>]

      file:///some/path/to/a/directory?[download_url_base=<URL-ENCODED-URL>]

    """

    def _validate_parsed_uri(self) -> None:
        query = parse_qs(self._parsed_storage_uri.query)
        self._download_url_base = get_optional_query_parameter(query, "download_url_base")

    def save_to_filename(self, file_path: str) -> None:
        try:
            shutil.copy(self._parsed_storage_uri.path, file_path)
        except FileNotFoundError:
            raise NotFoundError("No File Found")

    def save_to_file(self, out_file: BinaryIO) -> None:
        try:
            with open(self._parsed_storage_uri.path, "rb") as in_file:
                for chunk in in_file:
                    out_file.write(chunk)
        except FileNotFoundError:
            raise NotFoundError("No File Found")

    def save_to_directory(self, destination_directory: str) -> None:
        try:
            distutils.dir_util.copy_tree(self._parsed_storage_uri.path, destination_directory)
        except DistutilsFileError:
            raise NotFoundError("No Files Found")

    def _ensure_exists(self) -> None:
        dirname = os.path.dirname(self._parsed_storage_uri.path)

        if not os.path.exists(dirname):
            os.makedirs(dirname)

    def load_from_filename(self, file_path: str) -> None:
        self._ensure_exists()

        shutil.copy(file_path, self._parsed_storage_uri.path)

    def load_from_file(self, in_file: BinaryIO) -> None:
        self._ensure_exists()

        with open(self._parsed_storage_uri.path, "wb") as out_file:
            for chunk in in_file:
                out_file.write(chunk)

    def load_from_directory(self, source_directory: str) -> None:
        self._ensure_exists()
        distutils.dir_util.copy_tree(source_directory, self._parsed_storage_uri.path)

    def delete(self) -> None:
        try:
            os.remove(self._parsed_storage_uri.path)
        except FileNotFoundError:
            raise NotFoundError("No File Found")

    def delete_directory(self) -> None:
        try:
            shutil.rmtree(self._parsed_storage_uri.path)
        except FileNotFoundError:
            raise NotFoundError("No Files Found")

    def get_download_url(self, seconds: int = 60, key: Optional[str] = None) -> str:
        """
        Return a temporary URL allowing access to the storage object.

        If a download_url_base is specified in the storage URI, then a call to get_download_url()
        will return the download_url_base joined with the object name.

        For example, if "http://www.someserver.com:1234/path/to/" were passed (urlencoded) as the
        download_url_base query parameter of the storage URI:

          file://some/path/to/a/file.txt?download_url_base=http%3A%2F%2Fwww.someserver.com%3A1234%2Fpath%2Fto%2

        then a call to get_download_url() would yield:

          http://www.someserver.com:1234/path/to/file.txt


        :param seconds: ignored for local storage
        :param key:     ignored for local storage
        :return:        the download url that can be used to access the storage object
        :raises:        DownloadUrlBaseUndefinedError
        """
        return _generate_download_url_from_base(
            self._download_url_base, self._parsed_storage_uri.path.split('/')[-1])

    def get_sanitized_uri(self) -> str:
        return remove_user_info(self._parsed_storage_uri)
