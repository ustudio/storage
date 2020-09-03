import ftplib
from ftplib import FTP, error_perm
import os
import re
import socket
from urllib.parse import parse_qsl

from typing import BinaryIO, Generator, List, Optional, Tuple

from storage.storage import Storage, register_storage_protocol, _generate_download_url_from_base
from storage.storage import DEFAULT_FTP_TIMEOUT, DEFAULT_FTP_KEEPALIVE_ENABLE, DEFAULT_FTP_KEEPCNT
from storage.storage import DEFAULT_FTP_KEEPIDLE, DEFAULT_FTP_KEEPINTVL, NotFoundError
from storage.url_parser import remove_user_info


class FTPStorageError(Exception):
    pass


@register_storage_protocol("ftp")
class FTPStorage(Storage):
    """FTP storage.

    The URI for working with FTP storage has the following format:

      ftp://username:password@hostname/path/to/file.txt[?download_url_base=<URL-ENCODED-URL>]

      ftp://username:password@hostname/path/to/directory[?download_url_base=<URL-ENCODED-URL>]

    If the ftp storage has access via HTTP, then a download_url_base can be specified
    that will allow get_download_url() to return access to that object via HTTP.
    """

    _download_url_base: Optional[str]

    def __init__(self, storage_uri: str) -> None:
        super(FTPStorage, self).__init__(storage_uri)
        self._username = self._parsed_storage_uri.username
        self._password = self._parsed_storage_uri.password
        self._hostname = self._parsed_storage_uri.hostname
        self._port = \
            self._parsed_storage_uri.port if self._parsed_storage_uri.port is not None else 21
        query = dict(parse_qsl(self._parsed_storage_uri.query))
        self._download_url_base = query.get("download_url_base", None)

    def _configure_keepalive(self, ftp_client: FTP) -> None:
        sock = ftp_client.sock
        if sock is None:
            raise FTPStorageError("FTP Client not fully initialized")

        sock.setsockopt(
            socket.SOL_SOCKET, socket.SO_KEEPALIVE, DEFAULT_FTP_KEEPALIVE_ENABLE)

        if hasattr(socket, "TCP_KEEPCNT"):
            sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, DEFAULT_FTP_KEEPCNT)

        if hasattr(socket, "TCP_KEEPIDLE"):
            sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, DEFAULT_FTP_KEEPIDLE)

        if hasattr(socket, "TCP_KEEPINTVL"):
            sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, DEFAULT_FTP_KEEPINTVL)

    def _connect(self) -> FTP:
        ftp_client = ftplib.FTP(timeout=DEFAULT_FTP_TIMEOUT)
        ftp_client.connect(self._hostname, port=self._port)

        self._configure_keepalive(ftp_client)

        ftp_client.login(self._username, self._password)

        return ftp_client

    def _cd_to_file(self, ftp_client: FTP) -> str:
        directory, filename = os.path.split(self._parsed_storage_uri.path.lstrip("/"))
        ftp_client.cwd(directory)
        return filename

    def _list(self, ftp_client: FTP) -> Tuple[List[str], List[str]]:
        directory_listing: List[str] = []

        ftp_client.retrlines('LIST', directory_listing.append)

        directories = []
        files = []

        for line in directory_listing:
            name = re.split(r"\s+", line, 8)[-1]

            if line.lower().startswith("d"):
                directories.append(name)
            else:
                files.append(name)

        return directories, files

    def _walk(
            self, ftp_client: FTP,
            target_directory: Optional[str] = None) -> \
            Generator[Tuple[str, List[str], List[str]], None, None]:
        if target_directory:
            ftp_client.cwd(target_directory)
        else:
            target_directory = ftp_client.pwd()

        dirs, files = self._list(ftp_client)

        yield target_directory, dirs, files

        for name in dirs:
            new_target = os.path.join(target_directory, name)

            for result in self._walk(ftp_client, target_directory=new_target):
                yield result

    def _create_directory_structure(
            self, ftp_client: FTP, target_path: str, restore: Optional[bool] = False) -> None:
        directories = target_path.lstrip('/').split('/')

        if restore:
            dirpath = ftp_client.pwd()

        for target_directory in directories:
            dirs, _ = self._list(ftp_client)

            # TODO (phd): warn the user that a file exists with the name of their target dir
            if target_directory not in dirs:
                ftp_client.mkd(target_directory)

            ftp_client.cwd(target_directory)

        if restore:
            ftp_client.cwd(dirpath)

    def save_to_filename(self, file_path: str) -> None:
        with open(file_path, "wb") as output_file:
            self.save_to_file(output_file)

    def save_to_file(self, out_file: BinaryIO) -> None:
        ftp_client = self._connect()
        filename = self._cd_to_file(ftp_client)

        try:
            ftp_client.retrbinary("RETR {0}".format(filename), callback=out_file.write)
        except error_perm as original_exc:
            if original_exc.args[0][:3] == "550":
                raise NotFoundError("No File Found") from original_exc
            raise original_exc

    def save_to_directory(self, destination_directory: str) -> None:
        ftp_client = self._connect()
        base_ftp_path = self._parsed_storage_uri.path

        try:
            ftp_client.cwd(base_ftp_path)

            for root, dirs, files in self._walk(ftp_client):
                relative_path = "/{}".format(root).replace(base_ftp_path, destination_directory, 1)

                if not os.path.exists(relative_path):
                    os.makedirs(relative_path)

                os.chdir(relative_path)

                for filename in files:
                    with open(os.path.join(relative_path, filename), "wb") as output_file:
                        ftp_client.retrbinary(
                            "RETR {0}".format(filename), callback=output_file.write)
        except error_perm as original_exc:
            if original_exc.args[0][:3] == "550":
                raise NotFoundError("No File Found") from original_exc
            raise original_exc

    def load_from_filename(self, file_path: str) -> None:
        with open(file_path, "rb") as input_file:
            self.load_from_file(input_file)

    def load_from_file(self, in_file: BinaryIO) -> None:
        ftp_client = self._connect()
        filename = self._cd_to_file(ftp_client)

        ftp_client.storbinary("STOR {0}".format(filename), in_file)

    def load_from_directory(self, source_directory: str) -> None:
        ftp_client = self._connect()
        base_ftp_path = self._parsed_storage_uri.path

        self._create_directory_structure(ftp_client, base_ftp_path)

        for root, dirs, files in os.walk(source_directory):
            relative_ftp_path = root.replace(source_directory, base_ftp_path, 1)

            ftp_client.cwd(relative_ftp_path)

            for directory in dirs:
                self._create_directory_structure(ftp_client, directory, restore=True)

            for file in files:
                file_path = os.path.join(root, file)

                with open(file_path, "rb") as input_file:
                    ftp_client.storbinary("STOR {0}".format(file), input_file)

    def delete(self) -> None:
        ftp_client = self._connect()
        filename = self._cd_to_file(ftp_client)

        try:
            ftp_client.delete(filename)
        except error_perm as original_exc:
            if original_exc.args[0][:3] == "550":
                raise NotFoundError("No File Found") from original_exc
            raise original_exc

    def delete_directory(self) -> None:
        ftp_client = self._connect()
        base_ftp_path = self._parsed_storage_uri.path

        try:
            ftp_client.cwd(base_ftp_path)

            directories_to_remove = []
            for root, directories, files in self._walk(ftp_client):
                for filename in files:
                    ftp_client.delete("/{}/{}".format(root, filename))

                directories_to_remove.append("/{}".format(root))
        except error_perm as original_exc:
            if original_exc.args[0][:3] == "550":
                raise NotFoundError("No File Found") from original_exc
            raise original_exc

        # delete directories _after_ removing files from directories
        # directories should be removed in reverse order - leaf directories before
        # parent directories - since there is no recursive delete
        directories_to_remove.sort(reverse=True)
        for directory in directories_to_remove:
            ftp_client.rmd("{}".format(directory))

    def get_download_url(self, seconds: int = 60, key: Optional[str] = None) -> str:
        """
        Return a temporary URL allowing access to the storage object.

        If a download_url_base is specified in the storage URI, then a call to get_download_url()
        will return the download_url_base joined with the object name.

        For example, if "http://www.someserver.com:1234/path/to/" were passed (urlencoded) as the
        download_url_base query parameter of the storage URI:

          ftp://username:password@hostname/some/path/to/a/file.txt?download_url_base=http%3A%2F%2Fwww
          .someserver.com%3A1234%2Fpath%2Fto%2

        then a call to get_download_url() would yield:

          http://www.someserver.com:1234/path/to/file.txt


        :param seconds: ignored for ftp storage
        :param key:     ignored for ftp storage
        :return:        the download url that can be used to access the storage object
        :raises:        DownloadUrlBaseUndefinedError
        """
        base = self._download_url_base
        object_name = self._parsed_storage_uri.path.split('/')[-1]
        return _generate_download_url_from_base(base, object_name)

    def get_sanitized_uri(self) -> str:
        return remove_user_info(self._parsed_storage_uri)


@register_storage_protocol("ftps")
class FTPSStorage(FTPStorage):
    def _connect(self) -> FTP:
        ftp_client = ftplib.FTP_TLS(timeout=DEFAULT_FTP_TIMEOUT)
        ftp_client.connect(self._hostname, port=self._port)
        self._configure_keepalive(ftp_client)
        ftp_client.login(self._username, self._password)
        ftp_client.prot_p()

        return ftp_client
