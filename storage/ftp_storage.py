import ftplib
import os
import re
import socket
import urlparse

from .storage import Storage, register_storage_protocol, _generate_download_url_from_base, \
    DEFAULT_FTP_TIMEOUT, DEFAULT_FTP_KEEPALIVE_ENABLE, DEFAULT_FTP_KEEPCNT, \
    DEFAULT_FTP_KEEPIDLE, DEFAULT_FTP_KEEPINTVL


@register_storage_protocol("ftp")
class FTPStorage(Storage):
    """FTP storage.

    The URI for working with FTP storage has the following format:

      ftp://username:password@hostname/path/to/file.txt[?download_url_base=<URL-ENCODED-URL>]

      ftp://username:password@hostname/path/to/directory[?download_url_base=<URL-ENCODED-URL>]

    If the ftp storage has access via HTTP, then a download_url_base can be specified
    that will allow get_download_url() to return access to that object via HTTP.
    """

    def __init__(self, storage_uri):
        super(FTPStorage, self).__init__(storage_uri)
        self._username = self._parsed_storage_uri.username
        self._password = self._parsed_storage_uri.password
        self._hostname = self._parsed_storage_uri.hostname
        self._port = \
            self._parsed_storage_uri.port if self._parsed_storage_uri.port is not None else 21
        query = urlparse.parse_qs(self._parsed_storage_uri.query)
        self._download_url_base = query.get("download_url_base", [None])[0]

    def _configure_keepalive(self, ftp_client):
        ftp_client.sock.setsockopt(
            socket.SOL_SOCKET, socket.SO_KEEPALIVE, DEFAULT_FTP_KEEPALIVE_ENABLE)

        if hasattr(socket, "TCP_KEEPCNT"):
            ftp_client.sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, DEFAULT_FTP_KEEPCNT)

        if hasattr(socket, "TCP_KEEPIDLE"):
            ftp_client.sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, DEFAULT_FTP_KEEPIDLE)

        if hasattr(socket, "TCP_KEEPINTVL"):
            ftp_client.sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, DEFAULT_FTP_KEEPINTVL)

    def _connect(self):
        ftp_client = ftplib.FTP(timeout=DEFAULT_FTP_TIMEOUT)
        ftp_client.connect(self._hostname, port=self._port)

        self._configure_keepalive(ftp_client)

        ftp_client.login(self._username, self._password)

        return ftp_client

    def _cd_to_file(self, ftp_client):
        directory, filename = os.path.split(self._parsed_storage_uri.path.lstrip("/"))
        ftp_client.cwd(directory)
        return filename

    def _list(self, ftp_client):
        directory_listing = []

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

    def _walk(self, ftp_client, target_directory=None):
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

    def _create_directory_structure(self, ftp_client, target_path, restore=False):
        directories = target_path.lstrip('/').split('/')

        if restore:
            restore = ftp_client.pwd()

        for target_directory in directories:
            dirs, _ = self._list(ftp_client)

            # TODO (phd): warn the user that a file exists with the name of their target dir
            if target_directory not in dirs:
                ftp_client.mkd(target_directory)

            ftp_client.cwd(target_directory)

        if restore:
            ftp_client.cwd(restore)

    def save_to_filename(self, file_path):
        with open(file_path, "wb") as output_file:
            self.save_to_file(output_file)

    def save_to_file(self, out_file):
        ftp_client = self._connect()
        filename = self._cd_to_file(ftp_client)

        ftp_client.retrbinary("RETR {0}".format(filename), callback=out_file.write)

    def save_to_directory(self, destination_directory):
        ftp_client = self._connect()
        base_ftp_path = self._parsed_storage_uri.path

        ftp_client.cwd(base_ftp_path)

        for root, dirs, files in self._walk(ftp_client):
            relative_path = "/{}".format(root).replace(base_ftp_path, destination_directory, 1)

            if not os.path.exists(relative_path):
                os.makedirs(relative_path)

            os.chdir(relative_path)

            for filename in files:
                with open(os.path.join(relative_path, filename), "wb") as output_file:
                    ftp_client.retrbinary("RETR {0}".format(filename), callback=output_file.write)

    def load_from_filename(self, file_path):
        with open(file_path, "rb") as input_file:
            self.load_from_file(input_file)

    def load_from_file(self, in_file):
        ftp_client = self._connect()
        filename = self._cd_to_file(ftp_client)

        ftp_client.storbinary("STOR {0}".format(filename), in_file)

    def load_from_directory(self, source_directory):
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

    def delete(self):
        ftp_client = self._connect()
        filename = self._cd_to_file(ftp_client)
        ftp_client.delete(filename)

    def delete_directory(self):
        ftp_client = self._connect()
        base_ftp_path = self._parsed_storage_uri.path
        ftp_client.cwd(base_ftp_path)

        directories_to_remove = []
        for root, directories, files in self._walk(ftp_client):
            for filename in files:
                ftp_client.delete("/{}/{}".format(root, filename))

            directories_to_remove.append("/{}".format(root))

        # delete directories _after_ removing files from directories
        # directories should be removed in reverse order - leaf directories before
        # parent directories - since there is no recursive delete
        directories_to_remove.sort(reverse=True)
        for directory in directories_to_remove:
            ftp_client.rmd("{}".format(directory))

    def get_download_url(self, seconds=60, key=None):
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
        return _generate_download_url_from_base(
            self._download_url_base, self._parsed_storage_uri.path.split('/')[-1])


@register_storage_protocol("ftps")
class FTPSStorage(FTPStorage):
    def _connect(self):
        ftp_client = ftplib.FTP_TLS(timeout=DEFAULT_FTP_TIMEOUT)
        ftp_client.connect(self._hostname, port=self._port)
        self._configure_keepalive(ftp_client)
        ftp_client.login(self._username, self._password)
        ftp_client.prot_p()

        return ftp_client
