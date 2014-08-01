import ftplib
import os
import os.path
import shutil
import urlparse

urlparse.uses_query.append("cloudfiles")


class Storage(object):

    def __init__(self, storage_uri):
        self._storage_uri = storage_uri
        self._parsed_storage_uri = urlparse.urlparse(storage_uri)

    def _class_name(self):
        return self.__class__.__name__

    def save_to_filename(self, file_path):
        raise NotImplementedError(
            "{0} does not implement 'save_to_filename'".format(self._class_name()))

    def save_to_file(self, out_file):
        raise NotImplementedError(
            "{0} does not implement 'save_to_file'".format(self._class_name()))

    def load_from_filename(self, file_path):
        raise NotImplementedError(
            "{0} does not implement 'load_from_filename'".format(self._class_name()))

    def load_from_file(self, in_file):
        raise NotImplementedError(
            "{0} does not implement 'load_from_file'".format(self._class_name()))

    def delete(self):
        raise NotImplementedError("{0} does not implement 'delete'".format(self._class_name()))


class LocalStorage(Storage):

    def save_to_filename(self, file_path):
        shutil.copy(self._parsed_storage_uri.path, file_path)

    def save_to_file(self, out_file):
        with open(self._parsed_storage_uri.path) as in_file:
            for chunk in in_file:
                out_file.write(chunk)

    def load_from_filename(self, file_path):
        shutil.copy(file_path, self._parsed_storage_uri.path)

    def load_from_file(self, in_file):
        with open(self._parsed_storage_uri.path, "wb") as out_file:
            for chunk in in_file:
                out_file.write(chunk)

    def delete(self):
        os.remove(self._parsed_storage_uri.path)


import pyrax


class CloudFilesStorage(Storage):

    def _authenticate(self):
        auth, _ = self._parsed_storage_uri.netloc.split("@")
        username, password = auth.split(":", 1)

        query = urlparse.parse_qs(self._parsed_storage_uri.query)
        public = query.get("public", ["True"])[0].lower() != "false"

        context = pyrax.create_context("rackspace", username, password)
        context.authenticate()
        self._cloudfiles = context.get_client("cloudfiles", "DFW", public=public)

    def _get_container_and_object_names(self):
        _, container_name = self._parsed_storage_uri.netloc.split("@")
        object_name = self._parsed_storage_uri.path[1:]
        return container_name, object_name

    def save_to_filename(self, file_path):
        with open(file_path, "wb") as output_fp:
            self.save_to_file(output_fp)

    def save_to_file(self, out_file):
        self._authenticate()
        container_name, object_name = self._get_container_and_object_names()

        for chunk in self._cloudfiles.fetch_object(
                container_name, object_name, chunk_size=4096):
            out_file.write(chunk)

    def _upload_file(self, file_or_path):
        self._authenticate()
        container_name, object_name = self._get_container_and_object_names()
        self._cloudfiles.upload_file(container_name, file_or_path, object_name)

    def load_from_filename(self, file_path):
        self._upload_file(file_path)

    def load_from_file(self, in_file):
        self._upload_file(in_file)

    def delete(self):
        self._authenticate()
        container_name, object_name = self._get_container_and_object_names()
        self._cloudfiles.delete_object(container_name, object_name)


class FTPStorage(Storage):
    def _connect(self):
        username = self._parsed_storage_uri.username
        password = self._parsed_storage_uri.password
        hostname = self._parsed_storage_uri.hostname
        port = 21

        ftp_client = ftplib.FTP()
        ftp_client.connect(hostname, port=port)
        ftp_client.login(username, password)

        return ftp_client

    def _cd_to_file(self, ftp_client):
        directory, filename = os.path.split(self._parsed_storage_uri.path.lstrip("/"))
        ftp_client.cwd(directory)
        return filename

    def save_to_filename(self, file_path):
        with open(file_path, "wb") as output_file:
            self.save_to_file(output_file)

    def save_to_file(self, out_file):
        ftp_client = self._connect()
        filename = self._cd_to_file(ftp_client)

        ftp_client.retrbinary("RETR {0}".format(filename), callback=out_file.write)

    def load_from_filename(self, file_path):
        with open(file_path, "rb") as input_file:
            self.load_from_file(input_file)

    def load_from_file(self, in_file):
        ftp_client = self._connect()
        filename = self._cd_to_file(ftp_client)

        ftp_client.storbinary("STOR {0}".format(filename), in_file)

    def delete(self):
        ftp_client = self._connect()
        filename = self._cd_to_file(ftp_client)
        ftp_client.delete(filename)


class FTPSStorage(FTPStorage):
    def _connect(self):
        username = self._parsed_storage_uri.username
        password = self._parsed_storage_uri.password
        hostname = self._parsed_storage_uri.hostname
        port = 21

        ftp_client = ftplib.FTP_TLS()
        ftp_client.connect(hostname, port=port)
        ftp_client.login(username, password)
        ftp_client.prot_p()

        return ftp_client


_STORAGE_TYPES = {
    "file": LocalStorage,
    "cloudfiles": CloudFilesStorage,
    "ftp": FTPStorage,
    "ftps": FTPSStorage
}


def get_storage(storage_uri):
    storage_type = storage_uri.split("://")[0]
    return _STORAGE_TYPES[storage_type](storage_uri)
