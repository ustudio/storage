import shutil
import urlparse


class Storage(object):

    def __init__(self, storage_uri):
        self._storage_uri = storage_uri
        self._parsed_storage_uri = urlparse.urlparse(storage_uri)

    def save_to_filename(self, file_path):
        raise NotImplementedError("A Storage class must implement 'save_to_filename'")

    def load_from_filename(self, file_path):
        raise NotImplementedError("A Storage class must implement 'load_from_filename'")


class LocalStorage(Storage):

    def save_to_filename(self, file_path):
        input_path = self._storage_uri.split("://", 1)[1]
        shutil.copy(input_path, file_path)

    def load_from_filename(self, file_path):
        output_path = self._storage_uri.split("://", 1)[1]
        shutil.copy(file_path, output_path)


import pyrax
pyrax.set_setting("identity_type", "rackspace")


class CloudFilesStorage(Storage):

    def _authenticate(self):
        auth, _ = self._parsed_storage_uri.netloc.split("@")
        username, password = auth.split(":", 1)
        pyrax.set_credentials(username, password)

    def _get_container_and_object_names(self):
        _, container_name = self._parsed_storage_uri.netloc.split("@")
        object_name = self._parsed_storage_uri.path[1:]
        return container_name, object_name

    def save_to_filename(self, file_path):
        self._authenticate()
        container_name, object_name = self._get_container_and_object_names()
        with open(file_path, "wb") as output_fp:
            for chunk in pyrax.cloudfiles.fetch_object(
                    container_name, object_name, chunk_size=4096):
                output_fp.write(chunk)

    def load_from_filename(self, file_path):
        self._authenticate()
        container_name, object_name = self._get_container_and_object_names()
        pyrax.cloudfiles.upload_file(container_name, file_path, object_name)


_STORAGE_TYPES = {
    "file": LocalStorage,
    "cloudfiles": CloudFilesStorage
}


def get_storage(storage_uri):
    storage_type = storage_uri.split("://")[0]
    return _STORAGE_TYPES[storage_type](storage_uri)
