import functools
import mimetypes
import os
import urllib
import urlparse

import pyrax

from . import retry
from .storage import Storage, InvalidStorageUri, timeout, register_storage_protocol, \
    _LARGE_CHUNK, DEFAULT_SWIFT_TIMEOUT


@register_storage_protocol("swift")
class SwiftStorage(Storage):
    """SwiftStorage is a storage object based on OpenStack Swift object_store.

    The URI for working with Swift storage has the following format:

      swift://username:password@container/object?
      auth_endpoint=URL&region=REGION&tenant_id=ID[&api_key=APIKEY][&public={True|False}]
      [&download_url_key=TEMPURLKEY]

      swift://username:password@container/directory/of/objects?
      auth_endpoint=URL&region=REGION&tenant_id=ID[&api_key=APIKEY][&public={True|False}]
      [&download_url_key=TEMPURLKEY]

    """

    def __init__(self, *args, **kwargs):
        super(SwiftStorage, self).__init__(*args, **kwargs)
        self.auth_endpoint = None

    def _authenticate(self):
        auth, _ = self._parsed_storage_uri.netloc.split("@")
        username, password = auth.split(":", 1)

        query = urlparse.parse_qs(self._parsed_storage_uri.query)
        public = query.get("public", ["True"])[0].lower() != "false"
        api_key = query.get("api_key", [None])[0]
        tenant_id = query.get("tenant_id", [None])[0]
        region = query.get("region", [None])[0]
        auth_endpoint = query.get("auth_endpoint", [None])[0]

        # This is the only auth parameter that's saved for later
        self.download_url_key = query.get("download_url_key", [None])[0]

        if auth_endpoint is None:
            auth_endpoint = self.auth_endpoint

        if not auth_endpoint:
            raise InvalidStorageUri("auth_endpoint is required.")

        # minimum set of required params
        if not username:
            raise InvalidStorageUri("username is required.")
        if not password:
            raise InvalidStorageUri("password is required.")
        if not region:
            raise InvalidStorageUri("region is required.")
        if not tenant_id:
            raise InvalidStorageUri("tenant_id is required.")

        def create_swift_context_and_authenticate():
            context = pyrax.create_context(id_type="pyrax.base_identity.BaseIdentity",
                                           username=username, password=password,
                                           api_key=api_key, tenant_id=tenant_id)
            context.auth_endpoint = auth_endpoint
            context.authenticate()
            return context

        context = timeout(DEFAULT_SWIFT_TIMEOUT, create_swift_context_and_authenticate)
        self._cloudfiles = context.get_client("swift", region, public=public)
        self._cloudfiles.timeout = DEFAULT_SWIFT_TIMEOUT

    def _get_container_and_object_names(self):
        _, container_name = self._parsed_storage_uri.netloc.split("@")
        object_name = self._parsed_storage_uri.path[1:]
        return container_name, object_name

    def _list_container_objects(self, container_name, prefix):
        container_objects = self._cloudfiles.list_container_objects(container_name, prefix=prefix)
        directories = []
        files = []

        for container_object in container_objects:
            if container_object.content_type == "application/directory":
                directories.append(container_object)
            else:
                files.append(container_object)

        return directories, files

    def save_to_filename(self, file_path):
        with open(file_path, "wb") as output_fp:
            self.save_to_file(output_fp)

    def save_to_file(self, out_file):
        self._authenticate()
        container_name, object_name = self._get_container_and_object_names()

        for chunk in self._cloudfiles.fetch_object(
                container_name, object_name, chunk_size=_LARGE_CHUNK):
            out_file.write(chunk)

    def save_to_directory(self, destination_directory):
        self._authenticate()
        container_name, prefix = self._get_container_and_object_names()

        directories, files = self._list_container_objects(container_name, prefix)

        for directory in directories:
            if directory.name == prefix:
                continue

            directory_path = directory.name.split('/', 1).pop()
            target_directory = os.path.join(destination_directory, directory_path)

            if not os.path.exists(target_directory):
                os.makedirs(target_directory)

        for file in files:
            directory = os.path.dirname(file.name.replace(prefix, destination_directory, 1))

            if not os.path.exists(directory):
                os.makedirs(directory)

            retry.attempt(
                self._cloudfiles.download_object,
                container_name,
                file,
                directory,
                structure=False)

    def _upload_file(self, file_or_path, object_path=None):
        self._authenticate()
        container_name, object_name = self._get_container_and_object_names()
        kwargs = {}
        mimetype = mimetypes.guess_type(object_name)[0]
        if mimetype is not None:
            kwargs["content_type"] = mimetype

        object_location = object_path or object_name

        self._cloudfiles.upload_file(container_name, file_or_path, object_location, **kwargs)

    def load_from_filename(self, file_path):
        self._upload_file(file_path)

    def load_from_file(self, in_file):
        self._upload_file(in_file)

    def load_from_directory(self, source_directory):
        self._authenticate()
        container_name, object_name = self._get_container_and_object_names()

        for root, _, files in os.walk(source_directory):
            container_path = root.replace(source_directory, object_name, 1)

            for file in files:
                retry.attempt(
                    self._upload_file,
                    os.path.join(root, file),
                    object_path=os.path.join(container_path, file))

    def delete(self):
        self._authenticate()
        container_name, object_name = self._get_container_and_object_names()
        self._cloudfiles.delete_object(container_name, object_name)

    def delete_directory(self):
        self._authenticate()
        container_name, object_name = self._get_container_and_object_names()

        # recursively find and delete all objects below object_name
        _, objects = self._list_container_objects(container_name, object_name)

        for obj in objects:
            self._cloudfiles.delete_object(container_name, obj.name)

    def get_download_url(self, seconds=60, key=None):
        self._authenticate()
        container_name, object_name = self._get_container_and_object_names()
        temp_url_key = key if key is not None else self.download_url_key

        download_url = self._cloudfiles.get_temp_url(
            container_name, object_name, seconds=seconds, method="GET", key=temp_url_key)

        parsed_url = urlparse.urlparse(download_url)
        parsed_url = parsed_url._replace(path=urllib.quote(parsed_url.path))

        return urlparse.urlunparse(parsed_url)


def register_swift_protocol(scheme, auth_endpoint):
    """Register a Swift based storage protocol under the specified scheme."""

    def decorate_swift_protocol(aClass):

        if not issubclass(aClass, SwiftStorage):
            raise Exception("'{0}' must subclass from SwiftStorage".format(aClass))

        @register_storage_protocol(scheme)
        class SwiftWrapper(aClass):
            __doc__ = aClass.__doc__

            def __init__(self, *args, **kwargs):
                super(SwiftWrapper, self).__init__(*args, **kwargs)
                self.auth_endpoint = auth_endpoint

        functools.update_wrapper(SwiftWrapper, aClass, ('__name__', '__module__'), ())
        return SwiftWrapper
    return decorate_swift_protocol


@register_swift_protocol(scheme="cloudfiles",
                         auth_endpoint=None)
class CloudFilesStorage(SwiftStorage):
    """Rackspace Cloudfiles storage.

    The URI for working with Rackspace Cloudfiles based storage has the following format:

      cloudfiles://username:key@container/object[?public={True|False}]

      cloudfiles://username:key@container/directory/of/objects[?public={True|False}]

    """

    def _authenticate(self):
        auth, _ = self._parsed_storage_uri.netloc.split("@")
        username, password = auth.split(":", 1)

        query = urlparse.parse_qs(self._parsed_storage_uri.query)
        public = query.get("public", ["True"])[0].lower() != "false"
        region = query.get("region", ["DFW"])[0]
        self.download_url_key = query.get("download_url_key", [None])[0]

        def create_cloudfiles_context_and_authenticate():
            context = pyrax.create_context("rackspace", username=username, password=password)
            context.authenticate()
            return context

        context = timeout(DEFAULT_SWIFT_TIMEOUT, create_cloudfiles_context_and_authenticate)
        self._cloudfiles = context.get_client("cloudfiles", region, public=public)
        self._cloudfiles.timeout = DEFAULT_SWIFT_TIMEOUT

        if self.download_url_key is None:
            temp_url_keys = filter(
                lambda (k, v): k.lower().endswith("temp_url_key"),
                self._cloudfiles.get_account_metadata().items())

            if len(temp_url_keys) > 0:
                self.download_url_key = temp_url_keys[0][1]
