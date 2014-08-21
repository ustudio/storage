import ftplib
import functools
import os
import os.path
import shutil
import urlparse

_STORAGE_TYPES = {}         # maintains supported storage protocols


def register_storage_protocol(scheme):
    """Register a storage protocol with the storage library by associating
    a scheme with the specified storage class (aClass)."""

    def decorate_storage_protocol(aClass):

        _STORAGE_TYPES[scheme] = aClass
        urlparse.uses_query.append(scheme)
        return aClass

    return decorate_storage_protocol


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


@register_storage_protocol("file")
class LocalStorage(Storage):

    def save_to_filename(self, file_path):
        shutil.copy(self._parsed_storage_uri.path, file_path)

    def save_to_file(self, out_file):
        with open(self._parsed_storage_uri.path) as in_file:
            for chunk in in_file:
                out_file.write(chunk)

    def _ensure_exists(self):
        dirname = os.path.dirname(self._parsed_storage_uri.path)

        if not os.path.exists(dirname):
            os.makedirs(dirname)

    def load_from_filename(self, file_path):
        self._ensure_exists()

        shutil.copy(file_path, self._parsed_storage_uri.path)

    def load_from_file(self, in_file):
        self._ensure_exists()

        with open(self._parsed_storage_uri.path, "wb") as out_file:
            for chunk in in_file:
                out_file.write(chunk)

    def delete(self):
        os.remove(self._parsed_storage_uri.path)


import pyrax


class InvalidStorageUri(RuntimeError):
    """Invalid storage URI was specified."""
    pass

@register_storage_protocol("swift")
class SwiftStorage(Storage):
    """SwiftStorage is a storage object based on OpenStack Swift object_store.

    The URI for working with Swift storage has the following format:

      swift://username:password@container/object?auth_endpoint=URL&region=REGION&tenant_id=ID[&api_key=APIKEY][&public={True|False}]

    """

    def __init__(self, *args, **kwargs):
        super(SwiftStorage, self).__init__(*args, **kwargs)
        self.username = None
        self.password = None
        self.auth_endpoint = None
        self.region = None
        self.api_key = None
        self.tenant_id = None
        self.public = True

    def _authenticate(self):
        auth, _ = self._parsed_storage_uri.netloc.split("@")
        self.username, self.password = auth.split(":", 1)

        query = urlparse.parse_qs(self._parsed_storage_uri.query)
        self.public = query.get("public", ["True"])[0].lower() != "false"
        self.api_key = query.get("api_key", [None])[0]
        self.tenant_id = query.get("tenant_id", [None])[0]
        self.region = query.get("region", [None])[0]
        auth_endpoint = query.get("auth_endpoint", [None])[0]
        if auth_endpoint:
            self.auth_endpoint = auth_endpoint

        # minimum set of required params
        if not self.username:
            raise InvalidStorageUri("username is required.")
        if not self.password:
            raise InvalidStorageUri("password is required.")
        if not self.auth_endpoint:
            raise InvalidStorageUri("auth_endpoint is required.")
        if not self.region:
            raise InvalidStorageUri("region is required.")
        if not self.tenant_id:
            raise InvalidStorageUri("tenant_id is required.")

        context = pyrax.create_context(id_type="pyrax.base_identity.BaseIdentity",
                                       username=self.username, password=self.password,
                                       api_key=self.api_key, tenant_id=self.tenant_id)
        context.auth_endpoint = self.auth_endpoint
        context.authenticate()
        self._cloudfiles = context.get_client("swift", self.region, public=self.public)

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

    """

    def _authenticate(self):
        auth, _ = self._parsed_storage_uri.netloc.split("@")
        username, password = auth.split(":", 1)

        query = urlparse.parse_qs(self._parsed_storage_uri.query)
        public = query.get("public", ["True"])[0].lower() != "false"

        context = pyrax.create_context("rackspace", username=username, password=password)
        context.authenticate()
        self._cloudfiles = context.get_client("cloudfiles", "DFW", public=public)


@register_swift_protocol(scheme="hpcloud",
                         auth_endpoint="https://region-a.geo-1.identity.hpcloudsvc.com:35357/v2.0/")
class HPCloudStorage(SwiftStorage):
    """HP Cloud (Helion) Swift storage.

    The URI for working with HP Cloud Storage has the following format:

      hpcloud://username:password@container/object?region=REGION&tenant_id=ID[&api_key=APIKEY][&public={True|False}]

    """
    pass


@register_storage_protocol("ftp")
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


@register_storage_protocol("ftps")
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


def get_storage(storage_uri):
    storage_type = storage_uri.split("://")[0]
    return _STORAGE_TYPES[storage_type](storage_uri)
