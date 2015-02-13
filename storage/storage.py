import ftplib
import functools
import mimetypes
import os
import os.path
import shutil
import urlparse

_STORAGE_TYPES = {}         # maintains supported storage protocols
_LARGE_CHUNK = 32 * 1024 * 1024


def register_storage_protocol(scheme):
    """Register a storage protocol with the storage library by associating
    a scheme with the specified storage class (aClass)."""

    def decorate_storage_protocol(aClass):

        _STORAGE_TYPES[scheme] = aClass
        urlparse.uses_query.append(scheme)
        return aClass

    return decorate_storage_protocol


class DownloadUrlBaseUndefinedError(Exception):
    """Exception raised when a download url has been requested and
    no download_url_base has been defined for the storage object.

    This exception is used with local file storage and FTP file storage objects.
    """
    pass

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

    def get_download_url(self, seconds=60, key=None):
        raise NotImplementedError("{0} does not implement 'get_download_url'".format(self._class_name()))


@register_storage_protocol("file")
class LocalStorage(Storage):
    """LocalStorage is a local file storage object.

    The URI for working with local file storage has the following format:

      file:///some/path/to/a/file.txt?[download_url_base=<URL-ENCODED-URL>]

    """
    def __init__(self, storage_uri):
        super(LocalStorage, self).__init__(storage_uri)
        query = urlparse.parse_qs(self._parsed_storage_uri.query)
        self._download_url_base = query.get("download_url_base", [None])[0]

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

    def get_download_url(self, seconds=60, key=None):
        """
        Return a temporary URL allowing access to the storage object.

        If a download_url_base is specified in the storage URI, then a call to get_download_url() will
        return the download_url_base joined with the object name.

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
        return _generate_download_url_from_base(self._download_url_base,
                self._parsed_storage_uri.path.split('/')[-1])


def _generate_download_url_from_base(base, object_name):
    """Generate a download url by joining the base with the storage object_name.

    If the base is not defined, raise an exception.
    """
    if base is None:
        raise DownloadUrlBaseUndefinedError("The storage uri has no download_url_base defined.")

    return urlparse.urljoin(base, object_name)


import pyrax


class InvalidStorageUri(RuntimeError):
    """Invalid storage URI was specified."""
    pass

@register_storage_protocol("swift")
class SwiftStorage(Storage):
    """SwiftStorage is a storage object based on OpenStack Swift object_store.

    The URI for working with Swift storage has the following format:

      swift://username:password@container/object?
      auth_endpoint=URL&region=REGION&tenant_id=ID[&api_key=APIKEY][&public={True|False}]
      [&download_url_key=TEMPURLKEY]

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
        self.download_url_key = None

    def _authenticate(self):
        auth, _ = self._parsed_storage_uri.netloc.split("@")
        self.username, self.password = auth.split(":", 1)

        query = urlparse.parse_qs(self._parsed_storage_uri.query)
        self.public = query.get("public", ["True"])[0].lower() != "false"
        self.api_key = query.get("api_key", [None])[0]
        self.tenant_id = query.get("tenant_id", [None])[0]
        self.region = query.get("region", [None])[0]
        self.auth_endpoint = query.get("auth_endpoint", [None])[0]
        self.download_url_key = query.get("download_url_key", [None])[0]

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
                container_name, object_name, chunk_size=_LARGE_CHUNK):
            out_file.write(chunk)

    def _upload_file(self, file_or_path):
        self._authenticate()
        container_name, object_name = self._get_container_and_object_names()
        kwargs = {}
        mimetype = mimetypes.guess_type(object_name)[0]
        if mimetype is not None:
            kwargs["content_type"] = mimetype
        self._cloudfiles.upload_file(container_name, file_or_path, object_name, **kwargs)

    def load_from_filename(self, file_path):
        self._upload_file(file_path)

    def load_from_file(self, in_file):
        self._upload_file(in_file)

    def delete(self):
        self._authenticate()
        container_name, object_name = self._get_container_and_object_names()
        self._cloudfiles.delete_object(container_name, object_name)

    def get_download_url(self, seconds=60, key=None):
        self._authenticate()
        container_name, object_name = self._get_container_and_object_names()
        temp_url_key = key if key is not None else self.download_url_key

        return self._cloudfiles.get_download_url(container_name, object_name, seconds=seconds,
            method="GET", key=temp_url_key)

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
        region = query.get("region", ["DFW"])[0]

        context = pyrax.create_context("rackspace", username=username, password=password)
        context.authenticate()
        self._cloudfiles = context.get_client("cloudfiles", region, public=public)


@register_swift_protocol(
    scheme="hpcloud", auth_endpoint="https://region-a.geo-1.identity.hpcloudsvc.com:35357/v2.0/")
class HPCloudStorage(SwiftStorage):
    """HP Cloud (Helion) Swift storage.

    The URI for working with HP Cloud Storage has the following format:

      hpcloud://username:password@container/object?
      region=REGION&tenant_id=ID[&api_key=APIKEY][&public={True|False}]

    """
    pass


@register_storage_protocol("ftp")
class FTPStorage(Storage):
    """FTP storage.

    The URI for working with FTP storage has the following format:

      ftp://username:password@hostname/path/to/file.txt[?download_url_base=<URL-ENCODED-URL>]

    If the ftp storage has access via HTTP, then a download_url_base can be specified
    that will allow get_download_url() to return access to that object via HTTP.
    """

    def __init__(self, storage_uri):
        super(FTPStorage, self).__init__(storage_uri)
        self._username = self._parsed_storage_uri.username
        self._password = self._parsed_storage_uri.password
        self._hostname = self._parsed_storage_uri.hostname
        self._port = 21
        query = urlparse.parse_qs(self._parsed_storage_uri.query)
        self._download_url_base = query.get("download_url_base", [None])[0]

    def _connect(self):
        ftp_client = ftplib.FTP()
        ftp_client.connect(self._hostname, port=self._port)
        ftp_client.login(self._username, self._password)

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

    def get_download_url(self, seconds=60, key=None):
        """
        Return a temporary URL allowing access to the storage object.

        If a download_url_base is specified in the storage URI, then a call to get_download_url() will
        return the download_url_base joined with the object name.

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
        return _generate_download_url_from_base(self._download_url_base,
            self._parsed_storage_uri.path.split('/')[-1])


@register_storage_protocol("ftps")
class FTPSStorage(FTPStorage):
    def _connect(self):
        ftp_client = ftplib.FTP_TLS()
        ftp_client.connect(self._hostname, port=self._port)
        ftp_client.login(self._username, self._password)
        ftp_client.prot_p()

        return ftp_client


def get_storage(storage_uri):
    storage_type = storage_uri.split("://")[0]
    return _STORAGE_TYPES[storage_type](storage_uri)
