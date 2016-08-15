import boto3
import boto3.s3.transfer
import distutils.dir_util
import ftplib
import functools
import mimetypes
import os
import os.path
import re
import shutil
import urllib
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

    def save_to_directory(self, directory_path):
        raise NotImplementedError(
            "{0} does not implement 'save_to_directory'".format(self._class_name()))

    def load_from_filename(self, file_path):
        raise NotImplementedError(
            "{0} does not implement 'load_from_filename'".format(self._class_name()))

    def load_from_file(self, in_file):
        raise NotImplementedError(
            "{0} does not implement 'load_from_file'".format(self._class_name()))

    def load_from_directory(self, directory_path):
        raise NotImplementedError(
            "{0} does not implement 'load_from_directory'".format(self._class_name()))

    def delete(self):
        raise NotImplementedError("{0} does not implement 'delete'".format(self._class_name()))

    def delete_directory(self):
        raise NotImplementedError(
            "{0} does not implement 'delete_directory'".format(self._class_name()))

    def get_download_url(self, seconds=60, key=None):
        raise NotImplementedError(
            "{0} does not implement 'get_download_url'".format(self._class_name()))


@register_storage_protocol("file")
class LocalStorage(Storage):
    """LocalStorage is a local file storage object.

    The URI for working with local file storage has the following format:

      file:///some/path/to/a/file.txt?[download_url_base=<URL-ENCODED-URL>]

      file:///some/path/to/a/directory?[download_url_base=<URL-ENCODED-URL>]

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

    def save_to_directory(self, destination_directory):
        distutils.dir_util.copy_tree(self._parsed_storage_uri.path, destination_directory)

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

    def load_from_directory(self, source_directory):
        self._ensure_exists()
        distutils.dir_util.copy_tree(source_directory, self._parsed_storage_uri.path)

    def delete(self):
        os.remove(self._parsed_storage_uri.path)

    def delete_directory(self):
        shutil.rmtree(self._parsed_storage_uri.path, True)

    def get_download_url(self, seconds=60, key=None):
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

        context = pyrax.create_context(id_type="pyrax.base_identity.BaseIdentity",
                                       username=username, password=password,
                                       api_key=api_key, tenant_id=tenant_id)
        context.auth_endpoint = auth_endpoint
        context.authenticate()
        self._cloudfiles = context.get_client("swift", region, public=public)

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

            self._cloudfiles.download_object(container_name, file, directory, structure=False)

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
                self._upload_file(
                    os.path.join(root, file), object_path=os.path.join(container_path, file))

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

        context = pyrax.create_context("rackspace", username=username, password=password)
        context.authenticate()
        self._cloudfiles = context.get_client("cloudfiles", region, public=public)


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

    def _connect(self):
        ftp_client = ftplib.FTP()
        ftp_client.connect(self._hostname, port=self._port)
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
        ftp_client = ftplib.FTP_TLS()
        ftp_client.connect(self._hostname, port=self._port)
        ftp_client.login(self._username, self._password)
        ftp_client.prot_p()

        return ftp_client


@register_storage_protocol("s3")
class S3Storage(Storage):
    def __init__(self, storage_uri):
        super(S3Storage, self).__init__(storage_uri)
        self._access_key = urllib.unquote(self._parsed_storage_uri.username)
        self._access_secret = urllib.unquote(self._parsed_storage_uri.password)
        self._bucket = self._parsed_storage_uri.hostname
        self._keyname = self._parsed_storage_uri.path.replace("/", "", 1)
        query = urlparse.parse_qs(self._parsed_storage_uri.query)
        self._region = query.get("region", [None])[0]

    def _connect(self):
        aws_session = boto3.session.Session(
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._access_secret,
            region_name=self._region)

        return aws_session.client("s3")

    def save_to_filename(self, file_path):
        client = self._connect()

        transfer = boto3.s3.transfer.S3Transfer(client)
        transfer.download_file(self._bucket, self._keyname, file_path)

    def save_to_file(self, out_file):
        client = self._connect()

        response = client.get_object(Bucket=self._bucket, Key=self._keyname)

        while True:
            chunk = response["Body"].read(_LARGE_CHUNK)
            out_file.write(chunk)
            if not chunk:
                break

    def save_to_directory(self, directory_path):
        client = self._connect()
        directory_prefix = "{}/".format(self._keyname)
        dir_object = client.list_objects(Bucket=self._bucket, Prefix=directory_prefix)
        dir_contents = dir_object["Contents"]

        for file in dir_contents:
            file_key = file["Key"].replace(self._keyname, "", 1)

            if file_key and not file_key.endswith("/"):
                file_path = os.path.dirname(file_key)

                if not os.path.exists(directory_path + file_path):
                    os.makedirs(directory_path + file_path)

                client.download_file(
                    self._bucket, file["Key"], directory_path + file_key)

    def load_from_filename(self, file_path):
        client = self._connect()

        transfer = boto3.s3.transfer.S3Transfer(client)
        transfer.upload_file(file_path, self._bucket, self._keyname)

    def load_from_file(self, in_file):
        client = self._connect()

        client.put_object(Bucket=self._bucket, Key=self._keyname, Body=in_file)

    def load_from_directory(self, source_directory):
        client = self._connect()

        for root, _, files in os.walk(source_directory):
            relative_path = root.replace(source_directory, self._keyname, 1)

            for file in files:
                upload_path = os.path.join(relative_path, file)
                client.upload_file(os.path.join(root, file), self._bucket, upload_path)

    def delete(self):
        client = self._connect()
        client.delete_object(Bucket=self._bucket, Key=self._keyname)

    def delete_directory(self):
        client = self._connect()
        directory_prefix = "{}/".format(self._keyname)
        dir_object = client.list_objects(Bucket=self._bucket, Prefix=directory_prefix)
        object_keys = [{"Key": o.get("Key", None)} for o in dir_object["Contents"]]
        client.delete_objects(
            Bucket=self._bucket,
            Delete={
                "Objects": object_keys
            })

    def get_download_url(self, seconds=60, key=None):
        client = self._connect()

        return client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self._bucket, "Key": self._keyname},
            ExpiresIn=seconds
        )


def get_storage(storage_uri):
    storage_type = storage_uri.split("://")[0]
    return _STORAGE_TYPES[storage_type](storage_uri)
