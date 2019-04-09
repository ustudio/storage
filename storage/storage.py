import Queue
import threading
import urlparse


_STORAGE_TYPES = {}         # maintains supported storage protocols
_LARGE_CHUNK = 32 * 1024 * 1024

DEFAULT_SWIFT_TIMEOUT = 60

"""Socket timeout (float seconds) for FTP transfers."""
DEFAULT_FTP_TIMEOUT = 60.0

"""Enable (1) or disable (0) KEEPALIVE probes for FTP command socket"""
DEFAULT_FTP_KEEPALIVE_ENABLE = 1

"""Socket KEEPALIVE Probes count for FTP transfers."""
DEFAULT_FTP_KEEPCNT = 5

"""Socket KEEPALIVE idle timeout for FTP transfers."""
DEFAULT_FTP_KEEPIDLE = 60

"""Socket KEEPALIVE interval for FTP transfers."""
DEFAULT_FTP_KEEPINTVL = 60


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


class TimeoutError(IOError):
    """Exception raised by timeout when a blocking operation times out."""
    pass


def timeout(seconds, worker):
    result_queue = Queue.Queue()

    def wrapper():
        try:
            result_queue.put(worker())
        except BaseException as e:
            result_queue.put(e)

    thread = threading.Thread(target=wrapper)
    thread.daemon = True
    thread.start()

    try:
        result = result_queue.get(True, seconds)
    except Queue.Empty:
        raise TimeoutError()

    if isinstance(result, BaseException):
        raise result
    return result


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


def _generate_download_url_from_base(base, object_name):
    """Generate a download url by joining the base with the storage object_name.

    If the base is not defined, raise an exception.
    """
    if base is None:
        raise DownloadUrlBaseUndefinedError("The storage uri has no download_url_base defined.")

    return urlparse.urljoin(base, object_name)


class InvalidStorageUri(RuntimeError):
    """Invalid storage URI was specified."""
    pass


def get_storage(storage_uri):
    storage_type = storage_uri.split("://")[0]
    return _STORAGE_TYPES[storage_type](storage_uri)
