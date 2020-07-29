import queue
import threading
from urllib.parse import ParseResult, urljoin, urlparse, uses_query

from typing import BinaryIO, Callable, Dict, List, Optional, Type, TypeVar, Union

from storage.url_parser import sanitize_resource_uri


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


def register_storage_protocol(scheme: str) -> Callable[[Type["Storage"]], Type["Storage"]]:
    """Register a storage protocol with the storage library by associating
    a scheme with the specified storage class (aClass)."""

    def decorate_storage_protocol(aClass: Type["Storage"]) -> Type["Storage"]:

        _STORAGE_TYPES[scheme] = aClass
        uses_query.append(scheme)
        return aClass

    return decorate_storage_protocol


class NotFoundError(Exception):
    pass


class DownloadUrlBaseUndefinedError(Exception):
    """Exception raised when a download url has been requested and
    no download_url_base has been defined for the storage object.

    This exception is used with local file storage and FTP file storage objects.
    """
    pass


class TimeoutError(IOError):
    """Exception raised by timeout when a blocking operation times out."""
    pass


T = TypeVar("T")


def timeout(seconds: int, worker: Callable[[], T]) -> T:
    result_queue: queue.Queue[Union[BaseException, T]] = queue.Queue()

    def wrapper() -> None:
        try:
            result_queue.put(worker())
        except BaseException as e:
            result_queue.put(e)

    thread = threading.Thread(target=wrapper)
    thread.daemon = True
    thread.start()

    try:
        result = result_queue.get(True, seconds)
    except queue.Empty:
        raise TimeoutError()

    if isinstance(result, BaseException):
        raise result
    return result


class Storage(object):
    _storage_uri: str
    _parsed_storage_uri: ParseResult

    def __init__(self, storage_uri: str) -> None:
        self._storage_uri = storage_uri
        self._parsed_storage_uri = urlparse(storage_uri)
        self._validate_parsed_uri()

    def _validate_parsed_uri(self) -> None:
        pass

    def _class_name(self) -> str:
        return self.__class__.__name__

    def save_to_filename(self, file_path: str) -> None:
        raise NotImplementedError(
            "{} does not implement 'save_to_filename'".format(self._class_name()))

    def save_to_file(self, out_file: BinaryIO) -> None:
        raise NotImplementedError(
            "{} does not implement 'save_to_file'".format(self._class_name()))

    def save_to_directory(self, directory_path: str) -> None:
        raise NotImplementedError(
            "{} does not implement 'save_to_directory'".format(self._class_name()))

    def load_from_filename(self, file_path: str) -> None:
        raise NotImplementedError(
            "{} does not implement 'load_from_filename'".format(self._class_name()))

    def load_from_file(self, in_file: BinaryIO) -> None:
        raise NotImplementedError(
            "{} does not implement 'load_from_file'".format(self._class_name()))

    def load_from_directory(self, directory_path: str) -> None:
        raise NotImplementedError(
            "{} does not implement 'load_from_directory'".format(self._class_name()))

    def delete(self) -> None:
        raise NotImplementedError("{} does not implement 'delete'".format(self._class_name()))

    def delete_directory(self) -> None:
        raise NotImplementedError(
            "{} does not implement 'delete_directory'".format(self._class_name()))

    def get_download_url(self, seconds: int = 60, key: Optional[str] = None) -> str:
        raise NotImplementedError(
            "{} does not implement 'get_download_url'".format(self._class_name()))

    def get_sanitized_uri(self) -> str:
        return sanitize_resource_uri(self._parsed_storage_uri)


def _generate_download_url_from_base(base: Union[str, None], object_name: str) -> str:
    """Generate a download url by joining the base with the storage object_name.

    If the base is not defined, raise an exception.
    """
    if base is None:
        raise DownloadUrlBaseUndefinedError("The storage uri has no download_url_base defined.")

    return urljoin(base, object_name)


class InvalidStorageUri(RuntimeError):
    """Invalid storage URI was specified."""
    pass


def get_storage(storage_uri: str) -> Storage:
    storage_type = urlparse(storage_uri).scheme
    try:
        return _STORAGE_TYPES[storage_type](storage_uri)
    except KeyError:
        raise InvalidStorageUri(f"Invalid storage type '{storage_type}'")


ParsedQuery = Dict[str, List[str]]


def get_optional_query_parameter(parsed_query: ParsedQuery, parameter: str) -> Optional[str]:
    query_arg = parsed_query.get(parameter, [])
    if len(query_arg) > 1:
        raise InvalidStorageUri(f"Too many `{parameter}` query values.")
    return query_arg[0] if len(query_arg) else None
