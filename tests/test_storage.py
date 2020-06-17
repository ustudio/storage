import threading

from storage.storage import get_storage, InvalidStorageUri, register_storage_protocol, Storage
from storage.storage import timeout, TimeoutError, _STORAGE_TYPES
from unittest import mock, TestCase


class TestTimeout(TestCase):
    @mock.patch("threading.Thread", wraps=threading.Thread)
    def test_calls_function_in_thread(self, mock_thread_class: mock.Mock) -> None:
        def worker() -> str:
            self.assertTrue(threading.current_thread().daemon)
            return "success"

        self.assertEqual("success", timeout(5, worker))

        mock_thread_class.assert_called_once_with(target=mock.ANY)

    def test_reraises_exception_raised_by_worker(self) -> None:
        def worker() -> None:
            raise Exception("some error")

        with self.assertRaisesRegex(Exception, "^some error$"):
            timeout(5, worker)

    def test_raises_timeout_error_when_worker_does_not_complete_within_timeout(self) -> None:
        event = threading.Event()

        def worker() -> None:
            event.wait()

        try:
            with self.assertRaises(TimeoutError):
                timeout(0, worker)
        finally:
            event.set()


class TestRegisterStorageProtocol(TestCase):

    def setUp(self) -> None:
        self.scheme = "myscheme"

    def test_register_storage_protocol_updates_storage_types(self) -> None:

        @register_storage_protocol(scheme=self.scheme)
        class MyStorageClass(Storage):
            pass

        self.assertIn(self.scheme, _STORAGE_TYPES)

        uri = "{0}://some/uri/path".format(self.scheme)
        store_obj = get_storage(uri)
        self.assertIsInstance(store_obj, MyStorageClass)

    def test_storage_provider_calls_validation_on_implementation(self) -> None:

        @register_storage_protocol(scheme=self.scheme)
        class ValidatingStorageClass(Storage):
            def _validate_parsed_uri(self) -> None:
                raise InvalidStorageUri("Nope I don't like it.")

        with self.assertRaises(InvalidStorageUri):
            get_storage(f"{self.scheme}://some/uri/path")


class TestGetStorage(TestCase):
    def test_raises_for_unsupported_scheme(self) -> None:
        with self.assertRaises(InvalidStorageUri) as error:
            get_storage("unsupported://creds:secret@bucket/path")

        self.assertEqual("Invalid storage type 'unsupported'", str(error.exception))

    def test_raises_for_missing_scheme(self) -> None:
        with self.assertRaises(InvalidStorageUri) as error:
            get_storage("//creds:secret@invalid/storage/uri")

        self.assertEqual("Invalid storage type ''", str(error.exception))

    def test_raises_for_missing_scheme_and_netloc(self) -> None:
        with self.assertRaises(InvalidStorageUri) as error:
            get_storage("invalid/storage/uri")

        self.assertEqual("Invalid storage type ''", str(error.exception))


class TestStorage(TestCase):
    def test_get_sanitized_uri_removes_username_and_password(self) -> None:
        storage = Storage(storage_uri="https://username:password@bucket/path/filename")
        sanitized_uri = storage.get_sanitized_uri()

        self.assertEqual("https://bucket/path/filename", sanitized_uri)

    def test_get_sanitized_uri_does_not_preserves_parameters(self) -> None:
        storage = Storage(storage_uri="https://username:password@bucket/path/filename?other=param")
        sanitized_uri = storage.get_sanitized_uri()

        self.assertEqual("https://bucket/path/filename", sanitized_uri)

    def test_get_sanitized_uri_preserves_port_number(self) -> None:
        storage = Storage(storage_uri="ftp://username:password@ftp.foo.com:8080/path/filename")
        sanitized_uri = storage.get_sanitized_uri()

        self.assertEqual("ftp://ftp.foo.com:8080/path/filename", sanitized_uri)
