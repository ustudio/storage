from storage.storage import get_storage, InvalidStorageUri, register_storage_protocol, Storage
from storage.storage import timeout, TimeoutError, _STORAGE_TYPES
import threading
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
            def validate_uri(self) -> None:
                raise InvalidStorageUri("Nope I don't like it.")

        with self.assertRaises(InvalidStorageUri):
            get_storage(f"{self.scheme}://some/uri/path")
