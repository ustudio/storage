import mock
import threading

from unittest import TestCase

import storage as storagelib
from storage.storage import Storage


class TestTimeout(TestCase):
    @mock.patch("threading.Thread", wraps=threading.Thread)
    def test_calls_function_in_thread(self, mock_thread_class):
        def worker():
            self.assertTrue(threading.current_thread().daemon)
            return "success"

        self.assertEqual("success", storagelib.storage.timeout(5, worker))

        mock_thread_class.assert_called_once_with(target=mock.ANY)

    def test_reraises_exception_raised_by_worker(self):
        def worker():
            raise Exception("some error")

        with self.assertRaisesRegexp(Exception, "^some error$"):
            storagelib.storage.timeout(5, worker)

    def test_raises_timeout_error_when_worker_does_not_complete_within_timeout(self):
        event = threading.Event()

        def worker():
            event.wait()

        try:
            with self.assertRaises(storagelib.storage.TimeoutError):
                storagelib.storage.timeout(0, worker)
        finally:
            event.set()


class TestRegisterStorageProtocol(TestCase):

    def setUp(self):
        self.scheme = "myscheme"

    def test_register_storage_protocol_updates_storage_types(self):

        @storagelib.register_storage_protocol(scheme=self.scheme)
        class MyStorageClass(storagelib.storage.Storage):
            pass

        self.assertIn(self.scheme, storagelib.storage._STORAGE_TYPES)

        uri = "{0}://some/uri/path".format(self.scheme)
        store_obj = storagelib.get_storage(uri)
        self.assertIsInstance(store_obj, MyStorageClass)


class TestStorage(TestCase):
    def test_get_sanitized_uri_removes_username_and_password(self):
        storage = Storage(storage_uri="https://username:password@bucket/path/filename")
        sanitized_uri = storage.get_sanitized_uri()

        self.assertEqual("https://bucket/path/filename", sanitized_uri)

    def test_get_sanitized_uri_does_not_preserves_parameters(self):
        storage = Storage(storage_uri="https://username:password@bucket/path/filename?other=param")
        sanitized_uri = storage.get_sanitized_uri()

        self.assertEqual("https://bucket/path/filename", sanitized_uri)

    def test_get_sanitized_uri_preserves_port_number(self):
        storage = Storage(storage_uri="ftp://username:password@ftp.foo.com:8080/path/filename")
        sanitized_uri = storage.get_sanitized_uri()

        self.assertEqual("ftp://ftp.foo.com:8080/path/filename", sanitized_uri)
