import contextlib
from io import BytesIO
import json
import tempfile
from unittest import mock
from urllib.parse import urlencode

from storage import get_storage
from storage.swift_storage import SwiftStorageError
from tests.service_test_case import ServiceTestCase

_LARGE_CHUNK = 32 * 1024 * 1024


class FileSpy(object):

    def __init__(self):
        self.chunks = []
        self.index = 0
        self.name = ""

    def write(self, chunk):
        self.chunks.append(chunk)
        self.index += len(chunk)

    def seek(self, index):
        self.index = index

    def assert_written(self, assertion):
        assert b"".join(self.chunks) == assertion

    def assert_number_of_chunks(self, n):
        assert n == len(self.chunks)


class TestSwiftStorageProvider(ServiceTestCase):
    def setUp(self):
        super().setUp()

        self.remaining_auth_failures = []
        self.remaining_file_failures = []

        self.file_fetches = {}
        self.file_contents = {}
        self.file_uploads = {}

        self.keystone_credentials = {
            "username": "USER",
            "password": "KEY",
            "tenant_id": "1234"
        }

        self.identity_service = self.add_service()
        self.identity_service.add_handler("GET", "/v2.0", self.identity_handler)
        self.identity_service.add_handler("POST", "/v2.0/tokens", self.authentication_handler)

        self.swift_service = self.add_service()

        self.mock_sleep_patch = mock.patch("time.sleep")
        self.mock_sleep = self.mock_sleep_patch.start()
        self.mock_sleep.side_effect = lambda x: None

    def tearDown(self):
        super().tearDown()

        self.mock_sleep_patch.stop()

    def _add_file_error(self, error: str) -> None:
        self.remaining_file_failures.append(error)

    def _add_file(self, filepath, file_content):
        if type(file_content) is not bytes:
            raise Exception("Object file contents must be bytes")

        self.file_contents[filepath] = file_content
        self.swift_service.add_handler(
            "GET", f"/v2.0/1234/CONTAINER{filepath}", self.swift_object_handler)

    @contextlib.contextmanager
    def _expect_file(self, filepath, file_content):
        self.swift_service.add_handler(
            "PUT", f"/v2.0/1234/CONTAINER{filepath}",
            self.swift_object_put_handler)
        yield
        self.assertEqual(file_content, self.file_uploads[filepath])

    def identity_handler(self, environ, start_response):
        start_response("200 OK", [("Content-Type", "application/json")])
        return [json.dumps({
            "version": {
                "media-types": {
                    "values": [
                        {
                            "type": "application/vnd.openstack.identity+json;version=2.0",
                            "base": "application/json"
                        }
                    ]
                },
                "links": [
                    {
                        "rel": "self",
                        "href": self.identity_service.url("/v2.0")
                    }
                ],
                "id": "v2.0",
                "status": "CURRENT"
            }
        }).encode("utf8")]

    def _valid_credentials(self, body_credentials):
        tenant_name = body_credentials["tenantName"]
        username = body_credentials["passwordCredentials"]["username"]
        password = body_credentials["passwordCredentials"]["password"]

        if username == self.keystone_credentials["username"] and \
           password == self.keystone_credentials["password"] and \
           tenant_name == self.keystone_credentials["tenant_id"]:
            return True
        else:
            return False

    def authentication_handler(self, environ, start_response):
        body_size = int(environ.get('CONTENT_LENGTH', 0))
        body = json.loads(environ["wsgi.input"].read(body_size))

        if len(self.remaining_auth_failures) > 0:
            failure = self.remaining_auth_failures.pop(0)

            start_response(failure, [("Content-type", "text/plain")])
            return [b"Internal Server Error"]

        # Forcing a 401 since swift service won't let us provide it
        if self.keystone_credentials == {}:
            start_response("401 Unauthorized", [("Content-type", "text/plain")])
            return [b"Unauthorized keystone credentials."]
        if not self._valid_credentials(body["auth"]):
            start_response("403 Forbidden", [("Content-type", "text/plain")])
            return [b"Invalid keystone credentials."]

        start_response("200 OK", [("Content-Type", "application/json")])
        return [json.dumps({
            "access": {
                "token": {
                    "expires": "2999-12-05T00:00:00",
                    "id": "TOKEN",
                    "tenant": {
                        "id": "1234",
                        "name": "1234"
                    }
                },
                "serviceCatalog": [{
                    "endpoints": [{
                        "adminURL": self.swift_service.url("/v2.0/1234"),
                        "region": "DFW",
                        "internalURL": self.swift_service.url("/v2.0/1234"),
                        "publicURL": self.swift_service.url("/v2.0/1234")
                    }],
                    "type": "object-store",
                    "name": "swift"
                }],
                "user": {
                    "id": "USERID",
                    "roles": [{
                        "tenantId": "1234",
                        "id": "3",
                        "name": "Member"
                    }],
                    "name": "USER"
                }
            }
        }).encode("utf8")]

    def swift_object_handler(self, environ, start_response):
        path = environ["REQUEST_PATH"].split("CONTAINER")[1]
        self.file_fetches.setdefault(path, 0)
        self.file_fetches[path] += 1

        if len(self.remaining_file_failures) > 0:
            failure = self.remaining_file_failures.pop(0)

            start_response(failure, [("Content-type", "text/plain")])
            return [b"Internal Server Error"]

        if path not in self.file_contents:
            start_response("404 NOT FOUND", [("Content-Type", "text/plain")])
            return [f"Object file {path} not in file contents dictionary".encode("utf8")]

        start_response("200 OK", [("Content-Type", "video/mp4")])
        return [self.file_contents[path]]

    def swift_object_put_handler(self, environ, start_response):
        path = environ["REQUEST_PATH"].split("CONTAINER")[1]
        header = b""
        while not header.endswith(b"\r\n"):
            header += environ["wsgi.input"].read(1)
        body_size = int(header.strip())
        self.file_uploads[path] = environ["wsgi.input"].read(body_size)
        start_response("201 OK", [("Content-Type", "text/plain")])
        return [b""]

    def _generate_swift_uri(self, filename):
        base_uri = f"swift://USER:KEY@CONTAINER{filename}"
        uri_params = urlencode({
            "auth_endpoint": self.identity_service.url("/v2.0"),
            "tenant_id": "1234",
            "region": "DFW"
        })

        return f"{base_uri}?{uri_params}"

    @contextlib.contextmanager
    def assert_raises_on_forbidden_keystone_access(self) -> None:
        self.keystone_credentials["username"] = "nobody"
        with self.run_services():
            with self.assertRaises(SwiftStorageError):
                yield

    @contextlib.contextmanager
    def assert_raises_on_unauthorized_keystone_access(self) -> None:
        self.keystone_credentials = {}
        with self.run_services():
            with self.assertRaises(SwiftStorageError):
                yield

    def assert_fetched_file_n_times(self, path: str, count: int) -> None:
        self.assertEqual(self.file_fetches.get(path, 0), count)

    def assert_requires_all_parameters(self, path, fn):
        base_uri = f"swift://USER:KEY@CONTAINER"
        all_params = {
            "tenant_id": "1234",
            "region": "DFW",
            "auth_endpoint": self.identity_service.url("/v2.0")
        }

        for key in all_params:
            params = all_params.copy()
            del params[key]
            uri = f"{base_uri}{path}?{urlencode(params)}"
            storage_object = get_storage(uri)

            with self.run_services():
                with self.assertRaises(SwiftStorageError):
                    fn(storage_object)

        for auth_string in ["USER:@", ":KEY@"]:
            uri = f"swift://{auth_string}CONTAINER{path}?{urlencode(all_params)}"
            storage_object = get_storage(uri)

            with self.run_services():
                with self.assertRaises(SwiftStorageError):
                    fn(storage_object)

    def test_save_to_file_raises_exception_when_missing_required_parameters(self) -> None:
        tmp_file = BytesIO()
        self._add_file("/path/to/file.mp4", b"FOOBAR")
        self.assert_requires_all_parameters("/path/to/file.mp4", lambda x: x.save_to_file(tmp_file))

    def test_save_to_file_writes_file_contents_to_file_object(self) -> None:
        self._add_file("/path/to/file.mp4", "FOOBAR".encode("utf8"))

        swift_uri = self._generate_swift_uri("/path/to/file.mp4")

        storage_object = get_storage(swift_uri)

        tmp_file = BytesIO()

        with self.run_services():
            storage_object.save_to_file(tmp_file)

        tmp_file.seek(0)

        self.assertEqual("FOOBAR".encode("utf8"), tmp_file.read())

    def test_save_to_file_writes_different_file_contents_to_file(self) -> None:
        self._add_file("/path/to/other/file.mp4", "BARFOO".encode("utf8"))

        swift_uri = self._generate_swift_uri("/path/to/other/file.mp4")

        storage_object = get_storage(swift_uri)

        tmp_file = BytesIO()

        with self.run_services():
            storage_object.save_to_file(tmp_file)

        tmp_file.seek(0)

        self.assertEqual("BARFOO".encode("utf8"), tmp_file.read())

    def test_save_to_file_makes_multiple_requests_when_chunking(self):
        file_contents = b"F" * _LARGE_CHUNK * 3
        self._add_file("/path/to/large/file.mp4", file_contents)

        swift_uri = self._generate_swift_uri("/path/to/large/file.mp4")

        storage_object = get_storage(swift_uri)

        tmp_file = FileSpy()

        with self.run_services():
            storage_object.save_to_file(tmp_file)

        tmp_file.assert_written(file_contents)
        tmp_file.assert_number_of_chunks(3)

    def test_save_to_file_raises_on_forbidden_keystone_credentials(self) -> None:
        self._add_file("/path/to/file.mp4", b"Contents")

        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)
        tmp_file = BytesIO()

        with self.assert_raises_on_forbidden_keystone_access():
            storage_object.save_to_file(tmp_file)

        with self.assert_raises_on_unauthorized_keystone_access():
            storage_object.save_to_file(tmp_file)

    def test_save_to_file_raises_internal_server_exception_after_max_retries(self):
        self.remaining_auth_failures = [
            "500 Internal Server Error", "500 Internal Server Error", "500 Internal Server Error",
            "500 Internal Server Error", "500 Internal Server Error"]

        self._add_file("/path/to/file.mp4", b"FOOBAR")

        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)
        tmp_file = BytesIO()

        with self.run_services():
            with self.assertRaises(SwiftStorageError):
                storage_object.save_to_file(tmp_file)

        self.assertEqual(0, len(self.remaining_auth_failures))

    def test_save_to_file_raises_bad_gateway_exception_after_max_retries(self):
        self.remaining_auth_failures = [
            "502 Bad Gateway", "502 Bad Gateway", "502 Bad Gateway", "502 Bad Gateway",
            "502 Bad Gateway"]

        self._add_file("/path/to/file.mp4", b"FOOBAR")

        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)
        tmp_file = BytesIO()

        with self.run_services():
            with self.assertRaises(SwiftStorageError):
                storage_object.save_to_file(tmp_file)

        self.assertEqual(0, len(self.remaining_auth_failures))

    def test_save_to_file_raises_storage_error_on_swift_service_not_found(self):
        self._add_file_error("404 Not Found")
        self._add_file("/path/to/file.mp4", b"FOOBAR")
        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)
        tmp_file = BytesIO()

        with self.run_services():
            with self.assertRaises(SwiftStorageError):
                storage_object.save_to_file(tmp_file)

        self.assert_fetched_file_n_times("/path/to/file.mp4", 1)

    def test_save_to_file_seeks_to_beginning_of_file_on_error(self):
        self._add_file_error("502 Bad Gateway")
        self._add_file("/path/to/file.mp4", b"FOOBAR")

        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)
        tmp_file = BytesIO()
        tmp_file.write(b"EXTRA")

        with self.run_services():
            storage_object.save_to_file(tmp_file)

        self.assert_fetched_file_n_times("/path/to/file.mp4", 2)
        tmp_file.seek(0)
        self.assertEqual(b"FOOBAR", tmp_file.read())

    def test_save_to_filename_raises_exception_when_missing_parameters(self) -> None:
        tmp_file = tempfile.NamedTemporaryFile()
        self._add_file("/path/to/file.mp4", b"FOOBAR")
        self.assert_requires_all_parameters(
            "/path/to/file.mp4", lambda x: x.save_to_filename(tmp_file.name))

    def test_save_to_filename_writes_file_contents_to_file_object(self) -> None:
        tmp_file = tempfile.NamedTemporaryFile()

        self._add_file("/path/to/filename.mp4", "FOOBAR".encode("utf8"))

        swift_uri = self._generate_swift_uri("/path/to/filename.mp4")

        storage_object = get_storage(swift_uri)

        with self.run_services():
            storage_object.save_to_filename(tmp_file.name)

        tmp_file.seek(0)

        self.assertEqual("FOOBAR".encode("utf8"), tmp_file.read())

    def test_save_to_filename_writes_different_file_contents_to_file(self) -> None:
        tmp_file = tempfile.NamedTemporaryFile()

        self._add_file("/path/to/filename.mp4", "BARFOO".encode("utf8"))

        swift_uri = self._generate_swift_uri("/path/to/filename.mp4")

        storage_object = get_storage(swift_uri)

        with self.run_services():
            storage_object.save_to_filename(tmp_file.name)

        tmp_file.seek(0)

        self.assertEqual("BARFOO".encode("utf8"), tmp_file.read())

    def test_save_to_filename_makes_multiple_requests_when_chunking(self):
        file_contents = b"F" * _LARGE_CHUNK * 3

        self._add_file("/path/to/filename.mp4", file_contents)

        swift_uri = self._generate_swift_uri("/path/to/filename.mp4")

        storage_object = get_storage(swift_uri)

        with self.run_services():
            with mock.patch("builtins.open", mock.mock_open()) as mock_file:
                storage_object.save_to_filename("foobar.mp4")

                mock_file.return_value.write.assert_has_calls([
                    mock.call(b"F" * _LARGE_CHUNK),
                    mock.call(b"F" * _LARGE_CHUNK),
                    mock.call(b"F" * _LARGE_CHUNK)
                ])

    def test_save_to_filename_raises_on_forbidden_keystone_credentials(self) -> None:
        tmp_file = tempfile.NamedTemporaryFile()

        self._add_file("/path/to/filename.mp4", "FOOBAR".encode("utf8"))

        swift_uri = self._generate_swift_uri("/path/to/filename.mp4")

        storage_object = get_storage(swift_uri)

        with self.assert_raises_on_forbidden_keystone_access():
            storage_object.save_to_filename(tmp_file.name)

        with self.assert_raises_on_unauthorized_keystone_access():
            storage_object.save_to_filename(tmp_file.name)

    def test_save_to_filename_raises_internal_server_exception_after_max_retries(self):
        self.remaining_auth_failures = [
            "500 Internal Server Error", "500 Internal Server Error", "500 Internal Server Error",
            "500 Internal Server Error", "500 Internal Server Error"]

        tmp_file = tempfile.NamedTemporaryFile()

        self._add_file("/path/to/filaname.mp4", "FOOBAR".encode("utf8"))

        swift_uri = self._generate_swift_uri("/path/to/filename.mp4")

        storage_object = get_storage(swift_uri)

        with self.run_services():
            with self.assertRaises(SwiftStorageError):
                storage_object.save_to_filename(tmp_file.name)

        self.assertEqual(0, len(self.remaining_auth_failures))

    def test_save_to_filename_raises_bad_gateway_exception_after_max_retries(self):
        self.remaining_auth_failures = [
            "502 Bad Gateway", "502 Bad Gateway", "502 Bad Gateway", "502 Bad Gateway",
            "502 Bad Gateway"]

        tmp_file = tempfile.NamedTemporaryFile()

        self._add_file("/path/to/filename.mp4", "FOOBAR".encode("utf8"))

        swift_uri = self._generate_swift_uri("/path/to/filename.mp4")

        storage_object = get_storage(swift_uri)

        with self.run_services():
            with self.assertRaises(SwiftStorageError):
                storage_object.save_to_filename(tmp_file.name)

        self.assertEqual(0, len(self.remaining_auth_failures))

    def test_save_to_filename_raises_storage_error_on_swift_service_not_found(self):
        self._add_file_error("404 Not Found")

        tmp_file = tempfile.NamedTemporaryFile()

        self._add_file("/path/to/filename.mp4", "FOOBAR".encode("utf8"))

        swift_uri = self._generate_swift_uri("/path/to/filename.mp4")

        storage_object = get_storage(swift_uri)

        with self.run_services():
            with self.assertRaises(SwiftStorageError):
                storage_object.save_to_filename(tmp_file.name)

        self.assert_fetched_file_n_times("/path/to/filename.mp4", 1)

    def test_save_to_filename_seeks_to_beginning_of_file_on_error(self):
        self._add_file_error("502 Bad Gateway")

        tmp_file = tempfile.NamedTemporaryFile()
        tmp_file.write(b"EXTRA")
        tmp_file.flush()

        self._add_file("/path/to/filename.mp4", "FOOBAR".encode("utf8"))

        swift_uri = self._generate_swift_uri("/path/to/filename.mp4")

        storage_object = get_storage(swift_uri)

        with self.run_services():
            storage_object.save_to_filename(tmp_file.name)

        self.assert_fetched_file_n_times("/path/to/filename.mp4", 2)
        tmp_file.seek(0)
        self.assertEqual(b"FOOBAR", tmp_file.read())

    def test_load_from_file_raises_when_missing_required_parameters(self):
        tmp_file = BytesIO(b"FOOBAR")
        self.assert_requires_all_parameters(
            "/path/to/file.mp4", lambda x: x.load_from_file(tmp_file))

    def test_load_from_file_puts_file_contents_at_object_endpoint(self):
        tmp_file = BytesIO(b"FOOBAR")
        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self._expect_file("/path/to/file.mp4", b"FOOBAR"):
            with self.run_services():
                storage_object.load_from_file(tmp_file)
