import contextlib
from io import BytesIO
from hashlib import sha1
import hmac
import json
import os
import tempfile
import time
from unittest import mock
from urllib.parse import urlencode, urlparse, parse_qsl

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

        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_files = []

        self.remaining_auth_failures = []
        self.remaining_container_failures = []
        self.remaining_file_failures = []
        self.remaining_object_put_failures = []
        self.remaining_file_delete_failures = []

        self.container_contents = {}
        self.directory_contents = {}
        self.file_contents = {}

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
        # this is fragile, but they import and use the function directly, so we mock in-module
        self.mock_swiftclient_sleep_patch = mock.patch("swiftclient.client.sleep")
        self.mock_swiftclient_sleep = self.mock_swiftclient_sleep_patch.start()
        self.mock_swiftclient_sleep.side_effect = lambda x: None

    def tearDown(self):
        super().tearDown()

        for fp in self.tmp_files:
            fp.close()

        self.tmp_dir.cleanup()
        self.mock_sleep_patch.stop()

    def _strip_slashes(self, path):
        while path.endswith(os.path.sep):
            path = path[:-1]
        while path.startswith(os.path.sep):
            path = path[1:]
        return path

    def _add_file_to_directory(self, filepath, file_content) -> None:
        if type(file_content) is not bytes:
            raise Exception("Object file contents must be bytes")

        self.container_contents[filepath] = file_content
        self.swift_service.add_handler("GET", "/v2.0/1234/CONTAINER", self.swift_container_handler)
        self._add_file(filepath, file_content)

    def _add_tmp_file_to_dir(self, directory, file_content):
        if type(file_content) is not bytes:
            raise Exception("Object file contents must be bytes")

        os.makedirs(directory, exist_ok=True)

        tmp_file = tempfile.NamedTemporaryFile(dir=directory)
        tmp_file.write(file_content)
        tmp_file.seek(0)

        self.tmp_files.append(tmp_file)
        return tmp_file

    def _add_file_error(self, error: str) -> None:
        self.remaining_file_failures.append(error)

    def _add_file(self, filepath, file_content) -> None:
        if type(file_content) is not bytes:
            raise Exception("Object file contents must be bytes")

        self.file_contents[filepath] = file_content
        self.swift_service.add_handler(
            "GET", f"/v2.0/1234/CONTAINER{filepath}", self.swift_object_handler)

    @contextlib.contextmanager
    def _expect_file(self, filepath, file_content) -> None:
        self.swift_service.add_handler(
            "PUT", f"/v2.0/1234/CONTAINER{filepath}", self.swift_object_put_handler)
        yield
        self.assertEqual(file_content, self.container_contents[filepath])

    @contextlib.contextmanager
    def _expect_delete(self, filepath) -> None:
        self.container_contents[filepath] = b"UNDELETED!"
        self.swift_service.add_handler(
            "DELETE", f"/v2.0/1234/CONTAINER{filepath}", self.swift_object_delete_handler)

        yield

        self.assertNotIn(
            filepath, self.container_contents, f"File {filepath} was not deleted as expected.")

    @contextlib.contextmanager
    def _expect_directory(self, filepath) -> None:
        for root, _, files in os.walk(self.tmp_dir.name):
            dirpath = self._strip_slashes(root.split(self.tmp_dir.name)[1])
            for basepath in files:
                relative_path = os.path.join(dirpath, basepath)
                remote_path = "/".join([filepath, relative_path])

                self.swift_service.add_handler(
                    "PUT", f"/v2.0/1234/CONTAINER{remote_path}", self.swift_object_put_handler)
        yield

        self.assert_container_contents_equal(filepath)

    @contextlib.contextmanager
    def _expect_delete_directory(self, filepath) -> None:
        expected_delete_paths = []
        for name in self.container_contents:
            delete_path = f"/v2.0/1234/CONTAINER/{self._strip_slashes(name)}"
            expected_delete_paths.append(delete_path)
            self.swift_service.add_handler("DELETE", delete_path, self.swift_object_delete_handler)

        yield

        for delete_path in expected_delete_paths:
            self.swift_service.assert_requested("DELETE", delete_path)

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

    def swift_container_handler(self, environ, start_response):
        if len(self.remaining_container_failures) > 0:
            failure = self.remaining_container_failures.pop(0)

            start_response(failure, [("Content-type", "text/plain")])
            return [b"Internal server error"]

        parsed_args = dict(parse_qsl(environ["QUERY_STRING"]))

        if "json" == parsed_args.get("format"):
            start_response("200 OK", [("Content-Type", "application/json")])
            return [json.dumps([
                {"name": v} for v in self.container_contents.keys()
            ]).encode("utf8")]

        start_response("200 OK", [("Content-Type", "text/plain")])
        return ["\n".join(self.container_contents).encode("utf8")]

    def swift_object_handler(self, environ, start_response):
        path = environ["REQUEST_PATH"].split("CONTAINER")[1]

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

        if len(self.remaining_object_put_failures) > 0:
            failure = self.remaining_object_put_failures.pop(0)
            start_response(failure, [("Content-type", "text/plain")])
            return [b"Internal server error."]

        header = b""
        while not header.endswith(b"\r\n"):
            header += environ["wsgi.input"].read(1)

        body_size = int(header.strip())
        self.container_contents[path] = environ["wsgi.input"].read(body_size)

        start_response("201 OK", [("Content-Type", "text/plain")])
        return [b""]

    def swift_object_delete_handler(self, environ, start_response):
        path = environ["REQUEST_PATH"].split("CONTAINER")[1]

        if len(self.remaining_file_delete_failures) > 0:
            failure = self.remaining_file_delete_failures.pop(0)
            start_response(failure, [("Content-type", "text/plain")])
            return [b"Internal server error."]

        del self.container_contents[path]
        start_response("204 OK", [("Content-type", "text/plain")])
        return [b""]

    def _generate_swift_uri(self, filename, download_url_key=None) -> str:
        base_uri = f"swift://USER:KEY@CONTAINER{filename}"
        uri_params = {
            "auth_endpoint": self.identity_service.url("/v2.0"),
            "tenant_id": "1234",
            "region": "DFW"
        }

        if download_url_key is not None:
            uri_params["download_url_key"] = download_url_key

        return f"{base_uri}?{urlencode(uri_params)}"

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

    def test_save_to_file_makes_multiple_requests_when_chunking(self) -> None:
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

    def test_save_to_file_raises_storage_error_on_swift_service_not_found(self) -> None:
        self._add_file_error("404 Not Found")
        self._add_file("/path/to/file.mp4", b"FOOBAR")

        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = BytesIO()

        with self.run_services():
            with self.assertRaises(SwiftStorageError):
                storage_object.save_to_file(tmp_file)

        self.swift_service.assert_requested_n_times(
            "GET", "/v2.0/1234/CONTAINER/path/to/file.mp4", 1)

    def test_save_to_file_seeks_to_beginning_of_file_on_error(self) -> None:
        self._add_file_error("502 Bad Gateway")
        self._add_file("/path/to/file.mp4", b"FOOBAR")

        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = BytesIO()
        tmp_file.write(b"EXTRA")

        with self.run_services():
            storage_object.save_to_file(tmp_file)

        self.swift_service.assert_requested_n_times(
            "GET", "/v2.0/1234/CONTAINER/path/to/file.mp4", 2)

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

    def test_save_to_filename_makes_multiple_requests_when_chunking(self) -> None:
        file_contents = b"F" * _LARGE_CHUNK * 3

        self._add_file("/path/to/filename.mp4", file_contents)

        swift_uri = self._generate_swift_uri("/path/to/filename.mp4")

        storage_object = get_storage(swift_uri)

        with self.run_services():
            with mock.patch("builtins.open", mock.mock_open()) as mock_file:
                storage_object.save_to_filename("/foobar.mp4")

                mock_file.return_value.write.assert_has_calls([
                    mock.call(b"F" * _LARGE_CHUNK),
                    mock.call(b"F" * _LARGE_CHUNK),
                    mock.call(b"F" * _LARGE_CHUNK)
                ])

    def test_save_to_filename_creates_dir_and_writes_file_contents_to_file_object(self) -> None:
        tmp_dir = tempfile.TemporaryDirectory()
        tmp_file = os.path.join(tmp_dir.name, "folder/folder2/file.mp4")

        self._add_file("/path/to/filename.mp4", "FOOBAR".encode("utf8"))

        swift_uri = self._generate_swift_uri("/path/to/filename.mp4")

        storage_object = get_storage(swift_uri)

        with self.run_services():
            storage_object.save_to_filename(tmp_file)

        self.assertTrue(os.path.exists(tmp_file))

        file_contents = ""
        with open(tmp_file, "rb") as fp:
            file_contents = fp.read()

        self.assertEqual("FOOBAR".encode("utf8"), file_contents)

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

        self.swift_service.assert_requested_n_times(
            "GET", "/v2.0/1234/CONTAINER/path/to/filename.mp4", 1)

    def test_save_to_filename_seeks_to_beginning_of_file_on_error(self) -> None:
        self._add_file_error("502 Bad Gateway")

        tmp_file = tempfile.NamedTemporaryFile()
        tmp_file.write(b"EXTRA")
        tmp_file.flush()

        self._add_file("/path/to/filename.mp4", "FOOBAR".encode("utf8"))

        swift_uri = self._generate_swift_uri("/path/to/filename.mp4")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            storage_object.save_to_filename(tmp_file.name)

        self.swift_service.assert_requested_n_times(
            "GET", "/v2.0/1234/CONTAINER/path/to/filename.mp4", 2)

        tmp_file.seek(0)
        self.assertEqual(b"FOOBAR", tmp_file.read())

    def test_load_from_file_raises_when_missing_required_parameters(self) -> None:
        tmp_file = BytesIO(b"FOOBAR")

        self.assert_requires_all_parameters(
            "/path/to/file.mp4", lambda x: x.load_from_file(tmp_file))

    def test_load_from_file_puts_file_contents_at_object_endpoint(self) -> None:
        tmp_file = BytesIO(b"FOOBAR")

        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self._expect_file("/path/to/file.mp4", b"FOOBAR"):
            with self.run_services():
                storage_object.load_from_file(tmp_file)

    def test_load_from_file_retries_on_error(self) -> None:
        self.remaining_object_put_failures.append("500 Internal server error")

        tmp_file = BytesIO(b"FOOBAR")

        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self._expect_file("/path/to/file.mp4", b"FOOBAR"):
            with self.run_services():
                storage_object.load_from_file(tmp_file)

        self.swift_service.assert_requested_n_times(
            "PUT", "/v2.0/1234/CONTAINER/path/to/file.mp4", 2)

    def test_load_from_file_does_not_retry_with_invalid_keystone_creds(self) -> None:
        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = BytesIO(b"FOOBAR")

        with self.assert_raises_on_forbidden_keystone_access():
            storage_object.load_from_file(tmp_file)

        with self.assert_raises_on_unauthorized_keystone_access():
            storage_object.load_from_file(tmp_file)

        self.swift_service.assert_not_requested("PUT", "/v2.0/1234/CONTAINER/path/to/file.mp4")

    def test_load_from_file_retries_on_authentication_server_errors(self) -> None:
        self.remaining_auth_failures = ["500 Error", "500 Error"]

        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = BytesIO(b"FOOBAR")

        with self._expect_file("/path/to/file.mp4", b"FOOBAR"):
            with self.run_services():
                storage_object.load_from_file(tmp_file)

        self.identity_service.assert_requested_n_times("POST", "/v2.0/tokens", 3)

    def test_load_from_filename_raises_when_missing_required_parameters(self) -> None:
        tmp_file = tempfile.NamedTemporaryFile()
        tmp_file.write(b"FOOBAR")
        tmp_file.flush()

        self.assert_requires_all_parameters(
            "/path/to/file.mp4", lambda x: x.load_from_filename(tmp_file.name))

    def test_load_from_filename_puts_file_contents_at_object_endpoint(self) -> None:
        tmp_file = tempfile.NamedTemporaryFile()
        tmp_file.write(b"FOOBAR")
        tmp_file.flush()

        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self._expect_file("/path/to/file.mp4", b"FOOBAR"):
            with self.run_services():
                storage_object.load_from_filename(tmp_file.name)

    def test_load_from_filename_retries_on_error(self) -> None:
        self.remaining_object_put_failures.append("500 Internal server error")

        tmp_file = tempfile.NamedTemporaryFile()
        tmp_file.write(b"FOOBAR")
        tmp_file.flush()

        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self._expect_file("/path/to/file.mp4", b"FOOBAR"):
            with self.run_services():
                storage_object.load_from_filename(tmp_file.name)

        self.swift_service.assert_requested_n_times(
            "PUT", "/v2.0/1234/CONTAINER/path/to/file.mp4", 2)

    def test_load_from_filename_does_not_retry_with_invalid_keystone_creds(self) -> None:
        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = tempfile.NamedTemporaryFile()
        tmp_file.write(b"FOOBAR")
        tmp_file.flush()

        with self.assert_raises_on_forbidden_keystone_access():
            storage_object.load_from_filename(tmp_file.name)

        with self.assert_raises_on_unauthorized_keystone_access():
            storage_object.load_from_filename(tmp_file.name)

        self.swift_service.assert_not_requested("PUT", "/v2.0/1234/CONTAINER/path/to/file.mp4")

    def test_load_from_filename_retries_on_authentication_server_errors(self) -> None:
        self.remaining_auth_failures = ["500 Error", "500 Error"]

        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = tempfile.NamedTemporaryFile()
        tmp_file.write(b"FOOBAR")
        tmp_file.flush()

        with self._expect_file("/path/to/file.mp4", b"FOOBAR"):
            with self.run_services():
                storage_object.load_from_filename(tmp_file.name)

        self.identity_service.assert_requested_n_times("POST", "/v2.0/tokens", 3)

    def test_delete_raises_on_missing_parameters(self) -> None:
        self.container_contents["/path/to/file.mp4"] = b"UNDELETED"

        self.assert_requires_all_parameters("/path/to/file.mp4", lambda x: x.delete())
        self.assertEqual(b"UNDELETED", self.container_contents["/path/to/file.mp4"])

    def test_delete_makes_delete_request_against_swift_service(self) -> None:
        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self._expect_delete("/path/to/file.mp4"):
            with self.run_services():
                storage_object.delete()

    def test_delete_retries_on_authentication_server_errors(self) -> None:
        self.remaining_auth_failures = ["500 Error", "500 Error"]

        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self._expect_delete("/path/to/file.mp4"):
            with self.run_services():
                storage_object.delete()

        self.identity_service.assert_requested_n_times("POST", "/v2.0/tokens", 3)

    def test_delete_retries_on_swift_server_errors(self) -> None:
        self.remaining_file_delete_failures = ["500 Error", "500 Error"]

        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self._expect_delete("/path/to/file.mp4"):
            with self.run_services():
                storage_object.delete()

        self.swift_service.assert_requested_n_times(
            "DELETE", "/v2.0/1234/CONTAINER/path/to/file.mp4", 3)

    @mock.patch("time.time")
    def test_get_download_url_raises_with_missing_download_url_key(self, mock_time) -> None:
        mock_time.return_value = 9000

        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            with self.assertRaises(SwiftStorageError):
                storage_object.get_download_url()

    @mock.patch("time.time")
    def test_get_download_url_returns_signed_url(self, mock_time) -> None:
        mock_time.return_value = 9000

        swift_uri = self._generate_swift_uri("/path/to/file.mp4", "KEY")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            url = storage_object.get_download_url()

        parsed = urlparse(url)
        expected = urlparse(self.swift_service.url("/v1/AUTH_account/CONTAINER/path/to/file.mp4"))

        self.assertEqual(parsed.path, expected.path)
        self.assertEqual(parsed.netloc, expected.netloc)

        query = dict(parse_qsl(parsed.query))

        self.assertEqual("9060", query["temp_url_expires"])
        self.assertTrue("temp_url_sig" in query)

    @mock.patch("time.time")
    def test_get_download_url_accepts_variable_seconds(self, mock_time) -> None:
        mock_time.return_value = 9000

        swift_uri = self._generate_swift_uri("/path/to/file.mp4", "KEY")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            url = storage_object.get_download_url(seconds=120)

        parsed = urlparse(url)
        query = dict(parse_qsl(parsed.query))

        self.assertEqual("9120", query["temp_url_expires"])

    def generate_signature(self, path, key, expires=60):
        timestamp = time.time()
        raw_string = f"GET\n{timestamp + expires}\n/v1/AUTH_account/CONTAINER{path}"
        return hmac.new(key, raw_string.encode("utf8"), sha1).hexdigest()

    @mock.patch("time.time")
    def test_get_download_url_uses_download_url_key_by_default(self, mock_time) -> None:
        mock_time.return_value = 9000

        swift_uri = self._generate_swift_uri("/path/to/file.mp4", "KEY")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            url = storage_object.get_download_url()

        parsed = urlparse(url)
        signature = dict(parse_qsl(parsed.query))["temp_url_sig"]
        expected_signature = self.generate_signature("/path/to/file.mp4", b"KEY")

        self.assertEqual(expected_signature, signature)

    @mock.patch("time.time")
    def test_get_download_url_uses_alternate_download_url_key(self, mock_time) -> None:
        mock_time.return_value = 9000

        swift_uri = self._generate_swift_uri("/path/to/file.mp4", "FOOBAR")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            url = storage_object.get_download_url()

        parsed = urlparse(url)
        signature = dict(parse_qsl(parsed.query))["temp_url_sig"]

        expected_signature = self.generate_signature("/path/to/file.mp4", b"FOOBAR")
        self.assertEqual(expected_signature, signature)

    @mock.patch("time.time")
    def test_get_download_url_uses_provided_key(self, mock_time) -> None:
        mock_time.return_value = 9000

        swift_uri = self._generate_swift_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            url = storage_object.get_download_url(key="BLARGH")

        parsed = urlparse(url)
        signature = dict(parse_qsl(parsed.query))["temp_url_sig"]

        expected_signature = self.generate_signature("/path/to/file.mp4", b"BLARGH")
        self.assertEqual(expected_signature, signature)

    @mock.patch("time.time")
    def test_get_download_url_overrides_download_url_key_with_provided_key(self, mock_time) -> None:
        mock_time.return_value = 9000

        swift_uri = self._generate_swift_uri("/path/to/file.mp4", "FOOBAR")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            url = storage_object.get_download_url(key="BARFOO")

        parsed = urlparse(url)
        signature = dict(parse_qsl(parsed.query))["temp_url_sig"]

        expected_signature = self.generate_signature("/path/to/file.mp4", b"BARFOO")
        self.assertEqual(expected_signature, signature)

    def test_save_to_directory_raises_exception_when_missing_required_parameters(self) -> None:
        self._add_file("/path/to/file.mp4", b"FOOBAR")
        self.assert_requires_all_parameters(
            "/path/to/files", lambda x: x.save_to_directory(self.tmp_dir.name))

    def test_save_to_directory_raises_on_forbidden_keystone_credentials(self) -> None:
        self._add_file("/path/to/files/file.mp4", b"Contents")
        self._add_file("/path/to/files/other_file.mp4", b"Contents")

        swift_uri = self._generate_swift_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.assert_raises_on_forbidden_keystone_access():
            storage_object.save_to_directory(self.tmp_dir.name)

        with self.assert_raises_on_unauthorized_keystone_access():
            storage_object.save_to_directory(self.tmp_dir.name)

    def assert_container_contents_equal(self, object_path):
        written_files = {}
        expected_files = {
            self._strip_slashes(f.split(object_path)[1]): v
            for f, v in self.container_contents.items()
        }

        for root, dirs, files in os.walk(self.tmp_dir.name):
            dirpath = self._strip_slashes(root.split(self.tmp_dir.name)[1])
            for basepath in files:
                fullpath = os.path.join(root, basepath)
                relpath = os.path.join(dirpath, basepath)
                with open(fullpath, "rb") as fp:
                    written_files[relpath] = fp.read()

        self.assertCountEqual(written_files, expected_files)

    def test_save_to_directory_downloads_files_matching_prefix_to_directory_location(self) -> None:
        self._add_file_to_directory("/path/to/files/file.mp4", b"Contents")
        self._add_file_to_directory("/path/to/files/other_file.mp4", b"Other Contents")

        swift_uri = self._generate_swift_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            storage_object.save_to_directory(self.tmp_dir.name)

        self.assert_container_contents_equal("/path/to/files")

    def test_save_to_directory_includes_subdirectories_in_local_path(self) -> None:
        self._add_file_to_directory("/path/to/files/file.mp4", b"Contents")
        self._add_file_to_directory("/path/to/files/other_file.mp4", b"Other Contents")
        self._add_file_to_directory("/path/to/files/folder/file2.mp4", b"Video Content")
        self._add_file_to_directory("/path/to/files/folder2/folder3/files3.mp4", b"Video Contents")

        swift_uri = self._generate_swift_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            storage_object.save_to_directory(self.tmp_dir.name)

        self.assert_container_contents_equal("/path/to/files")

    def test_save_to_directory_retries_on_error(self) -> None:
        self.remaining_container_failures.append("500 Internal server error")
        self._add_file_to_directory("/path/to/files/file.mp4", b"Contents")

        swift_uri = self._generate_swift_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            storage_object.save_to_directory(self.tmp_dir.name)

        self.swift_service.assert_requested_n_times("GET", "/v2.0/1234/CONTAINER", 2)

    def test_save_to_directory_only_retries_put_object_when_store_object_fails(self) -> None:
        self.remaining_file_failures.append("500 Internal server error")
        self.remaining_file_failures.append("500 Internal server error")
        self._add_file_to_directory("/path/to/files/file.mp4", b"Contents")
        swift_uri = self._generate_swift_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            storage_object.save_to_directory(self.tmp_dir.name)

        self.identity_service.assert_requested_n_times("POST", "/v2.0/tokens", 1)
        self.swift_service.assert_requested("GET", "/v2.0/1234/CONTAINER")
        self.swift_service.assert_requested_n_times(
            "GET", "/v2.0/1234/CONTAINER/path/to/files/file.mp4", 3)

    def test_save_to_directory_raises_internal_server_exception_after_max_retries(self) -> None:
        self.remaining_auth_failures = [
            "500 Error", "500 Error", "500 Error", "500 Error", "500 Error"]

        self._add_file_to_directory("/path/to/files/file.mp4", b"contents")

        swift_uri = self._generate_swift_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            with self.assertRaises(SwiftStorageError):
                storage_object.save_to_directory(self.tmp_dir.name)

        self.assertEqual(0, len(self.remaining_auth_failures))

    def test_load_from_directory_raises_when_missing_required_parameters(self) -> None:
        self.assert_requires_all_parameters(
            "/path/to/files", lambda x: x.load_from_directory(self.tmp_dir.name))

    def test_load_from_directory_puts_file_contents_at_object_endpoint(self) -> None:
        self._add_tmp_file_to_dir(self.tmp_dir.name, b"FOOBAR")
        self._add_tmp_file_to_dir(self.tmp_dir.name, b"FIZZBUZZ")

        swift_uri = self._generate_swift_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self._expect_directory("/path/to/files"):
            with self.run_services():
                storage_object.load_from_directory(self.tmp_dir.name)

    def test_load_from_directory_does_not_retry_with_invalid_keystone_creds(self) -> None:
        file1 = self._add_tmp_file_to_dir(self.tmp_dir.name, b"FOOBAR")
        file2 = self._add_tmp_file_to_dir(self.tmp_dir.name, b"FIZZBUZZ")

        swift_uri = self._generate_swift_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.assert_raises_on_forbidden_keystone_access():
            storage_object.load_from_directory(self.tmp_dir.name)

        with self.assert_raises_on_unauthorized_keystone_access():
            storage_object.load_from_directory(self.tmp_dir.name)

        self.swift_service.assert_not_requested(
            "PUT", f"/v2.0/1234/CONTAINER/path/to/files/{os.path.basename(file1.name)}")
        self.swift_service.assert_not_requested(
            "PUT", f"/v2.0/1234/CONTAINER/path/to/files/{os.path.basename(file2.name)}")

    def test_load_from_directory_retries_on_error(self) -> None:
        self.remaining_object_put_failures.append("500 Internal server error")

        file1 = self._add_tmp_file_to_dir(self.tmp_dir.name, b"FOOBAR")
        file2 = self._add_tmp_file_to_dir(self.tmp_dir.name, b"FIZZBUZZ")

        swift_uri = self._generate_swift_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self._expect_directory("/path/to/files"):
            with self.run_services():
                storage_object.load_from_directory(self.tmp_dir.name)

        file1_requests = self.swift_service.assert_requested(
            "PUT", f"/v2.0/1234/CONTAINER/path/to/files/{os.path.basename(file1.name)}")
        file2_requests = self.swift_service.assert_requested(
            "PUT", f"/v2.0/1234/CONTAINER/path/to/files/{os.path.basename(file2.name)}")
        self.assertCountEqual([2, 1], [file1_requests.count, file2_requests.count])

    def test_load_from_directory_includes_subdirectories_in_object_endpoint(self) -> None:
        dir_name = os.path.join(self.tmp_dir.name, "files2")
        self._add_tmp_file_to_dir(dir_name, b"NESTED")

        self._add_tmp_file_to_dir(self.tmp_dir.name, b"FOOBAR")
        self._add_tmp_file_to_dir(self.tmp_dir.name, b"FIZZBUZZ")

        swift_uri = self._generate_swift_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self._expect_directory("/path/to/files"):
            with self.run_services():
                storage_object.load_from_directory(self.tmp_dir.name)

        self.assert_container_contents_equal("/path/to/files")

    def test_load_from_directory_raises_internal_server_exception_after_max_retries(self) -> None:
        self.remaining_auth_failures = [
            "500 Error", "500 Error", "500 Error", "500 Error", "500 Error"]

        self._add_tmp_file_to_dir(self.tmp_dir.name, b"FOOBAR")

        swift_uri = self._generate_swift_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            with self.assertRaises(SwiftStorageError):
                storage_object.load_from_directory(self.tmp_dir.name)

        self.assertEqual(0, len(self.remaining_auth_failures))

    def test_delete_directory_raises_on_missing_parameters(self) -> None:
        self.container_contents["/path/to/files/file.mp4"] = b"UNDELETED"

        self.assert_requires_all_parameters("/path/to/files", lambda x: x.delete_directory())
        self.assertEqual(b"UNDELETED", self.container_contents["/path/to/files/file.mp4"])

    def test_delete_directory_makes_delete_request_against_swift_service(self) -> None:
        self.swift_service.add_handler("GET", "/v2.0/1234/CONTAINER", self.swift_container_handler)
        self.container_contents["/path/to/files/file.mp4"] = b"Contents"
        self.container_contents["/path/to/files/folder/file2.mp4"] = b"Video Content"

        swift_uri = self._generate_swift_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self._expect_delete_directory("/path/to/files"):
            with self.run_services():
                storage_object.delete_directory()

    def test_delete_directory_retries_on_authentication_server_errors(self) -> None:
        self.swift_service.add_handler("GET", "/v2.0/1234/CONTAINER", self.swift_container_handler)
        self.remaining_auth_failures = ["500 Error", "500 Error"]

        swift_uri = self._generate_swift_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self._expect_delete_directory("/path/to/files"):
            with self.run_services():
                storage_object.delete_directory()

        self.identity_service.assert_requested_n_times("POST", "/v2.0/tokens", 3)

    def test_delete_directory_retries_on_swift_server_errors(self) -> None:
        self.swift_service.add_handler("GET", "/v2.0/1234/CONTAINER", self.swift_container_handler)
        self.remaining_file_delete_failures = ["500 Error", "500 Error"]
        self.container_contents["/path/to/files/file.mp4"] = b"UNDELETED"
        self.container_contents["/path/to/files/folder/file2.mp4"] = b"UNDELETED"

        swift_uri = self._generate_swift_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self._expect_delete_directory("/path/to/files"):
            with self.run_services():
                storage_object.delete_directory()

        file1_requests = self.swift_service.assert_requested(
            "DELETE", "/v2.0/1234/CONTAINER/path/to/files/file.mp4")
        file2_requests = self.swift_service.assert_requested(
            "DELETE", "/v2.0/1234/CONTAINER/path/to/files/folder/file2.mp4")
        self.assertCountEqual([3, 1], [file1_requests.count, file2_requests.count])
