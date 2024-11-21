import contextlib
from io import BytesIO
from hashlib import sha256
import hmac
import json
import os
import tempfile
import time
from unittest import mock
from urllib.parse import urlencode, urlparse, parse_qsl

from keystoneauth1.exceptions.http import BadGateway, Forbidden, InternalServerError, Unauthorized
from swiftclient.exceptions import ClientException
from typing import Any, Dict, Generator, List, Optional, TYPE_CHECKING

from storage.storage import get_storage, InvalidStorageUri, NotFoundError
from storage.swift_storage import SwiftStorageError
from tests.storage_test_case import StorageTestCase
from tests.swift_service_test_case import strip_slashes, SwiftServiceTestCase
from tests.helpers import FileSpy

if TYPE_CHECKING:
    from tests.service_test_case import Environ
    from wsgiref.types import StartResponse  # type: ignore[import-not-found]


_LARGE_CHUNK = 32 * 1024 * 1024


class TestSwiftStorageProvider(StorageTestCase, SwiftServiceTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.auth_failure = ""

        self.keystone_credentials = {
            "username": "USER",
            "password": "KEY",
            "tenant_id": "1234"
        }

        self.identity_service = self.add_service()
        self.identity_service.add_handler("GET", "/v2.0", self.identity_handler)
        self.identity_service.add_handler("POST", "/v2.0/tokens", self.authentication_handler)

        self.mock_sleep_patch = mock.patch("time.sleep")
        self.mock_sleep = self.mock_sleep_patch.start()
        self.mock_sleep.side_effect = lambda x: None

    def tearDown(self) -> None:
        super().tearDown()
        self.mock_sleep_patch.stop()

    def identity_handler(self, environ: "Environ", start_response: "StartResponse") -> List[bytes]:
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

    def _valid_credentials(self, body_credentials: Dict[str, Any]) -> bool:
        tenant_name = body_credentials["tenantName"]
        username = body_credentials["passwordCredentials"]["username"]
        password = body_credentials["passwordCredentials"]["password"]

        if username == self.keystone_credentials["username"] and \
           password == self.keystone_credentials["password"] and \
           tenant_name == self.keystone_credentials["tenant_id"]:
            return True
        else:
            return False

    def authentication_handler(
            self, environ: "Environ", start_response: "StartResponse") -> List[bytes]:
        body_size = int(environ.get("CONTENT_LENGTH", 0))
        body = json.loads(environ["wsgi.input"].read(body_size))

        if len(self.auth_failure) > 0:
            failure = self.auth_failure

            start_response(failure, [("Content-type", "text/plain")])
            return [b"Internal Server Error"]

        # Forcing a 401 since swift service won't let us provide it
        if self.keystone_credentials == {}:
            start_response("401 Unauthorized", [("Content-type", "text/plain")])
            return [b"Unauthorized keystone credentials."]
        if not self._valid_credentials(body["auth"]):
            start_response("403 Forbidden", [("Content-type", "text/plain")])
            return [b"Invalid keystone credentials."]

        start_response("200 OK", [("Content-type", "application/json")])
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

    def _generate_storage_uri(
            self, filename: str, parameters: Optional[Dict[str, str]] = None) -> str:
        base_uri = f"swift://USER:KEY@CONTAINER{filename}"
        query_args = {
            "auth_endpoint": self.identity_service.url("/v2.0"),
            "tenant_id": "1234",
            "region": "DFW"
        }

        if parameters is not None:
            query_args.update(parameters)

        return f"{base_uri}?{urlencode(query_args)}"

    @contextlib.contextmanager
    def assert_raises_on_forbidden_keystone_access(self) -> Generator[None, None, None]:
        self.keystone_credentials["username"] = "nobody"
        with self.run_services():
            with self.assertRaises(Forbidden):
                yield

    @contextlib.contextmanager
    def assert_raises_on_unauthorized_keystone_access(self) -> Generator[None, None, None]:
        self.keystone_credentials = {}
        with self.run_services():
            with self.assertRaises(Unauthorized):
                yield

    def assert_requires_all_parameters(self, path: str) -> None:
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

            with self.assertRaises(SwiftStorageError):
                get_storage(uri)

        for auth_string in ["USER:@", ":KEY@"]:
            uri = f"swift://{auth_string}CONTAINER{path}?{urlencode(all_params)}"

            with self.assertRaises(InvalidStorageUri):
                get_storage(uri)

    def test_save_to_file_raises_exception_when_missing_required_parameters(self) -> None:
        self.assert_requires_all_parameters("/path/to/file.mp4")

    def test_save_to_file_writes_file_contents_to_file_object(self) -> None:
        self.add_container_object("/v2.0/1234/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = BytesIO()

        with self.run_services():
            storage_object.save_to_file(tmp_file)

        tmp_file.seek(0)

        self.assertEqual(b"FOOBAR", tmp_file.read())

    def test_save_to_file_writes_different_file_contents_to_file(self) -> None:
        self.add_container_object("/v2.0/1234/CONTAINER", "/path/to/other/file.mp4", b"BARFOO")

        swift_uri = self._generate_storage_uri("/path/to/other/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = BytesIO()

        with self.run_services():
            storage_object.save_to_file(tmp_file)

        tmp_file.seek(0)

        self.assertEqual(b"BARFOO", tmp_file.read())

    def test_save_to_file_makes_multiple_requests_when_chunking(self) -> None:
        file_contents = b"F" * _LARGE_CHUNK * 3
        self.add_container_object("/v2.0/1234/CONTAINER", "/path/to/large/file.mp4", file_contents)

        swift_uri = self._generate_storage_uri("/path/to/large/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = FileSpy()

        with self.run_services():
            storage_object.save_to_file(tmp_file)

        tmp_file.assert_written(file_contents)
        tmp_file.assert_number_of_chunks(3)

    def test_save_to_file_raises_on_forbidden_keystone_credentials(self) -> None:
        self.add_container_object("/v2.0/1234/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = BytesIO()

        with self.assert_raises_on_forbidden_keystone_access():
            storage_object.save_to_file(tmp_file)

        with self.assert_raises_on_unauthorized_keystone_access():
            storage_object.save_to_file(tmp_file)

    def test_save_to_file_raises_internal_server_exception(self) -> None:
        self.auth_failure = "500 Internal Server Error"

        self.add_container_object("/v2.0/1234/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = BytesIO()

        with self.run_services():
            with self.assertRaises(InternalServerError):
                storage_object.save_to_file(tmp_file)

    def test_save_to_file_raises_bad_gateway_exception(self) -> None:
        self.auth_failure = "502 Bad Gateway"

        self.add_container_object("/v2.0/1234/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = BytesIO()

        with self.run_services():
            with self.assertRaises(BadGateway):
                storage_object.save_to_file(tmp_file)

    def test_save_to_file_raises_not_found_error_when_file_does_not_exist(self) -> None:
        self.add_file_error("404 Not Found")
        self.add_container_object("/v2.0/1234/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = BytesIO()

        with self.run_services():
            with self.assertRaises(NotFoundError):
                storage_object.save_to_file(tmp_file)

        self.swift_service.assert_requested_n_times(
            "GET", "/v2.0/1234/CONTAINER/path/to/file.mp4", 1)

    def test_save_to_file_raises_original_exception_when_not_404(self) -> None:
        self.add_file_error("502 Bad Gateway")
        self.add_container_object("/v2.0/1234/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = BytesIO()

        with self.run_services():
            with self.assertRaises(ClientException):
                storage_object.save_to_file(tmp_file)

        self.swift_service.assert_requested_n_times(
            "GET", "/v2.0/1234/CONTAINER/path/to/file.mp4", 1)

    def test_save_to_filename_raises_exception_when_missing_parameters(self) -> None:
        self.assert_requires_all_parameters("/path/to/file.mp4")

    def test_save_to_filename_writes_file_contents_to_file_object(self) -> None:
        self.add_container_object("/v2.0/1234/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = tempfile.NamedTemporaryFile()

        with self.run_services():
            storage_object.save_to_filename(tmp_file.name)

        tmp_file.seek(0)

        self.assertEqual(b"FOOBAR", tmp_file.read())

    def test_save_to_filename_writes_different_file_contents_to_file(self) -> None:
        self.add_container_object("/v2.0/1234/CONTAINER", "/path/to/file.mp4", b"BARFOO")

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = tempfile.NamedTemporaryFile()

        with self.run_services():
            storage_object.save_to_filename(tmp_file.name)

        tmp_file.seek(0)

        self.assertEqual(b"BARFOO", tmp_file.read())

    def test_save_to_filename_makes_multiple_requests_when_chunking(self) -> None:
        file_contents = b"F" * _LARGE_CHUNK * 3

        self.add_container_object("/v2.0/1234/CONTAINER", "/path/to/file.mp4", file_contents)

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            with mock.patch("builtins.open", mock.mock_open()) as mock_file:
                storage_object.save_to_filename("/foobar.mp4")

                mock_file.return_value.write.assert_has_calls([
                    mock.call(b"F" * _LARGE_CHUNK),
                    mock.call(b"F" * _LARGE_CHUNK),
                    mock.call(b"F" * _LARGE_CHUNK)
                ])

    def test_save_to_filename_raises_on_forbidden_keystone_credentials(self) -> None:
        self.add_container_object("/v2.0/1234/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = tempfile.NamedTemporaryFile()

        with self.assert_raises_on_forbidden_keystone_access():
            storage_object.save_to_filename(tmp_file.name)

        with self.assert_raises_on_unauthorized_keystone_access():
            storage_object.save_to_filename(tmp_file.name)

    def test_save_to_filename_raises_internal_server_exception(self) -> None:
        self.auth_failure = "500 Internal Server Error"

        self.add_container_object("/v2.0/1234/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = tempfile.NamedTemporaryFile()

        with self.run_services():
            with self.assertRaises(InternalServerError):
                storage_object.save_to_filename(tmp_file.name)

    def test_save_to_filename_raises_bad_gateway_exception(self) -> None:
        self.auth_failure = "502 Bad Gateway"

        self.add_container_object("/v2.0/1234/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = tempfile.NamedTemporaryFile()

        with self.run_services():
            with self.assertRaises(BadGateway):
                storage_object.save_to_filename(tmp_file.name)

    def test_save_to_filename_raises_not_found_error_when_file_does_not_exist(self) -> None:
        self.add_file_error("404 Not Found")
        self.add_container_object("/v2.0/1234/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = tempfile.NamedTemporaryFile()

        with self.run_services():
            with self.assertRaises(NotFoundError):
                storage_object.save_to_filename(tmp_file.name)

        self.swift_service.assert_requested_n_times(
            "GET", "/v2.0/1234/CONTAINER/path/to/file.mp4", 1)

    def test_save_to_filename_raises_original_exception_when_not_404(self) -> None:
        self.add_file_error("502 Bad Gateway")
        self.add_container_object("/v2.0/1234/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = tempfile.NamedTemporaryFile()

        with self.run_services():
            with self.assertRaises(ClientException):
                storage_object.save_to_filename(tmp_file.name)

        self.swift_service.assert_requested_n_times(
            "GET", "/v2.0/1234/CONTAINER/path/to/file.mp4", 1)

    def test_load_from_file_raises_when_missing_required_parameters(self) -> None:
        self.assert_requires_all_parameters("/path/to/file.mp4")

    def test_load_from_file_puts_file_contents_at_object_endpoint(self) -> None:
        tmp_file = BytesIO(b"FOOBAR")

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self.expect_put_object("/v2.0/1234/CONTAINER", "/path/to/file.mp4", b"FOOBAR"):
            with self.run_services():
                storage_object.load_from_file(tmp_file)

    def test_load_from_file_does_not_rety_on_error(self) -> None:
        self.remaining_object_put_failures.append("500 Internal server error")

        tmp_file = BytesIO(b"FOOBAR")

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        put_path = "/v2.0/1234/CONTAINER/path/to/file.mp4"
        self.swift_service.add_handler("PUT", put_path, self.object_put_handler)

        with self.run_services():
            with self.assertRaises(ClientException):
                storage_object.load_from_file(tmp_file)

        self.swift_service.assert_requested_n_times(
            "PUT", "/v2.0/1234/CONTAINER/path/to/file.mp4", 1)

    def test_load_from_file_does_not_retry_with_invalid_keystone_creds(self) -> None:
        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = BytesIO(b"FOOBAR")

        with self.assert_raises_on_forbidden_keystone_access():
            storage_object.load_from_file(tmp_file)

        with self.assert_raises_on_unauthorized_keystone_access():
            storage_object.load_from_file(tmp_file)

        self.swift_service.assert_not_requested("PUT", "/v2.0/1234/CONTAINER/path/to/file.mp4")

    def test_load_from_file_raises_on_authentication_server_errors(self) -> None:
        self.auth_failure = "500 Internal Server Error"

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = BytesIO(b"FOOBAR")

        with self.run_services():
            with self.assertRaises(InternalServerError):
                storage_object.load_from_file(tmp_file)

    def test_load_from_filename_raises_when_missing_required_parameters(self) -> None:
        self.assert_requires_all_parameters("/path/to/file.mp4")

    def test_load_from_filename_puts_file_contents_at_object_endpoint(self) -> None:
        tmp_file = tempfile.NamedTemporaryFile()
        tmp_file.write(b"FOOBAR")
        tmp_file.flush()

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self.expect_put_object("/v2.0/1234/CONTAINER", "/path/to/file.mp4", b"FOOBAR"):
            with self.run_services():
                storage_object.load_from_filename(tmp_file.name)

    def test_load_from_filename_sends_guessed_content_type_from_extension(self) -> None:
        tmp_file = tempfile.NamedTemporaryFile()
        tmp_file.write(b"FOOBAR")
        tmp_file.flush()

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self.expect_put_object("/v2.0/1234/CONTAINER", "/path/to/file.mp4", b"FOOBAR"):
            with self.run_services():
                storage_object.load_from_filename(tmp_file.name)

        request = self.swift_service.assert_requested(
            "PUT", "/v2.0/1234/CONTAINER/path/to/file.mp4")
        request.assert_header_equals("Content-type", "video/mp4")

    def test_load_from_filename_sends_default_content_type_when_none_is_determined(self) -> None:
        tmp_file = tempfile.NamedTemporaryFile()
        tmp_file.write(b"FOOBAR")
        tmp_file.flush()

        swift_uri = self._generate_storage_uri("/path/to/file")
        storage_object = get_storage(swift_uri)

        with self.expect_put_object("/v2.0/1234/CONTAINER", "/path/to/file", b"FOOBAR"):
            with self.run_services():
                storage_object.load_from_filename(tmp_file.name)

        request = self.swift_service.assert_requested(
            "PUT", "/v2.0/1234/CONTAINER/path/to/file")
        request.assert_header_equals("Content-type", "application/octet-stream")

    def test_load_from_filename_does_not_retry_on_error(self) -> None:
        self.remaining_object_put_failures.append("500 Internal server error")

        tmp_file = tempfile.NamedTemporaryFile()
        tmp_file.write(b"FOOBAR")
        tmp_file.flush()

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        put_path = "/v2.0/1234/CONTAINER/path/to/file.mp4"
        self.swift_service.add_handler("PUT", put_path, self.object_put_handler)

        with self.run_services():
            with self.assertRaises(ClientException):
                storage_object.load_from_filename(tmp_file.name)

        self.swift_service.assert_requested_n_times(
            "PUT", "/v2.0/1234/CONTAINER/path/to/file.mp4", 1)

    def test_load_from_filename_does_not_retry_with_invalid_keystone_creds(self) -> None:
        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = tempfile.NamedTemporaryFile()
        tmp_file.write(b"FOOBAR")
        tmp_file.flush()

        with self.assert_raises_on_forbidden_keystone_access():
            storage_object.load_from_filename(tmp_file.name)

        with self.assert_raises_on_unauthorized_keystone_access():
            storage_object.load_from_filename(tmp_file.name)

        self.swift_service.assert_not_requested("PUT", "/v2.0/1234/CONTAINER/path/to/file.mp4")

    def test_load_from_filename_raises_on_authentication_server_errors(self) -> None:
        self.auth_failure = "500 Internal Server Error"

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        tmp_file = tempfile.NamedTemporaryFile()
        tmp_file.write(b"FOOBAR")
        tmp_file.flush()

        with self.run_services():
            with self.assertRaises(InternalServerError):
                storage_object.load_from_filename(tmp_file.name)

    def test_delete_raises_on_missing_parameters(self) -> None:
        self.assert_requires_all_parameters("/path/to/file.mp4")

    def test_delete_makes_delete_request_against_swift_service(self) -> None:
        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self.expect_delete_object("/v2.0/1234/CONTAINER", "/path/to/file.mp4"):
            with self.run_services():
                storage_object.delete()

    def test_delete_raises_on_authentication_server_errors(self) -> None:
        self.auth_failure = "500 Internal Server Error"

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            with self.assertRaises(InternalServerError):
                storage_object.delete()

    def test_delete_raises_raises_not_found_error_when_file_does_not_exist(self) -> None:
        self.remaining_file_delete_failures = ["404 Not Found"]

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            with self.assertRaises(NotFoundError):
                storage_object.delete()

    def test_delete_raises_raises_original_exception_when_not_404(self) -> None:
        self.remaining_file_delete_failures = ["500 Error"]

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        delete_path = "/v2.0/1234/CONTAINER/path/to/file.mp4"
        self.swift_service.add_handler("DELETE", delete_path, self.object_delete_handler)

        with self.run_services():
            with self.assertRaises(ClientException):
                storage_object.delete()

    def test_delete_does_not_retry_on_swift_server_errors(self) -> None:
        self.remaining_file_delete_failures = ["500 Error", "500 Error"]

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        self.container_contents["/path/to/file.mp4"] = b"UNDELETED!"
        delete_path = "/v2.0/1234/CONTAINER/path/to/file.mp4"
        self.swift_service.add_handler("DELETE", delete_path, self.object_delete_handler)

        with self.run_services():
            with self.assertRaises(ClientException):
                storage_object.delete()

        self.swift_service.assert_requested_n_times(
            "DELETE", "/v2.0/1234/CONTAINER/path/to/file.mp4", 1)

    @mock.patch("time.time")
    def test_get_download_url_raises_with_missing_download_url_key(
            self, mock_time: mock.Mock) -> None:
        mock_time.return_value = 9000

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            with self.assertRaises(SwiftStorageError):
                storage_object.get_download_url()

    @mock.patch("time.time")
    def test_get_download_url_returns_signed_url(self, mock_time: mock.Mock) -> None:
        mock_time.return_value = 9000

        swift_uri = self._generate_storage_uri("/path/to/file.mp4", {"download_url_key": "KEY"})
        storage_object = get_storage(swift_uri)

        with self.run_services():
            url = storage_object.get_download_url()

        parsed = urlparse(url)
        expected = urlparse(self.swift_service.url("/v2.0/1234/CONTAINER/path/to/file.mp4"))

        self.assertEqual(parsed.path, expected.path)
        self.assertEqual(parsed.netloc, expected.netloc)

        query = dict(parse_qsl(parsed.query))

        self.assertEqual("9060", query["temp_url_expires"])
        self.assertTrue("temp_url_sig" in query)

    @mock.patch("time.time")
    def test_get_download_url_accepts_variable_seconds(self, mock_time: mock.Mock) -> None:
        mock_time.return_value = 9000

        swift_uri = self._generate_storage_uri("/path/to/file.mp4", {"download_url_key": "KEY"})
        storage_object = get_storage(swift_uri)

        with self.run_services():
            url = storage_object.get_download_url(seconds=120)

        parsed = urlparse(url)
        query = dict(parse_qsl(parsed.query))

        self.assertEqual("9120", query["temp_url_expires"])

    def test_get_sanitized_uri_returns_storage_uri_without_username_and_password(self) -> None:
        query_args = {
            "auth_endpoint": self.identity_service.url("/v2.0"),
            "tenant_id": "1234",
            "region": "DFW"
        }

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            sanitized_uri = storage_object.get_sanitized_uri()

        self.assertEqual(
            f"swift://container/path/to/file.mp4?{urlencode(query_args)}", sanitized_uri)

    def test_get_sanitized_uri_returns_storage_uri_without_download_url_key(self) -> None:
        swift_uri = self._generate_storage_uri("/path/to/file.mp4", {"download_url_key": "KEY"})
        storage_object = get_storage(swift_uri)

        with self.run_services():
            sanitized_uri = storage_object.get_sanitized_uri()

        parsed = urlparse(sanitized_uri)
        query = dict(parse_qsl(parsed.query))

        self.assertEqual({
            "auth_endpoint": self.identity_service.url("/v2.0"),
            "tenant_id": "1234",
            "region": "DFW"
        }, query)

    def generate_signature(self, path: str, key: bytes, expires: int = 60) -> str:
        timestamp = time.time()
        raw_string = f"GET\n{timestamp + expires}\n/v2.0/1234/CONTAINER{path}"
        return hmac.new(key, raw_string.encode("utf8"), sha256).hexdigest()

    @mock.patch("time.time")
    def test_get_download_url_uses_download_url_key_by_default(self, mock_time: mock.Mock) -> None:
        mock_time.return_value = 9000

        swift_uri = self._generate_storage_uri("/path/to/file.mp4", {"download_url_key": "KEY"})
        storage_object = get_storage(swift_uri)

        with self.run_services():
            url = storage_object.get_download_url()

        parsed = urlparse(url)
        signature = dict(parse_qsl(parsed.query))["temp_url_sig"]
        expected_signature = self.generate_signature("/path/to/file.mp4", b"KEY")

        self.assertEqual(expected_signature, signature)

    @mock.patch("time.time")
    def test_get_download_url_uses_alternate_download_url_key(self, mock_time: mock.Mock) -> None:
        mock_time.return_value = 9000

        swift_uri = self._generate_storage_uri("/path/to/file.mp4", {"download_url_key": "FOOBAR"})
        storage_object = get_storage(swift_uri)

        with self.run_services():
            url = storage_object.get_download_url()

        parsed = urlparse(url)
        signature = dict(parse_qsl(parsed.query))["temp_url_sig"]

        expected_signature = self.generate_signature("/path/to/file.mp4", b"FOOBAR")
        self.assertEqual(expected_signature, signature)

    @mock.patch("time.time")
    def test_get_download_url_uses_provided_key(self, mock_time: mock.Mock) -> None:
        mock_time.return_value = 9000

        swift_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            url = storage_object.get_download_url(key="BLARGH")

        parsed = urlparse(url)
        signature = dict(parse_qsl(parsed.query))["temp_url_sig"]

        expected_signature = self.generate_signature("/path/to/file.mp4", b"BLARGH")
        self.assertEqual(expected_signature, signature)

    @mock.patch("time.time")
    def test_get_download_url_overrides_download_url_key_with_provided_key(
            self, mock_time: mock.Mock) -> None:
        mock_time.return_value = 9000

        swift_uri = self._generate_storage_uri("/path/to/file.mp4", {"download_url_key": "FOOBAR"})
        storage_object = get_storage(swift_uri)

        with self.run_services():
            url = storage_object.get_download_url(key="BARFOO")

        parsed = urlparse(url)
        signature = dict(parse_qsl(parsed.query))["temp_url_sig"]

        expected_signature = self.generate_signature("/path/to/file.mp4", b"BARFOO")
        self.assertEqual(expected_signature, signature)

    def test_save_to_directory_raises_exception_when_missing_required_parameters(self) -> None:
        self.assert_requires_all_parameters("/path/to/files")

    def test_save_to_directory_raises_on_forbidden_keystone_credentials(self) -> None:
        self.add_container_object("/v2.0/1234/CONTAINER", "/path/to/files/file.mp4", b"FOOBAR")
        self.add_container_object(
            "/v2.0/1234/CONTAINER", "/path/to/files/other_file.mp4", b"BARFOO")

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.assert_raises_on_forbidden_keystone_access():
            storage_object.save_to_directory(self.tmp_dir.name)

        with self.assert_raises_on_unauthorized_keystone_access():
            storage_object.save_to_directory(self.tmp_dir.name)

    def test_save_to_directory_downloads_files_matching_prefix_to_directory_location(self) -> None:
        self._add_file_to_directory("/path/to/files/file.mp4", b"Contents")
        self._add_file_to_directory("/path/to/files/other_file.mp4", b"Other Contents")

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            storage_object.save_to_directory(self.tmp_dir.name)

        self.assert_container_contents_equal("/path/to/files")

    def test_save_to_directory_includes_subdirectories_in_local_path(self) -> None:
        self._add_file_to_directory("/path/to/files/file.mp4", b"Contents")
        self._add_file_to_directory("/path/to/files/other_file.mp4", b"Other Contents")
        self._add_file_to_directory("/path/to/files/folder/file2.mp4", b"Video Content")
        self._add_file_to_directory("/path/to/files/folder2/folder3/files3.mp4", b"Video Contents")

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            storage_object.save_to_directory(self.tmp_dir.name)

        self.assert_container_contents_equal("/path/to/files")

    def test_save_to_directory_ignores_directory_placeholder_objects(self) -> None:
        self._add_file_to_directory("/path/to/files/file.mp4", b"Contents")
        self._add_file_to_directory("/path/to/files/other_file.mp4", b"Other Contents")
        self._add_file_to_directory("/path/to/files/folder/", b"")
        self._add_file_to_directory("/path/to/files/folder/file2.mp4", b"Video Content")
        self._add_file_to_directory("/path/to/files/folder2/folder3/files3.mp4", b"Video Contents")

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            storage_object.save_to_directory(self.tmp_dir.name)

        self.assert_container_contents_equal("/path/to/files")

    def test_save_to_directory_retries_on_error(self) -> None:
        self.remaining_file_failures.append("500 Internal server error")
        self._add_file_to_directory("/path/to/files/file.mp4", b"Contents")

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            storage_object.save_to_directory(self.tmp_dir.name)

        self.swift_service.assert_requested_n_times(
            "GET", "/v2.0/1234/CONTAINER/path/to/files/file.mp4", 2)

    def test_save_to_directory_only_retries_put_object_when_store_object_fails(self) -> None:
        self.remaining_file_failures.append("500 Internal server error")
        self.remaining_file_failures.append("500 Internal server error")
        self._add_file_to_directory("/path/to/files/file.mp4", b"Contents")
        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            storage_object.save_to_directory(self.tmp_dir.name)

        self.identity_service.assert_requested_n_times("POST", "/v2.0/tokens", 1)
        self.swift_service.assert_requested("GET", "/v2.0/1234/CONTAINER")
        self.swift_service.assert_requested_n_times(
            "GET", "/v2.0/1234/CONTAINER/path/to/files/file.mp4", 3)

    def test_save_to_directory_raises_internal_server_exception(self) -> None:
        self.auth_failure = "500 Internal Server Error"

        self._add_file_to_directory("/path/to/files/file.mp4", b"contents")

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            with self.assertRaises(InternalServerError):
                storage_object.save_to_directory(self.tmp_dir.name)

    def test_save_to_directory_raises_not_found_error_when_directory_does_not_exist(self) -> None:
        self.swift_service.add_handler("GET", "/v2.0/1234/CONTAINER", self.swift_container_handler)
        self.remaining_container_failures.append("404 Not Found")

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            with self.assertRaises(NotFoundError):
                storage_object.save_to_directory(self.tmp_dir.name)

    def test_save_to_directory_raises_not_found_error_when_empty(self) -> None:
        self.container_contents = []
        self.swift_service.add_handler("GET", "/v2.0/1234/CONTAINER", self.swift_container_handler)

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            with self.assertRaises(NotFoundError):
                storage_object.save_to_directory(self.tmp_dir.name)

    def test_save_to_directory_raises_original_exception_when_not_404(self) -> None:
        self.swift_service.add_handler("GET", "/v2.0/1234/CONTAINER", self.swift_container_handler)
        self.remaining_container_failures.append("500 Internal server error")

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            with self.assertRaises(ClientException):
                storage_object.save_to_directory(self.tmp_dir.name)

    def test_load_from_directory_raises_when_missing_required_parameters(self) -> None:
        self.assert_requires_all_parameters("/path/to/files")

    def test_load_from_directory_puts_file_contents_at_object_endpoint(self) -> None:
        self._add_tmp_file_to_dir(self.tmp_dir.name, b"FOOBAR")
        self._add_tmp_file_to_dir(self.tmp_dir.name, b"FIZZBUZZ")

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.expect_directory("/path/to/files"):
            with self.run_services():
                storage_object.load_from_directory(self.tmp_dir.name)

    def test_load_from_directory_sends_guessed_content_type_from_extension(self) -> None:
        temp_file = self._add_tmp_file_to_dir(self.tmp_dir.name, b"FOOBAR", suffix=".mp4")
        temp_file2 = self._add_tmp_file_to_dir(self.tmp_dir.name, b"FIZZBUZZ", suffix=".jpg")

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.expect_directory("/path/to/files"):
            with self.run_services():
                storage_object.load_from_directory(self.tmp_dir.name)

        request = self.swift_service.assert_requested(
            "PUT", f"/v2.0/1234/CONTAINER/path/to/files/{os.path.basename(temp_file.name)}")
        request.assert_header_equals("Content-type", "video/mp4")

        other_request = self.swift_service.assert_requested(
            "PUT", f"/v2.0/1234/CONTAINER/path/to/files/{os.path.basename(temp_file2.name)}")
        other_request.assert_header_equals("Content-type", "image/jpeg")

    def test_load_from_directory_sends_default_content_type_when_none_is_determined(self) -> None:
        temp_file = self._add_tmp_file_to_dir(self.tmp_dir.name, b"FOOBAR", suffix=".mp4")
        temp_file2 = self._add_tmp_file_to_dir(self.tmp_dir.name, b"FIZZBUZZ")

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.expect_directory("/path/to/files"):
            with self.run_services():
                storage_object.load_from_directory(self.tmp_dir.name)

        request = self.swift_service.assert_requested(
            "PUT", f"/v2.0/1234/CONTAINER/path/to/files/{os.path.basename(temp_file.name)}")
        request.assert_header_equals("Content-type", "video/mp4")

        other_request = self.swift_service.assert_requested(
            "PUT", f"/v2.0/1234/CONTAINER/path/to/files/{os.path.basename(temp_file2.name)}")
        other_request.assert_header_equals("Content-type", "application/octet-stream")

    def test_load_from_directory_does_not_retry_with_invalid_keystone_creds(self) -> None:
        file1 = self._add_tmp_file_to_dir(self.tmp_dir.name, b"FOOBAR")
        file2 = self._add_tmp_file_to_dir(self.tmp_dir.name, b"FIZZBUZZ")

        swift_uri = self._generate_storage_uri("/path/to/files")
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

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.expect_directory("/path/to/files"):
            with self.run_services():
                storage_object.load_from_directory(self.tmp_dir.name)

        file1_requests = self.swift_service.get_all_requests(
            "PUT", f"/v2.0/1234/CONTAINER/path/to/files/{os.path.basename(file1.name)}")
        file2_requests = self.swift_service.get_all_requests(
            "PUT", f"/v2.0/1234/CONTAINER/path/to/files/{os.path.basename(file2.name)}")
        self.assertCountEqual([2, 1], [len(file1_requests), len(file2_requests)])

    def test_load_from_directory_includes_subdirectories_in_object_endpoint(self) -> None:
        dir_name = os.path.join(self.tmp_dir.name, "files2")
        self._add_tmp_file_to_dir(dir_name, b"NESTED")

        self._add_tmp_file_to_dir(self.tmp_dir.name, b"FOOBAR")
        self._add_tmp_file_to_dir(self.tmp_dir.name, b"FIZZBUZZ")

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.expect_directory("/path/to/files"):
            with self.run_services():
                storage_object.load_from_directory(self.tmp_dir.name)

        self.assert_container_contents_equal("/path/to/files")

    def test_load_from_directory_raises_internal_server_exception(self) -> None:
        self.auth_failure = "500 Internal Server Error"

        self._add_tmp_file_to_dir(self.tmp_dir.name, b"FOOBAR")

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            with self.assertRaises(InternalServerError):
                storage_object.load_from_directory(self.tmp_dir.name)

    def test_delete_directory_raises_on_missing_parameters(self) -> None:
        self.assert_requires_all_parameters("/path/to/files")

    def test_delete_directory_makes_delete_request_against_swift_service(self) -> None:
        self.swift_service.add_handler("GET", "/v2.0/1234/CONTAINER", self.swift_container_handler)
        self.container_contents["/path/to/files/file.mp4"] = b"Contents"
        self.container_contents["/path/to/files/folder/file2.mp4"] = b"Video Content"

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.expect_delete_directory("/path/to/files"):
            with self.run_services():
                storage_object.delete_directory()

    def test_delete_directory_raises_on_authentication_server_errors(self) -> None:
        self.swift_service.add_handler("GET", "/v2.0/1234/CONTAINER", self.swift_container_handler)
        self.auth_failure = "500 Internal Server Error"

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            with self.assertRaises(InternalServerError):
                storage_object.delete_directory()

    def test_delete_directory_does_not_retry_on_swift_server_errors(self) -> None:
        self.swift_service.add_handler("GET", "/v2.0/1234/CONTAINER", self.swift_container_handler)
        self.remaining_file_delete_failures = ["500 Error", "500 Error"]
        self.container_contents["/path/to/files/file.mp4"] = b"UNDELETED"
        self.container_contents["/path/to/files/folder/file2.mp4"] = b"UNDELETED"

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        expected_delete_paths = []
        for name in self.container_contents:
            delete_path = f"/v2.0/1234/CONTAINER/{strip_slashes(name)}"
            expected_delete_paths.append(delete_path)
            self.swift_service.add_handler("DELETE", delete_path, self.object_delete_handler)

        with self.run_services():
            with self.assertRaises(ClientException):
                storage_object.delete_directory()

        file1_requests = self.swift_service.get_all_requests(
            "DELETE", "/v2.0/1234/CONTAINER/path/to/files/file.mp4")
        file2_requests = self.swift_service.get_all_requests(
            "DELETE", "/v2.0/1234/CONTAINER/path/to/files/folder/file2.mp4")
        self.assertCountEqual([1, 0], [len(file1_requests), len(file2_requests)])

    def test_delete_to_directory_raises_not_found_error_when_directory_does_not_exist(self) -> None:
        self.container_contents = []
        self.swift_service.add_handler("GET", "/v2.0/1234/CONTAINER", self.swift_container_handler)

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            with self.assertRaises(NotFoundError):
                storage_object.delete_directory()

    def test_delete_to_directory_raises_not_found_error_when_empty(self) -> None:
        self.swift_service.add_handler("GET", "/v2.0/1234/CONTAINER", self.swift_container_handler)
        self.remaining_container_failures.append("404 Not Found")

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            with self.assertRaises(NotFoundError):
                storage_object.delete_directory()

    def test_delete_to_directory_raises_original_exception_when_not_404(self) -> None:
        self.swift_service.add_handler("GET", "/v2.0/1234/CONTAINER", self.swift_container_handler)
        self.remaining_container_failures.append("500 Internal server error")

        swift_uri = self._generate_storage_uri("/path/to/files")
        storage_object = get_storage(swift_uri)

        with self.run_services():
            with self.assertRaises(ClientException):
                storage_object.delete_directory()

    def test_swift_rejects_multiple_query_values_for_auth_endpoint_setting(self) -> None:
        self.assert_rejects_multiple_query_values("object.mp4", "auth_endpoint")

    def test_swift_rejects_multiple_query_values_for_region_setting(self) -> None:
        self.assert_rejects_multiple_query_values("object.mp4", "region", values=["DFW", "ORD"])

    def test_swift_rejects_multiple_query_values_for_download_url_key_setting(self) -> None:
        self.assert_rejects_multiple_query_values(
            "object.mp4", "download_url_key", values=["KEY", "ALT-KEY"])

    def test_swift_rejects_multiple_query_values_for_tenant_setting(self) -> None:
        self.assert_rejects_multiple_query_values("object.mp4", "tenant_id", values=["1234", "567"])
