import contextlib
import io
import json
from unittest import mock
from urllib.parse import parse_qsl, urlencode, urlparse

from keystoneauth1.exceptions.http import Forbidden, Unauthorized
from typing import Dict, Generator, List, Optional, TYPE_CHECKING

from storage.storage import get_storage, InvalidStorageUri
from tests.storage_test_case import StorageTestCase
from tests.swift_service_test_case import SwiftServiceTestCase

if TYPE_CHECKING:
    from tests.service_test_case import Environ
    from wsgiref.types import StartResponse


class TestCloudFilesStorageProvider(StorageTestCase, SwiftServiceTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.download_url_key = {"download_url_key": "KEY"}
        self.keystone_credentials = {
            "username": "USER",
            "key": "TOKEN"
        }

        self.identity_service = self.add_service()
        self.identity_service.add_handler("GET", "/v2.0", self.identity_handler)
        self.identity_service.add_handler("POST", "/v2.0/tokens", self.authentication_handler)

        self.alt_cloudfiles_service = self.add_service()
        self.internal_cloudfiles_service = self.add_service()

        self.mock_sleep_patch = mock.patch("time.sleep")
        self.mock_sleep = self.mock_sleep_patch.start()
        self.mock_sleep.side_effect = lambda x: None

    def tearDown(self) -> None:
        super().tearDown()
        self.mock_sleep_patch.stop()

    def _generate_storage_uri(
            self, object_path: str, parameters: Optional[Dict[str, str]] = None) -> str:
        base_uri = f"cloudfiles://USER:TOKEN@CONTAINER{object_path}"
        if parameters is not None:
            return f"{base_uri}?{urlencode(parameters)}"
        return base_uri

    def _has_valid_credentials(self, auth_data: Dict[str, str]) -> bool:
        if auth_data["username"] == self.keystone_credentials["username"] and \
                auth_data["apiKey"] == self.keystone_credentials["key"]:
            return True
        else:
            return False

    def assert_requires_all_parameters(self, object_path: str) -> None:
        for auth_string in ["USER:@", ":TOKEN@"]:
            cloudfiles_uri = f"cloudfiles://{auth_string}CONTAINER{object_path}"

            with self.assertRaises(InvalidStorageUri):
                get_storage(cloudfiles_uri)

    @contextlib.contextmanager
    def assert_raises_on_forbidden_access(self) -> Generator[None, None, None]:
        self.keystone_credentials["username"] = "nobody"
        with self.run_services():
            with self.assertRaises(Forbidden):
                yield

    @contextlib.contextmanager
    def assert_raises_on_unauthorized_access(self) -> Generator[None, None, None]:
        self.keystone_credentials = {}
        with self.run_services():
            with self.assertRaises(Unauthorized):
                yield

    @contextlib.contextmanager
    def use_local_identity_service(self) -> Generator[None, None, None]:
        with mock.patch(
                "storage.cloudfiles_storage.CloudFilesStorage.auth_endpoint",
                new_callable=mock.PropertyMock) as mock_endpoint:
            mock_endpoint.return_value = self.identity_service.url("/v2.0")
            yield

    @contextlib.contextmanager
    def expect_head_account_object(self, path: str) -> Generator[None, None, None]:
        self.swift_service.add_handler("HEAD", path, self.object_head_account_handler)
        yield
        self.swift_service.assert_requested("HEAD", path)

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

    def authentication_handler(
            self, environ: "Environ", start_response: "StartResponse") -> List[bytes]:
        body_size = int(environ.get("CONTENT_LENGTH", 0))
        body = json.loads(environ["wsgi.input"].read(body_size))

        # Forcing a 401 since swift service won't let us provide it
        if self.keystone_credentials == {}:
            start_response("401 Unauthorized", [("Content-type", "text/plain")])
            return [b"Unauthorized keystone credentials."]
        if not self._has_valid_credentials(body["auth"]["RAX-KSKEY:apiKeyCredentials"]):
            start_response("403 Forbidden", [("Content-type", "text/plain")])
            return [b"Invalid keystone credentials."]

        start_response("200 OK", [("Content-type", "application/json")])
        return [json.dumps({
            "access": {
                "serviceCatalog": [{
                    "endpoints": [
                        {
                            "tenantId": "MOSSO-TENANT",
                            "publicURL": self.swift_service.url("/v2.0/MOSSO-TENANT"),
                            "internalURL": self.internal_cloudfiles_service.url(
                                "/v2.0/MOSSO-TENANT"),
                            "region": "DFW"
                        },
                        {
                            "tenantId": "MOSSO-TENANT",
                            "publicURL": self.alt_cloudfiles_service.url("/v2.0/MOSSO-TENANT"),
                            "internalURL": self.alt_cloudfiles_service.url("/v2.0/MOSSO-TENANT"),
                            "region": "ORD"
                        }
                    ],
                    "name": "cloudfiles",
                    "type": "object-store"
                }],
                "user": {
                    "RAX-AUTH:defaultRegion": "DFW",
                    "roles": [{
                        "name": "object-store:default",
                        "tenantId": "MOSSO-TENANT",
                        "id": "ID"
                    }],
                    "name": "USER",
                    "id": "IDENTIFIER"
                },
                "token": {
                    "expires": "2019-07-18T05:47:13.090Z",
                    "RAX-AUTH:authenticatedBy": ["APIKEY"],
                    "id": "KEY",
                    "tenant": {
                        "name": "MOSSO-TENANT",
                        "id": "MOSSO-TENANT"
                    }
                }
            }
        }).encode("utf8")]

    def object_head_account_handler(
            self, environ: "Environ", start_response: "StartResponse") -> List[bytes]:
        start_response("204 OK", [
            ("Content-type", "text-plain"),
            ("X-Account-Meta-Temp-Url-Key", "TEMPKEY")
        ])
        return [b""]

    def test_cloudfiles_default_auth_endpoint_points_to_correct_host(self) -> None:
        cloudfiles_uri = self._generate_storage_uri("/path/to/file.mp4", self.download_url_key)
        storage_object = get_storage(cloudfiles_uri)

        self.assertEqual(
            "https://identity.api.rackspacecloud.com/v2.0",
            storage_object.auth_endpoint)  # type: ignore

    def test_save_to_file_raises_exception_when_missing_required_parameters(self) -> None:
        self.assert_requires_all_parameters("/path/to/file.mp4")

    def test_save_to_file_raises_on_forbidden_credentials(self) -> None:
        self.add_container_object("/v2.0/MOSSO-TENANT/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        temp = io.BytesIO()

        cloudfiles_uri = self._generate_storage_uri("/path/to/file.mp4", self.download_url_key)
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.assert_raises_on_forbidden_access():
                storage_object.save_to_file(temp)

        with self.use_local_identity_service():
            with self.assert_raises_on_unauthorized_access():
                storage_object.save_to_file(temp)

    def test_save_to_file_writes_file_contents_to_file_object(self) -> None:
        self.add_container_object("/v2.0/MOSSO-TENANT/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        temp = io.BytesIO()

        cloudfiles_uri = self._generate_storage_uri("/path/to/file.mp4", self.download_url_key)
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.run_services():
                storage_object.save_to_file(temp)

        temp.seek(0)
        self.assertEqual(b"FOOBAR", temp.read())

    def test_save_to_file_uses_default_region_when_one_is_not_provided(self) -> None:
        self.add_container_object("/v2.0/MOSSO-TENANT/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        temp = io.BytesIO()

        cloudfiles_uri = self._generate_storage_uri("/path/to/file.mp4", self.download_url_key)
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.run_services():
                storage_object.save_to_file(temp)

        self.swift_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 1)
        self.alt_cloudfiles_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 0)

    def test_save_to_file_uses_provided_region_parameter(self) -> None:
        self.object_contents["/path/to/file.mp4"] = b"FOOBAR"

        get_path = f"/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4"
        self.alt_cloudfiles_service.add_handler("GET", get_path, self.object_handler)

        temp = io.BytesIO()

        cloudfiles_uri = self._generate_storage_uri("/path/to/file.mp4", {
            "region": "ORD",
            "download_url_key": "KEY"
        })
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.run_services():
                storage_object.save_to_file(temp)

        self.alt_cloudfiles_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 1)
        self.swift_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 0)

    def test_save_to_file_uses_default_endpoint_type_when_one_is_not_provided(self) -> None:
        self.add_container_object("/v2.0/MOSSO-TENANT/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        temp = io.BytesIO()

        cloudfiles_uri = self._generate_storage_uri("/path/to/file.mp4", self.download_url_key)
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.run_services():
                storage_object.save_to_file(temp)

        self.swift_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 1)
        self.internal_cloudfiles_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 0)

    def test_save_to_file_uses_provided_public_parameter(self) -> None:
        self.object_contents["/path/to/file.mp4"] = b"FOOBAR"

        get_path = f"/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4"
        self.internal_cloudfiles_service.add_handler("GET", get_path, self.object_handler)

        temp = io.BytesIO()

        cloudfiles_uri = self._generate_storage_uri("/path/to/file.mp4", {
            "public": "false",
            "download_url_key": "KEY"
        })
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.run_services():
                storage_object.save_to_file(temp)

        self.internal_cloudfiles_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 1)
        self.swift_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 0)

    def test_save_to_file_uses_provided_public_parameter_case_insensitive(self) -> None:
        self.object_contents["/path/to/file.mp4"] = b"FOOBAR"

        get_path = f"/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4"
        self.internal_cloudfiles_service.add_handler("GET", get_path, self.object_handler)

        temp = io.BytesIO()

        cloudfiles_uri = self._generate_storage_uri("/path/to/file.mp4", {
            "public": "False",
            "download_url_key": "KEY"
        })
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.run_services():
                storage_object.save_to_file(temp)

        self.internal_cloudfiles_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 1)
        self.swift_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 0)

    def test_load_from_file_puts_file_contents_at_object_endpoint(self) -> None:
        temp = io.BytesIO(b"FOOBAR")

        cloudfiles_uri = self._generate_storage_uri("/path/to/file.mp4", self.download_url_key)
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.expect_put_object(
                    "/v2.0/MOSSO-TENANT/CONTAINER", "/path/to/file.mp4", b"FOOBAR"):
                with self.run_services():
                    storage_object.load_from_file(temp)

    def test_delete_makes_delete_request_against_swift_service(self) -> None:
        cloudfiles_uri = self._generate_storage_uri("/path/to/file.mp4", self.download_url_key)
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.expect_delete_object("/v2.0/MOSSO-TENANT/CONTAINER", "/path/to/file.mp4"):
                with self.run_services():
                    storage_object.delete()

    @mock.patch("time.time")
    def test_get_download_url_returns_signed_url(self, mock_time: mock.Mock) -> None:
        mock_time.return_value = 9000

        cloudfiles_uri = self._generate_storage_uri("/path/to/file.mp4", self.download_url_key)
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.run_services():
                url = storage_object.get_download_url()

        parsed = urlparse(url)
        expected = urlparse(self.swift_service.url("/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4"))

        self.assertEqual(parsed.path, expected.path)
        self.assertEqual(parsed.netloc, expected.netloc)

        query = dict(parse_qsl(parsed.query))

        self.assertEqual("9060", query["temp_url_expires"])
        self.assertTrue("temp_url_sig" in query)

    @mock.patch("time.time")
    def test_get_download_url_uses_temp_url_key_when_download_url_key_not_present(
            self, mock_time: mock.Mock) -> None:
        mock_time.return_value = 9000

        cloudfiles_uri = self._generate_storage_uri("/path/to/file.mp4")
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.run_services():
                with self.expect_head_account_object("/v2.0/MOSSO-TENANT"):
                    url = storage_object.get_download_url()

        parsed = urlparse(url)
        expected = urlparse(self.swift_service.url("/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4"))

        self.assertEqual(parsed.path, expected.path)
        self.assertEqual(parsed.netloc, expected.netloc)

    def test_cloudfiles_rejects_multiple_query_values_for_public_setting(self) -> None:
        self.assert_rejects_multiple_query_values(
            "object.mp4", "public", values=["public", "private"])

    def test_cloudfiles_rejects_multiple_query_values_for_region_setting(self) -> None:
        self.assert_rejects_multiple_query_values("object.mp4", "region", values=["DFW", "ORD"])

    def test_cloudfiles_rejects_multiple_query_values_for_download_url_key_setting(self) -> None:
        self.assert_rejects_multiple_query_values("object.mp4", "download_url_key")

    def test_get_sanitized_uri_returns_storage_uri_without_username_and_password(self) -> None:
        cloudfiles_uri = self._generate_storage_uri("/path/to/file.mp4", self.download_url_key)
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.run_services():
                sanitized_uri = storage_object.get_sanitized_uri()

        self.assertEqual(
            "cloudfiles://container/path/to/file.mp4?download_url_key=KEY", sanitized_uri)
