import contextlib
import io
import json
from unittest import mock
from urllib.parse import urlencode

from storage import get_storage
from storage import cloudfiles_storage
from storage.swift_storage import SwiftStorageError
from tests.swift_service_test_case import SwiftServiceTestCase


class TestCloudFilesStorageProvider(SwiftServiceTestCase):
    def setUp(self):
        super().setUp()

        self.keystone_credentials = {
            "username": "USER",
            "key": "TOKEN"
        }

        self.object_contents = {}

        self.identity_service = self.add_service()
        self.identity_service.add_handler("GET", "/v2.0", self.identity_handler)
        self.identity_service.add_handler("POST", "/v2.0/tokens", self.authentication_handler)

        self.cloudfiles_service = self.add_service()
        self.alt_cloudfiles_service = self.add_service()
        self.internal_cloudfiles_service = self.add_service()

        self.mock_sleep_patch = mock.patch("time.sleep")
        self.mock_sleep = self.mock_sleep_patch.start()
        self.mock_sleep.side_effect = lambda x: None

    def tearDown(self):
        super().tearDown()
        self.mock_sleep_patch.stop()

    def _generate_cloudfiles_uri(self, object_path, parameters=None) -> str:
        base_uri = f"cloudfiles://USER:TOKEN@CONTAINER{object_path}"
        if parameters is not None:
            return f"{base_uri}?{urlencode(parameters)}"
        return base_uri

    def _has_valid_credentials(self, auth_data) -> bool:
        if auth_data["username"] == self.keystone_credentials["username"] and \
                auth_data["apiKey"] == self.keystone_credentials["key"]:
            return True
        else:
            return False

    def add_container_object(self, container_path, object_path, content) -> None:
        if type(content) is not bytes:
            raise Exception("Object file contents numst be bytes")

        self.object_contents[object_path] = content

        get_path = f"{container_path}{object_path}"
        self.cloudfiles_service.add_handler("GET", get_path, self.object_handler)

    def assert_requires_all_parameters(self, object_path, fn) -> None:
        for auth_string in ["USER:@", ":TOKEN@"]:
            cloudfiles_uri = f"cloudfiles://{auth_string}CONTAINER{object_path}"
            storage_object = get_storage(cloudfiles_uri)

            with self.run_services():
                with self.assertRaises(SwiftStorageError):
                    fn(storage_object)

    @contextlib.contextmanager
    def assert_raises_on_forbidden_access(self) -> None:
        self.keystone_credentials["username"] = "nobody"
        with self.run_services():
            with self.assertRaises(SwiftStorageError):
                yield

    @contextlib.contextmanager
    def assert_raises_on_unauthorized_access(self) -> None:
        self.keystone_credentials = {}
        with self.run_services():
            with self.assertRaises(SwiftStorageError):
                yield

    @contextlib.contextmanager
    def use_local_identity_service(self):
        with mock.patch(
                "storage.cloudfiles_storage.CloudFilesStorage.auth_endpoint",
                new_callable=mock.PropertyMock) as mock_endpoint:
            mock_endpoint.return_value = self.identity_service.url("/v2.0")
            yield

    @contextlib.contextmanager
    def expect_put_object(self, container_path, object_path, content) -> None:
        put_path = f"{container_path}{object_path}"
        self.cloudfiles_service.add_handler("PUT", put_path, self.object_put_handler)
        yield
        self.assertEqual(content, self.container_contents[object_path])
        self.cloudfiles_service.assert_requested("PUT", put_path)

    @contextlib.contextmanager
    def expect_delete_object(self, container_path, object_path):
        self.container_contents[object_path] = b"UNDELETED!"
        delete_path = f"{container_path}{object_path}"
        self.cloudfiles_service.add_handler("DELETE", delete_path, self.object_delete_handler)
        yield
        self.assertNotIn(
            object_path, self.container_contents,
            f"File {object_path} was not deleted as expected.")

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

    def authentication_handler(self, environ, start_response):
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
                            "publicURL": self.cloudfiles_service.url("/v2.0/MOSSO-TENANT"),
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

    def object_handler(self, environ, start_response):
        path = environ["REQUEST_PATH"].split("CONTAINER")[1]
        start_response("200 OK", [("Content-type", "video/mp4")])
        return [self.object_contents[path]]

    def object_put_handler(self, environ, start_response):
        path = environ["REQUEST_PATH"].split("CONTAINER")[1]

        header = b""
        while not header.endswith(b"\r\n"):
            header += environ["wsgi.input"].read(1)

        body_size = int(header.strip())
        self.container_contents[path] = environ["wsgi.input"].read(body_size)

        start_response("201 OK", [("Content-type", "text/plain")])
        return [b""]

    def object_delete_handler(self, environ, start_response):
        path = environ["REQUEST_PATH"].split("CONTAINER")[1]
        del self.container_contents[path]
        start_response("204 OK", [("Content-type", "text-plain")])
        return [b""]

    def test_cloudfiles_default_auth_endpoint_points_to_correct_host(self) -> None:
        self.assertEqual(
            "https://identity.api.rackspacecloud.com/v2.0",
            cloudfiles_storage.CloudFilesStorage.auth_endpoint)

    def test_save_to_file_raises_exception_when_missing_required_parameters(self) -> None:
        self.add_container_object("/v2.0/MOSSO-TENANT/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        temp = io.BytesIO()

        with self.use_local_identity_service():
            self.assert_requires_all_parameters("/path/to/file.mp4", lambda x: x.save_to_file(temp))

    def test_save_to_file_raises_on_forbidden_credentials(self) -> None:
        self.add_container_object("/v2.0/MOSSO-TENANT/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        temp = io.BytesIO()

        cloudfiles_uri = self._generate_cloudfiles_uri("/path/to/file.mp4")
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

        cloudfiles_uri = self._generate_cloudfiles_uri("/path/to/file.mp4")
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.run_services():
                storage_object.save_to_file(temp)

        temp.seek(0)
        self.assertEqual(b"FOOBAR", temp.read())

    def test_save_to_file_uses_default_region_when_one_is_not_provided(self) -> None:
        self.add_container_object("/v2.0/MOSSO-TENANT/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        temp = io.BytesIO()

        cloudfiles_uri = self._generate_cloudfiles_uri("/path/to/file.mp4")
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.run_services():
                storage_object.save_to_file(temp)

        self.cloudfiles_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 1)
        self.alt_cloudfiles_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 0)

    def test_save_to_file_uses_provided_region_parameter(self) -> None:
        self.object_contents["/path/to/file.mp4"] = b"FOOBAR"

        get_path = f"/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4"
        self.alt_cloudfiles_service.add_handler("GET", get_path, self.object_handler)

        temp = io.BytesIO()

        cloudfiles_uri = self._generate_cloudfiles_uri("/path/to/file.mp4", {"region": "ORD"})
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.run_services():
                storage_object.save_to_file(temp)

        self.alt_cloudfiles_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 1)
        self.cloudfiles_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 0)

    def test_save_to_file_uses_default_endpoint_type_when_one_is_not_provided(self) -> None:
        self.add_container_object("/v2.0/MOSSO-TENANT/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        temp = io.BytesIO()

        cloudfiles_uri = self._generate_cloudfiles_uri("/path/to/file.mp4")
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.run_services():
                storage_object.save_to_file(temp)

        self.cloudfiles_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 1)
        self.internal_cloudfiles_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 0)

    def test_save_to_file_uses_provided_public_parameter(self) -> None:
        self.object_contents["/path/to/file.mp4"] = b"FOOBAR"

        get_path = f"/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4"
        self.internal_cloudfiles_service.add_handler("GET", get_path, self.object_handler)

        temp = io.BytesIO()

        cloudfiles_uri = self._generate_cloudfiles_uri("/path/to/file.mp4", {"public": "false"})
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.run_services():
                storage_object.save_to_file(temp)

        self.internal_cloudfiles_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 1)
        self.cloudfiles_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 0)

    def test_save_to_file_uses_provided_public_parameter_case_insensitive(self) -> None:
        self.object_contents["/path/to/file.mp4"] = b"FOOBAR"

        get_path = f"/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4"
        self.internal_cloudfiles_service.add_handler("GET", get_path, self.object_handler)

        temp = io.BytesIO()

        cloudfiles_uri = self._generate_cloudfiles_uri("/path/to/file.mp4", {"public": "False"})
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.run_services():
                storage_object.save_to_file(temp)

        self.internal_cloudfiles_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 1)
        self.cloudfiles_service.assert_requested_n_times(
            "GET", "/v2.0/MOSSO-TENANT/CONTAINER/path/to/file.mp4", 0)

    def test_load_from_file_puts_file_contents_at_object_endpoint(self) -> None:
        temp = io.BytesIO(b"FOOBAR")

        cloudfiles_uri = self._generate_cloudfiles_uri("/path/to/file.mp4")
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.expect_put_object(
                    "/v2.0/MOSSO-TENANT/CONTAINER", "/path/to/file.mp4", b"FOOBAR"):
                with self.run_services():
                    storage_object.load_from_file(temp)

    def test_delete_makes_delete_request_against_swift_service(self) -> None:
        cloudfiles_uri = self._generate_cloudfiles_uri("/path/to/file.mp4")
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.expect_delete_object("/v2.0/MOSSO-TENANT/CONTAINER", "/path/to/file.mp4"):
                with self.run_services():
                    storage_object.delete()
