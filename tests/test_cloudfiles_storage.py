import contextlib
import io
from unittest import mock

from storage import get_storage
from storage import cloudfiles_storage
from tests.swift_service_test_case import SwiftServiceTestCase


class TestCloudFilesStorageProvider(SwiftServiceTestCase):
    def setUp(self):
        super().setUp()

        self.object_contents = {}

        self.identity_service = self.add_service()
        self.identity_service.add_handler("GET", "/v1.0", self.authentication_handler)

        self.cloudfiles_service = self.add_service()

    def _generate_cloudfiles_uri(self, filename):
        return f"cloudfiles://USER:TOKEN@CONTAINER{filename}"

    def add_container_object(self, container_path, object_path, content) -> None:
        if type(content) is not bytes:
            raise Exception("Object file contents numst be bytes")

        self.object_contents[object_path] = content

        get_path = f"{container_path}{object_path}"
        self.cloudfiles_service.add_handler("GET", get_path, self.object_handler)

    @contextlib.contextmanager
    def use_local_identity_service(self):
        with mock.patch(
                "storage.cloudfiles_storage.CloudFilesStorage.auth_endpoint",
                new_callable=mock.PropertyMock) as mock_endpoint:
            mock_endpoint.return_value = self.identity_service.url("/v1.0")
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

    def authentication_handler(self, environ, start_response):
        # username = environ["HTTP_X_AUTH_USER"]
        # key = environ["HTTP_X_AUTH_KEY"]

        # CHECK THAT THEY ARE CORRECT~~~~~~~~~~~~

        headers = [
            ("Content-type", "application/json"),
            ("X-Storage-Token", "TOKEN"),
            ("X-Auth-Token", "TOKEN"),
            ("X-Tenant-Id", "TENANT"),
            ("X-Storage-Url", self.cloudfiles_service.url("/v1/MOSSO-TENANT"))
        ]
        start_response("204 No Content", headers)
        return [b""]

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
            "https://identity.api.rackspacecloud.com/v1.0",
            cloudfiles_storage.CloudFilesStorage.auth_endpoint)

    def test_save_to_file_writes_file_contents_to_file_object(self) -> None:
        self.add_container_object("/v1/MOSSO-TENANT/CONTAINER", "/path/to/file.mp4", b"FOOBAR")

        temp = io.BytesIO()

        cloudfiles_uri = self._generate_cloudfiles_uri("/path/to/file.mp4")
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.run_services():
                storage_object.save_to_file(temp)

        temp.seek(0)
        self.assertEqual(b"FOOBAR", temp.read())

    def test_load_from_file_puts_file_contents_at_object_endpoint(self) -> None:
        temp = io.BytesIO(b"FOOBAR")

        cloudfiles_uri = self._generate_cloudfiles_uri("/path/to/file.mp4")
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.expect_put_object(
                    "/v1/MOSSO-TENANT/CONTAINER", "/path/to/file.mp4", b"FOOBAR"):
                with self.run_services():
                    storage_object.load_from_file(temp)

    def test_delete_makes_delete_request_against_swift_service(self) -> None:
        cloudfiles_uri = self._generate_cloudfiles_uri("/path/to/file.mp4")
        storage_object = get_storage(cloudfiles_uri)

        with self.use_local_identity_service():
            with self.expect_delete_object("/v1/MOSSO-TENANT/CONTAINER", "/path/to/file.mp4"):
                with self.run_services():
                    storage_object.delete()
