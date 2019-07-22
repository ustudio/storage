from io import BytesIO
import json
from urllib.parse import urlencode

from storage import get_storage
from tests.service_test_case import ServiceTestCase


class TestSwift2(ServiceTestCase):
    def setUp(self):
        super().setUp()

        self.identity_service = self.add_service()
        self.identity_service.add_handler("GET", "/v2.0", self.identity_handler)
        self.identity_service.add_handler("POST", "/v2.0/tokens", self.authentitcation_handler)

        self.swift_service = self.add_service()
        self.swift_service.add_handler(
            "GET", "/v2.0/1234/CONTAINER/path/to/file.mp4", self.swift_object_handler)
        self.swift_service.add_handler(
            "GET", "/v2.0/1234/CONTAINER/path/to/other/file.mp4", self.swift_other_object_handler)

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

    def authentitcation_handler(self, environ, start_response):
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
        start_response("200 OK", [("Content-Type", "video/mp4")])
        return ["FOOBAR".encode("utf8")]

    def swift_other_object_handler(self, environ, start_response):
        start_response("200 OK", [("Content-Type", "video/mp4")])
        return ["BARFOO".encode("utf8")]

    def _generate_swift_uri(self, filename):
        base_uri = f"swift2://USER:KEY@CONTAINER/{filename}"
        uri_params = urlencode({
            "auth_endpoint": self.identity_service.url("/v2.0"),
            "tenant_id": "1234",
            "region": "DFW"
        })

        return f"{base_uri}?{uri_params}"

    def test_save_to_file_writes_file_contents_to_file_object(self) -> None:
        file_contents = "FOOBAR".encode("utf8")

        swift_uri = self._generate_swift_uri("path/to/file.mp4")

        storage = get_storage(swift_uri)

        tmp_file = BytesIO()

        with self.run_services():
            storage.save_to_file(tmp_file)

        tmp_file.seek(0)

        self.assertEqual(tmp_file.read(), file_contents)

    def test_save_to_file_writes_different_file_contents_to_file(self) -> None:
        file_contents = "BARFOO".encode("utf8")

        swift_uri = self._generate_swift_uri("path/to/other/file.mp4")

        storage = get_storage(swift_uri)

        tmp_file = BytesIO()

        with self.run_services():
            storage.save_to_file(tmp_file)

        tmp_file.seek(0)

        self.assertEqual(tmp_file.read(), file_contents)

    def test_fetch(self):
        from urllib.request import urlopen
        with self.run_services():
            result = urlopen(self.identity_service.url("/v2.0"))
            self.assertEqual(200, result.status)
            print(result.status, result.read())

    def test_save_to_file_raises_exception_when_missing_auth_url(self) -> None:
        pass

    def test_save_to_file_raises_exception_when_missing_username(self) -> None:
        pass

    def test_save_to_file_raises_exception_when_missing_password(self) -> None:
        pass
