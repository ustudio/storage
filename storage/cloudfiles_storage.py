from urllib.parse import parse_qs

from keystoneauth1 import session
from keystoneauth1.identity import v2
import swiftclient

from typing import Any, Dict

from .storage import DEFAULT_SWIFT_TIMEOUT
from storage.storage import InvalidStorageUri
from storage.swift_storage import register_swift_protocol, SwiftStorage


class RackspaceAuth(v2.Password):

    def get_auth_data(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        auth_data = super().get_auth_data(*args, **kwargs)
        return {
            "RAX-KSKEY:apiKeyCredentials": {
                "username": auth_data["passwordCredentials"]["username"],
                "apiKey": auth_data["passwordCredentials"]["password"]
            }
        }


@register_swift_protocol("cloudfiles", "https://identity.api.rackspacecloud.com/v2.0")
class CloudFilesStorage(SwiftStorage):

    def validate_uri(self) -> None:
        query = parse_qs(self._parsed_storage_uri.query)
        if len(query.get("public", [])) > 1:
            raise InvalidStorageUri("Too many `public` query values.")
        if len(query.get("region", [])) > 1:
            raise InvalidStorageUri("Too many `region` query values.")
        if len(query.get("download_url_key", [])) > 1:
            raise InvalidStorageUri("Too many `download_url_key` query values.")

        if self._parsed_storage_uri.username == "":
            raise InvalidStorageUri("Missing username")
        if self._parsed_storage_uri.password == "":
            raise InvalidStorageUri("Missing API key")

        public_value = query.get("public", ["true"])[0].lower()
        self.public_endpoint = "publicURL" if public_value == "true" else "internalURL"
        self.region = query.get("region", ["DFW"])[0]
        download_url_key = query.get("download_url_key", [])
        self.download_url_key = download_url_key[0] if len(download_url_key) else None

    def get_connection(self) -> swiftclient.client.Connection:
        if not hasattr(self, "_connection"):
            os_options = {
                "region_name": self.region,
                "endpoint_type": self.public_endpoint
            }

            user = self._parsed_storage_uri.username
            key = self._parsed_storage_uri.password

            auth = RackspaceAuth(auth_url=self.auth_endpoint, username=user, password=key)

            keystone_session = session.Session(auth=auth)

            connection = swiftclient.client.Connection(
                session=keystone_session, os_options=os_options, timeout=DEFAULT_SWIFT_TIMEOUT)

            if self.download_url_key is None:
                for header_key, header_value in connection.head_account().items():
                    if header_key.endswith("temp-url-key"):
                        self.download_url_key = header_value
                        break

            self._connection = connection
        return self._connection
