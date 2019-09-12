from urllib.parse import parse_qs

from keystoneauth1 import session
from keystoneauth1.identity import v2
import swiftclient

from typing import Any, Dict

from storage.storage import get_optional_query_parameter, InvalidStorageUri, DEFAULT_SWIFT_TIMEOUT
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

    def _validate_parsed_uri(self) -> None:
        query = parse_qs(self._parsed_storage_uri.query)
        public_value = get_optional_query_parameter(query, "public")
        public_value = public_value if public_value is not None else "true"
        self.public_endpoint = "publicURL" if public_value.lower() == "true" else "internalURL"
        region = get_optional_query_parameter(query, "region")
        self.region = region if region is not None else "DFW"
        self.download_url_key = get_optional_query_parameter(query, "download_url_key")

        if self._parsed_storage_uri.username == "":
            raise InvalidStorageUri("Missing username")
        if self._parsed_storage_uri.password == "":
            raise InvalidStorageUri("Missing API key")

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
