from urllib.parse import parse_qsl

from keystoneauth1 import session  # type: ignore
from keystoneauth1.identity import v2  # type: ignore
import swiftclient  # type: ignore

from typing import Any, Dict

from .storage import DEFAULT_SWIFT_TIMEOUT
from storage.swift_storage import register_swift_protocol, SwiftStorage, SwiftStorageError


class RackspaceAuth(v2.Password):  # type: ignore

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

    def get_connection(self) -> swiftclient.client.Connection:
        if not hasattr(self, "_connection"):
            query = dict(parse_qsl(self._parsed_storage_uri.query))
            public_endpoint = query.get("public", "true").lower()

            os_options = {
                "region_name": query.get("region", "DFW"),
                "endpoint_type": "publicURL" if public_endpoint == "true" else "internalURL"
            }

            user = self._parsed_storage_uri.username
            key = self._parsed_storage_uri.password

            if user == "":
                raise SwiftStorageError(f"Missing username")
            if key == "":
                raise SwiftStorageError(f"Missing API key")

            auth = RackspaceAuth(auth_url=self.auth_endpoint, username=user, password=key)

            keystone_session = session.Session(auth=auth)

            connection = swiftclient.client.Connection(
                session=keystone_session, os_options=os_options, timeout=DEFAULT_SWIFT_TIMEOUT)

            self.download_url_key = query.get("download_url_key", None)
            if self.download_url_key is None:
                for header_key, header_value in connection.head_account().items():
                    if header_key.endswith("temp-url-key"):
                        self.download_url_key = header_value
                        break

            self._connection = connection
        return self._connection
