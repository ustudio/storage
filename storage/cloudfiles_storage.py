from urllib.parse import parse_qsl

from keystoneauth1 import session
from keystoneauth1.identity import v2
import swiftclient

from .storage import DEFAULT_SWIFT_TIMEOUT
from storage.swift_storage import register_swift_protocol, SwiftStorage, SwiftStorageError

ENDPOINT_TYPE_MAP = {"true": "publicURL", "false": "internalURL"}


class RackspaceAuth(v2.Password):

    def get_auth_data(self, *args, **kwargs):
        auth_data = super().get_auth_data(*args, **kwargs)
        return {
            "RAX-KSKEY:apiKeyCredentials": {
                "username": auth_data["passwordCredentials"]["username"],
                "apiKey": auth_data["passwordCredentials"]["password"]
            }
        }


@register_swift_protocol("cloudfiles", "https://identity.api.rackspacecloud.com/v2.0")
class CloudFilesStorage(SwiftStorage):

    def get_connection(self):
        if not hasattr(self, "_connection"):
            query = dict(parse_qsl(self._parsed_storage_uri.query))
            endpoint_type = query.get("public", "true").lower()

            os_options = {
                "region_name": query.get("region", "DFW"),
                "endpoint_type": ENDPOINT_TYPE_MAP[endpoint_type]
            }

            auth, _ = self._parsed_storage_uri.netloc.split("@")
            user, key = auth.split(":", 1)

            if user == "":
                raise SwiftStorageError(f"Missing username")
            if key == "":
                raise SwiftStorageError(f"Missing API key")

            auth = RackspaceAuth(auth_url=self.auth_endpoint, username=user, password=key)

            keystone_session = session.Session(auth=auth)

            connection = swiftclient.client.Connection(
                session=keystone_session, os_options=os_options, timeout=DEFAULT_SWIFT_TIMEOUT)
            self._connection = connection
        return self._connection
