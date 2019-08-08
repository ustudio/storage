import swiftclient

from storage.swift_storage import register_swift_protocol, SwiftStorage


@register_swift_protocol("cloudfiles", "https://identity.api.rackspacecloud.com/v1.0")
class CloudFilesStorage(SwiftStorage):

    def get_connection(self):
        auth, _ = self._parsed_storage_uri.netloc.split("@")
        user, key = auth.split(":", 1)

        return swiftclient.client.Connection(authurl=self.auth_endpoint, user=user, key=key)
