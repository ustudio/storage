import swiftclient

from storage.swift_storage import register_swift_protocol, SwiftStorage, SwiftStorageError


@register_swift_protocol("cloudfiles", "https://identity.api.rackspacecloud.com/v1.0")
class CloudFilesStorage(SwiftStorage):

    def get_connection(self):
        if not hasattr(self, "_connection"):
            auth, _ = self._parsed_storage_uri.netloc.split("@")
            user, key = auth.split(":", 1)

            if user == "":
                raise SwiftStorageError(f"Missing username")
            if key == "":
                raise SwiftStorageError(f"Missing API key")

            connection = swiftclient.client.Connection(
                authurl=self.auth_endpoint, user=user, key=key)
            self._connection = connection
        return self._connection
