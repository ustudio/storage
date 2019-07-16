from unittest import TestCase, mock

from storage import get_storage


class TestSwiftStorage(TestCase):
    def setUp(self):
        super().setUp()

        self.object_content = mock.Mock()

        client_patcher = mock.patch("swiftclient.client.Connection")
        self.mock_client_class = client_patcher.start()
        self.addCleanup(client_patcher.stop)

        self.mock_client = self.mock_client_class.return_value
        self.mock_client.url = "https://storage101.com/v1/AUTH_account"
        self.mock_client.get_object.return_value = [
            [{}, self.object_content],
            [{}, self.object_content]
        ]

    def _swift_storage_url(self):
        return "swift://username:pwd@containername/path/to/file.mp4?region=REG" \
            "&auth_endpoint=http://identity.svr.com:1234/v2&tenant_id=123456"

    def assert_create_connection_with_credentials(self):
        self.mock_client_class.assert_called_once_with(
            authurl="http://identity.svr.com:1234/v2", user="username", key="pwd")

    def assert_gets_object_with_credentials(self):
        self.assert_create_connection_with_credentials()
        self.mock_client.get_object.assert_called_once_with(
            "containername", "path/to/file.mp4", resp_chunk_size=33554432)

    def assert_put_object_with_credentials(self, mock_file):
        self.assert_create_connection_with_credentials()
        self.mock_client.put_object.assert_called_once_with(
            "containername", "path/to/file.mp4", contents=mock_file, content_type="video/mp4")

    def test_save_to_file_downloads_file_to_file_object(self):
        mock_file = mock.Mock()

        storage = get_storage(self._swift_storage_url())

        storage.save_to_file(mock_file)

        self.assert_gets_object_with_credentials()

        mock_file.write.assert_has_calls([
            mock.call(self.object_content),
            mock.call(self.object_content)
        ])

    @mock.patch("builtins.open")
    def test_save_to_filename_downloads_file_to_file_location(self, mock_open):
        mock_file = mock.Mock()

        storage = get_storage(self._swift_storage_url())

        storage.save_to_filename(mock_file)

        self.assert_gets_object_with_credentials()

        mock_open.assert_called_once_with(mock_file, "wb")
        mock_open.return_value.__enter__.return_value.write.assert_has_calls([
            mock.call(self.object_content),
            mock.call(self.object_content)
        ])

    def test_load_from_file_uploads_file_from_file_object(self):
        mock_file = mock.Mock()

        storage = get_storage(self._swift_storage_url())

        storage.load_from_file(mock_file)

        self.assert_put_object_with_credentials(mock_file)

    @mock.patch("builtins.open")
    def test_load_from_filename_uploads_file_from_file_location(self, mock_open):
        file_data = "objectcontent"
        mock_file = mock.Mock()
        mock_open.return_value.__enter__.return_value = file_data

        storage = get_storage(self._swift_storage_url())

        storage.load_from_filename(mock_file)

        self.assert_put_object_with_credentials(file_data)

        mock_open.assert_called_once_with(mock_file, "rb")

    def test_delete_deletes_storage_object(self):
        storage = get_storage(self._swift_storage_url())

        storage.delete()

        self.assert_create_connection_with_credentials()

        self.mock_client.delete_object.assert_called_once_with("containername", "path/to/file.mp4")

    @mock.patch("storage.swift_storage.generate_temp_url")
    def test_get_download_url_returns_signed_url_with_default_expiration(self, mock_generate_url):
        base_url = self._swift_storage_url()

        storage = get_storage(f"{base_url}&download_url_key=super_secret_key")

        storage.get_download_url()

        self.assert_create_connection_with_credentials()

        mock_generate_url.assert_called_once_with(
            "/v1/AUTH_account/containername/path/to/file.mp4", 60, "super_secret_key", "GET")

    @mock.patch("storage.swift_storage.generate_temp_url")
    def test_get_download_url_returns_signed_url_with_provided_expiration(self, mock_generate_url):
        base_url = self._swift_storage_url()

        storage = get_storage(f"{base_url}&download_url_key=super_secret_key")

        storage.get_download_url(seconds=1000)

        self.assert_create_connection_with_credentials()

        mock_generate_url.assert_called_once_with(
            "/v1/AUTH_account/containername/path/to/file.mp4", 1000, "super_secret_key", "GET")

    @mock.patch("storage.swift_storage.generate_temp_url")
    def test_get_download_url_does_not_use_key_when_provided(self, mock_generate_url):
        base_url = self._swift_storage_url()

        storage = get_storage(f"{base_url}&download_url_key=super_secret_key")

        storage.get_download_url(key="ALT_KEY")

        self.assert_create_connection_with_credentials()

        mock_generate_url.assert_called_once_with(
            "/v1/AUTH_account/containername/path/to/file.mp4", 60, "ALT_KEY", "GET")
