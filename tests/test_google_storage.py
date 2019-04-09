import base64
import datetime
import json
from unittest import TestCase

import mock

from storage import get_storage


class TestGoogleStorage(TestCase):
    def setUp(self):
        super(TestGoogleStorage, self).setUp()

        credentials = base64.urlsafe_b64encode(json.dumps({"SOME": "CREDENTIALS"}))

        self.storage = get_storage("gs://{}@bucketname/path/filename".format(credentials))

        service_account_patcher = mock.patch(
            "google.oauth2.service_account.Credentials.from_service_account_info")
        self.mock_from_service_account_info = service_account_patcher.start()
        self.addCleanup(service_account_patcher.stop)

        client_patcher = mock.patch("google.cloud.storage.client.Client")
        self.mock_client_class = client_patcher.start()
        self.addCleanup(client_patcher.stop)

        self.mock_credentials = self.mock_from_service_account_info.return_value
        self.mock_client = self.mock_client_class.return_value
        self.mock_bucket = self.mock_client.get_bucket.return_value
        self.mock_blob = self.mock_bucket.blob.return_value

    def test_save_to_filename_downloads_blob_to_file_location(self):
        self.storage.save_to_filename("SOME-FILE")

        self.mock_from_service_account_info.assert_called_once_with({"SOME": "CREDENTIALS"})

        self.mock_client_class.assert_called_once_with(credentials=self.mock_credentials)
        self.mock_client.get_bucket.assert_called_once_with("bucketname")
        self.mock_bucket.blob.assert_called_once_with("path/filename")
        self.mock_blob.download_to_filename.assert_called_once_with("SOME-FILE")

    def test_save_to_file_downloads_blob_to_file_object(self):
        mock_file = mock.Mock()

        self.storage.save_to_file(mock_file)

        self.mock_from_service_account_info.assert_called_once_with({"SOME": "CREDENTIALS"})

        self.mock_client_class.assert_called_once_with(credentials=self.mock_credentials)
        self.mock_client.get_bucket.assert_called_once_with("bucketname")
        self.mock_bucket.blob.assert_called_once_with("path/filename")
        self.mock_blob.download_to_file.assert_called_once_with(mock_file)

    def test_load_from_filename_uploads_blob_from_file_location(self):
        self.storage.load_from_filename("SOME-FILE")

        self.mock_from_service_account_info.assert_called_once_with({"SOME": "CREDENTIALS"})

        self.mock_client_class.assert_called_once_with(credentials=self.mock_credentials)
        self.mock_client.get_bucket.assert_called_once_with("bucketname")
        self.mock_bucket.blob.assert_called_once_with("path/filename")
        self.mock_blob.upload_from_filename.assert_called_once_with("SOME-FILE")

    def test_load_from_file_uploads_blob_from_file_object(self):
        mock_file = mock.Mock()

        self.storage.load_from_file(mock_file)

        self.mock_from_service_account_info.assert_called_once_with({"SOME": "CREDENTIALS"})

        self.mock_client_class.assert_called_once_with(credentials=self.mock_credentials)
        self.mock_client.get_bucket.assert_called_once_with("bucketname")
        self.mock_bucket.blob.assert_called_once_with("path/filename")
        self.mock_blob.upload_from_file.assert_called_once_with(mock_file)

    def test_delete_deletes_blob(self):
        self.storage.delete()

        self.mock_from_service_account_info.assert_called_once_with({"SOME": "CREDENTIALS"})

        self.mock_client_class.assert_called_once_with(credentials=self.mock_credentials)
        self.mock_client.get_bucket.assert_called_once_with("bucketname")
        self.mock_bucket.blob.assert_called_once_with("path/filename")
        self.mock_blob.delete.assert_called_once_with()

    def test_get_download_url_returns_signed_url_with_default_expiration(self):
        mock_signed_url = self.mock_blob.generate_signed_url.return_value

        result = self.storage.get_download_url()

        self.assertEqual(mock_signed_url, result)

        self.mock_from_service_account_info.assert_called_once_with({"SOME": "CREDENTIALS"})

        self.mock_client_class.assert_called_once_with(credentials=self.mock_credentials)
        self.mock_client.get_bucket.assert_called_once_with("bucketname")
        self.mock_bucket.blob.assert_called_once_with("path/filename")
        self.mock_blob.generate_signed_url.assert_called_once_with(datetime.timedelta(seconds=60))

    def test_get_download_url_returns_signed_url_with_provided_expiration(self):
        self.storage.get_download_url(1000)

        self.mock_blob.generate_signed_url.assert_called_once_with(datetime.timedelta(seconds=1000))

    def test_get_download_url_does_not_use_key_when_provided(self):
        self.storage.get_download_url(key="KEY")

        self.mock_blob.generate_signed_url.assert_called_once_with(datetime.timedelta(seconds=60))
