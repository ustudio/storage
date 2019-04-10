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

    def assert_gets_bucket_with_credentials(self):
        self.mock_from_service_account_info.assert_called_once_with({"SOME": "CREDENTIALS"})
        self.mock_client_class.assert_called_once_with(credentials=self.mock_credentials)
        self.mock_client.get_bucket.assert_called_once_with("bucketname")

    def test_save_to_filename_downloads_blob_to_file_location(self):
        self.storage.save_to_filename("SOME-FILE")

        self.assert_gets_bucket_with_credentials()

        self.mock_bucket.blob.assert_called_once_with("path/filename")
        self.mock_blob.download_to_filename.assert_called_once_with("SOME-FILE")

    def test_save_to_file_downloads_blob_to_file_object(self):
        mock_file = mock.Mock()

        self.storage.save_to_file(mock_file)

        self.assert_gets_bucket_with_credentials()

        self.mock_bucket.blob.assert_called_once_with("path/filename")
        self.mock_blob.download_to_file.assert_called_once_with(mock_file)

    def test_load_from_filename_uploads_blob_from_file_location(self):
        self.storage.load_from_filename("SOME-FILE")

        self.assert_gets_bucket_with_credentials()

        self.mock_bucket.blob.assert_called_once_with("path/filename")
        self.mock_blob.upload_from_filename.assert_called_once_with("SOME-FILE")

    def test_load_from_file_uploads_blob_from_file_object(self):
        mock_file = mock.Mock()

        self.storage.load_from_file(mock_file)

        self.assert_gets_bucket_with_credentials()

        self.mock_bucket.blob.assert_called_once_with("path/filename")
        self.mock_blob.upload_from_file.assert_called_once_with(mock_file)

    def test_delete_deletes_blob(self):
        self.storage.delete()

        self.assert_gets_bucket_with_credentials()

        self.mock_bucket.blob.assert_called_once_with("path/filename")
        self.mock_blob.delete.assert_called_once_with()

    def test_get_download_url_returns_signed_url_with_default_expiration(self):
        mock_signed_url = self.mock_blob.generate_signed_url.return_value

        result = self.storage.get_download_url()

        self.assertEqual(mock_signed_url, result)

        self.assert_gets_bucket_with_credentials()

        self.mock_bucket.blob.assert_called_once_with("path/filename")
        self.mock_blob.generate_signed_url.assert_called_once_with(datetime.timedelta(seconds=60))

    def test_get_download_url_returns_signed_url_with_provided_expiration(self):
        self.storage.get_download_url(1000)

        self.mock_blob.generate_signed_url.assert_called_once_with(datetime.timedelta(seconds=1000))

    def test_get_download_url_does_not_use_key_when_provided(self):
        self.storage.get_download_url(key="KEY")

        self.mock_blob.generate_signed_url.assert_called_once_with(datetime.timedelta(seconds=60))

    def _mock_blob(self, name):
        blob = mock.Mock()
        blob.name = name
        return blob

    @mock.patch("os.path.exists")
    @mock.patch("os.makedirs")
    def test_save_to_directory_downloads_blobs_matching_prefix_to_directory_location(
            self, mock_makedirs, mock_exists):
        mock_exists.side_effect = [True, False, False]

        mock_blobs = [
            self._mock_blob("path/filename/file1"),
            self._mock_blob("path/filename/subdir1/subdir2/file2"),
            self._mock_blob("path/filename/subdir3/path/filename/file3")
        ]
        self.mock_bucket.list_blobs.return_value = iter(mock_blobs)

        self.storage.save_to_directory("directory-name")

        self.assert_gets_bucket_with_credentials()

        self.mock_bucket.list_blobs.assert_called_once_with(prefix="path/filename/")

        mock_blobs[0].download_to_filename.assert_called_once_with("directory-name/file1")
        mock_blobs[1].download_to_filename.assert_called_once_with(
            "directory-name/subdir1/subdir2/file2")
        mock_blobs[2].download_to_filename.assert_called_once_with(
            "directory-name/subdir3/path/filename/file3")

        self.assertEqual(
            [
                mock.call("directory-name"),
                mock.call("directory-name/subdir1/subdir2"),
                mock.call("directory-name/subdir3/path/filename")
            ],
            mock_exists.call_args_list)

        self.assertEqual(
            [
                mock.call("directory-name/subdir1/subdir2"),
                mock.call("directory-name/subdir3/path/filename")
            ],
            mock_makedirs.call_args_list)

    @mock.patch("os.path.exists")
    @mock.patch("os.makedirs")
    def test_save_to_directory_ignores_placeholder_directory_entries_when_present(
            self, mock_makedirs, mock_exists):
        mock_exists.side_effect = [False, True, False, False]

        mock_blobs = [
            self._mock_blob("path/filename/dir/"),
            self._mock_blob("path/filename/dir/file.txt"),
            self._mock_blob("path/filename/dir/emptysubdir/"),
            self._mock_blob("path/filename/emptydir/")
        ]
        self.mock_bucket.list_blobs.return_value = iter(mock_blobs)

        self.storage.save_to_directory("directory-name")

        self.assertEqual(0, mock_blobs[0].download_to_filename.call_count)
        mock_blobs[1].download_to_filename.assert_called_once_with(
            "directory-name/dir/file.txt")
        self.assertEqual(0, mock_blobs[2].download_to_filename.call_count)
        self.assertEqual(0, mock_blobs[3].download_to_filename.call_count)

        self.assertEqual(
            [
                mock.call("directory-name/dir"),
                mock.call("directory-name/dir"),
                mock.call("directory-name/dir/emptysubdir"),
                mock.call("directory-name/emptydir")
            ],
            mock_exists.call_args_list)

        self.assertEqual(
            [
                mock.call("directory-name/dir"),
                mock.call("directory-name/dir/emptysubdir"),
                mock.call("directory-name/emptydir")
            ],
            mock_makedirs.call_args_list)

    @mock.patch("os.path.exists")
    @mock.patch("random.uniform")
    @mock.patch("time.sleep")
    def test_save_to_directory_retries_file_download_on_error(
            self, mock_sleep, mock_uniform, mock_exists):
        mock_exists.return_value = True

        mock_blobs = [
            self._mock_blob("path/filename/file1"),
            self._mock_blob("path/filename/subdir1/subdir2/file2"),
            self._mock_blob("path/filename/subdir3/path/filename/file3")
        ]
        mock_blobs[1].download_to_filename.side_effect = [Exception, None]
        self.mock_bucket.list_blobs.return_value = iter(mock_blobs)

        self.storage.save_to_directory("directory-name")

        mock_blobs[0].download_to_filename.assert_called_once_with("directory-name/file1")

        self.assertEqual(2, mock_blobs[1].download_to_filename.call_count)
        mock_blobs[1].download_to_filename.assert_called_with(
            "directory-name/subdir1/subdir2/file2")

        mock_blobs[2].download_to_filename.assert_called_once_with(
            "directory-name/subdir3/path/filename/file3")

        mock_uniform.assert_called_once_with(0, 1)
        mock_sleep.assert_called_once_with(mock_uniform.return_value)

    @mock.patch("os.path.exists")
    @mock.patch("random.uniform")
    @mock.patch("time.sleep")
    def test_save_to_directory_fails_after_five_unsuccessful_download_attempts(
            self, mock_sleep, mock_uniform, mock_exists):
        mock_uniform_results = [mock.Mock() for i in range(4)]
        mock_uniform.side_effect = mock_uniform_results
        mock_exists.return_value = True

        mock_blobs = [
            self._mock_blob("path/filename/file1"),
            self._mock_blob("path/filename/subdir1/subdir2/file2"),
            self._mock_blob("path/filename/subdir3/path/filename/file3")
        ]
        mock_blobs[1].download_to_filename.side_effect = Exception
        self.mock_bucket.list_blobs.return_value = iter(mock_blobs)

        with self.assertRaises(Exception):
            self.storage.save_to_directory("directory-name")

        mock_blobs[0].download_to_filename.assert_called_once_with("directory-name/file1")

        self.assertEqual(5, mock_blobs[1].download_to_filename.call_count)
        mock_blobs[1].download_to_filename.assert_called_with(
            "directory-name/subdir1/subdir2/file2")

        self.assertEqual(0, mock_blobs[2].download_to_filename.call_count)

        mock_uniform.assert_has_calls([
            mock.call(0, 1),
            mock.call(0, 3),
            mock.call(0, 7),
            mock.call(0, 15)
        ])
        mock_sleep.assert_has_calls([
            mock.call(mock_uniform_results[0]),
            mock.call(mock_uniform_results[1]),
            mock.call(mock_uniform_results[2]),
            mock.call(mock_uniform_results[3])
        ])

    @mock.patch("os.walk")
    def test_load_from_directory_uploads_files_to_bucket_with_prefix(self, mock_walk):
        mock_blobs = [mock.Mock(), mock.Mock(), mock.Mock(), mock.Mock()]
        self.mock_bucket.blob.side_effect = mock_blobs

        mock_walk.return_value = [
            ("/path/to/directory-name", ["subdir", "emptysubdir"], ["root1"]),
            ("/path/to/directory-name/subdir", ["nesteddir"], ["sub1", "sub2"]),
            ("/path/to/directory-name/subdir/nesteddir", [], ["nested1"]),
            ("/path/to/directory-name/emptysubdir", [], [])
        ]

        self.storage.load_from_directory("/path/to/directory-name")

        self.assert_gets_bucket_with_credentials()

        mock_walk.assert_called_once_with("/path/to/directory-name")

        self.mock_bucket.blob.assert_has_calls([
            mock.call("path/filename/root1"),
            mock.call("path/filename/subdir/sub1"),
            mock.call("path/filename/subdir/sub2"),
            mock.call("path/filename/subdir/nesteddir/nested1")
        ])

        mock_blobs[0].upload_from_filename.assert_called_once_with("/path/to/directory-name/root1")
        mock_blobs[1].upload_from_filename.assert_called_once_with(
            "/path/to/directory-name/subdir/sub1")
        mock_blobs[2].upload_from_filename.assert_called_once_with(
            "/path/to/directory-name/subdir/sub2")
        mock_blobs[3].upload_from_filename.assert_called_once_with(
            "/path/to/directory-name/subdir/nesteddir/nested1")

    @mock.patch("os.walk")
    def test_load_from_directory_handles_repeated_directory_structure(self, mock_walk):
        mock_blobs = [mock.Mock(), mock.Mock()]
        self.mock_bucket.blob.side_effect = mock_blobs

        mock_walk.return_value = [
            ("dir/name", ["dir"], []),
            ("dir/name/dir", ["name"], []),
            ("dir/name/dir/name", ["foo"], ["file1"]),
            ("dir/name/dir/name/foo", [], ["file2"])
        ]

        self.storage.load_from_directory("dir/name")

        self.assert_gets_bucket_with_credentials()

        mock_walk.assert_called_once_with("dir/name")

        self.mock_bucket.blob.assert_has_calls([
            mock.call("path/filename/dir/name/file1"),
            mock.call("path/filename/dir/name/foo/file2")
        ])

        mock_blobs[0].upload_from_filename.assert_called_once_with("dir/name/dir/name/file1")
        mock_blobs[1].upload_from_filename.assert_called_once_with("dir/name/dir/name/foo/file2")

    @mock.patch("os.walk")
    @mock.patch("time.sleep")
    def test_load_from_directory_retries_file_upload_on_error(self, mock_sleep, mock_walk):
        mock_blobs = [mock.Mock(), mock.Mock(), mock.Mock()]
        mock_blobs[1].upload_from_filename.side_effect = [Exception, None]
        self.mock_bucket.blob.side_effect = mock_blobs

        mock_walk.return_value = [
            ("/dir", [], ["file1", "file2", "file3"])
        ]

        self.storage.load_from_directory("/dir")

        self.mock_bucket.blob.assert_has_calls([
            mock.call("path/filename/file1"),
            mock.call("path/filename/file2"),
            mock.call("path/filename/file3")
        ])

        mock_blobs[0].upload_from_filename.assert_called_once_with("/dir/file1")

        self.assertEqual(2, mock_blobs[1].upload_from_filename.call_count)
        mock_blobs[1].upload_from_filename.assert_called_with("/dir/file2")

        mock_blobs[2].upload_from_filename.assert_called_once_with("/dir/file3")

    @mock.patch("os.walk")
    @mock.patch("time.sleep")
    def test_load_from_directory_fails_after_five_unsuccessful_upload_attempts(
            self, mock_sleep, mock_walk):
        mock_blobs = [mock.Mock(), mock.Mock(), mock.Mock()]
        mock_blobs[1].upload_from_filename.side_effect = Exception
        self.mock_bucket.blob.side_effect = mock_blobs

        mock_walk.return_value = [
            ("/dir", [], ["file1", "file2", "file3"])
        ]

        with self.assertRaises(Exception):
            self.storage.load_from_directory("/dir")

        mock_blobs[0].upload_from_filename.assert_called_once_with("/dir/file1")

        self.assertEqual(5, mock_blobs[1].upload_from_filename.call_count)
        mock_blobs[1].upload_from_filename.assert_called_with("/dir/file2")

        self.assertEqual(0, mock_blobs[2].upload_from_filename.call_count)

    def test_delete_directory_deletes_blobs_with_prefix(self):
        mock_blobs = [mock.Mock(), mock.Mock(), mock.Mock()]
        self.mock_bucket.list_blobs.return_value = iter(mock_blobs)

        self.storage.delete_directory()

        self.assert_gets_bucket_with_credentials()

        self.mock_bucket.list_blobs.assert_called_once_with("path/filename/")

        mock_blobs[0].delete.assert_called_once_with()
        mock_blobs[1].delete.assert_called_once_with()
        mock_blobs[2].delete.assert_called_once_with()
