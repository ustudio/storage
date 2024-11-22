import os
from unittest import mock, TestCase
from urllib.parse import quote

from typing import cast, Dict, List, Optional
from botocore.exceptions import ClientError

from storage.storage import get_storage, InvalidStorageUri, NotFoundError
from storage.s3_storage import S3Storage
from tests.helpers import create_temp_nested_directory_with_files, NestedDirectoryDict
from tests.helpers import cleanup_nested_directory
from tests.storage_test_case import StorageTestCase


class TestS3Storage(StorageTestCase, TestCase):

    temp_directory: Optional[NestedDirectoryDict]

    def setUp(self) -> None:
        super().setUp()
        self.temp_directory = None

    def tearDown(self) -> None:
        super().tearDown()
        if self.temp_directory is not None:
            cleanup_nested_directory(self.temp_directory)

    def _generate_storage_uri(
            self, object_path: str, parameters: Optional[Dict[str, str]] = None) -> str:
        return "s3://access_key:access_secret@bucket/some/file"

    def test_requires_username_in_uri(self) -> None:
        with self.assertRaises(InvalidStorageUri):
            get_storage("s3://hostname/path")

    def test_requires_password_in_uri(self) -> None:
        with self.assertRaises(InvalidStorageUri):
            get_storage("s3://username@hostname/path")

    def test_requires_hostname_in_uri(self) -> None:
        with self.assertRaises(InvalidStorageUri):
            get_storage("s3://username:password@/path")

    def test_s3storage_init_sets_correct_keyname(self) -> None:
        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        self.assertEqual("some/file", cast(S3Storage, storage)._keyname)

    @mock.patch("boto3.session.Session", autospec=True)
    def test_handles_urlencoded_keys(self, mock_session_class: mock.Mock) -> None:
        encoded_key = quote("access/key", safe="")
        encoded_secret = quote("access/secret", safe="")

        storage = get_storage(
            "s3://{0}:{1}@bucket/some/file?region=US_EAST".format(encoded_key, encoded_secret))

        cast(S3Storage, storage)._connect()

        mock_session_class.assert_called_with(
            aws_access_key_id="access/key",
            aws_secret_access_key="access/secret",
            region_name="US_EAST")

    @mock.patch("boto3.session.Session", autospec=True)
    def test_load_from_file(self, mock_session_class: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value

        mock_file = mock.Mock()

        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        storage.load_from_file(mock_file)

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name="US_EAST")

        mock_session.client.assert_called_with("s3")

        mock_s3.put_object.assert_called_with(Bucket="bucket", Key="some/file", Body=mock_file)

    @mock.patch("boto3.session.Session", autospec=True)
    def test_load_from_file_guesses_content_type_based_on_filename(
            self, mock_session_class: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value

        mock_file = mock.Mock()

        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/whatever.jpg")

        storage.load_from_file(mock_file)

        mock_s3.put_object.assert_called_with(
            Bucket="bucket", Key="some/whatever.jpg", Body=mock_file, ContentType="image/jpeg")

    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    @mock.patch("boto3.session.Session", autospec=True)
    def test_load_from_filename(
            self, mock_session_class: mock.Mock, mock_transfer_class: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value
        mock_transfer = mock_transfer_class.return_value

        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        storage.load_from_filename("source/file")

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name="US_EAST")

        mock_session.client.assert_called_with("s3")

        mock_transfer_class.assert_called_with(mock_s3)

        mock_transfer.upload_file.assert_called_with(
            "source/file", "bucket", "some/file", extra_args=None)

    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    @mock.patch("boto3.session.Session", autospec=True)
    def test_load_from_filename_guesses_content_type_based_on_filename(
            self, mock_session_class: mock.Mock, mock_transfer_class: mock.Mock) -> None:
        mock_transfer = mock_transfer_class.return_value

        storage = get_storage("s3://access_key:access_secret@bucket/some/file")

        storage.load_from_filename("source/whatever.jpeg")

        mock_transfer.upload_file.assert_called_with(
            "source/whatever.jpeg", "bucket", "some/file", extra_args={"ContentType": "image/jpeg"})

    @mock.patch("boto3.session.Session", autospec=True)
    def test_save_to_file(self, mock_session_class: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value

        mock_body = mock.Mock()
        mock_body.read.side_effect = [b"some", b"file", b"contents", None]
        mock_s3.get_object.return_value = {
            "Body": mock_body
        }

        mock_file = mock.Mock()

        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")
        storage.save_to_file(mock_file)

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name="US_EAST")

        mock_session.client.assert_called_with("s3")
        mock_s3.get_object.assert_called_with(Bucket="bucket", Key="some/file")
        mock_file.write.assert_has_calls([
            mock.call(b"some"),
            mock.call(b"file"),
            mock.call(b"contents")
        ], any_order=False)

    @mock.patch("boto3.session.Session", autospec=True)
    def test_save_to_file_raises_when_file_does_not_exist(
            self, mock_session_class: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value
        mock_s3.get_object.return_value = {}
        mock_file = mock.Mock()

        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        with self.assertRaises(NotFoundError):
            storage.save_to_file(mock_file)

        mock_session.client.assert_called_with("s3")
        mock_s3.get_object.assert_called_with(Bucket="bucket", Key="some/file")
        mock_file.write.assert_not_called()

    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    @mock.patch("boto3.session.Session", autospec=True)
    def test_save_to_filename(
            self, mock_session_class: mock.Mock, mock_transfer_class: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value

        mock_transfer = mock_transfer_class.return_value

        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        storage.save_to_filename("destination/file")

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name="US_EAST")

        mock_session.client.assert_called_with("s3")

        mock_transfer_class.assert_called_with(mock_s3)
        mock_transfer.download_file.assert_called_with("bucket", "some/file", "destination/file")

    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    @mock.patch("boto3.session.Session", autospec=True)
    def test_save_to_filename_raises_not_found_error_when_file_does_not_exist(
            self, mock_session_class: mock.Mock, mock_transfer_class: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value

        mock_transfer = mock_transfer_class.return_value
        mock_transfer.download_file.side_effect = ClientError({
            "Error": {
                "Code": "404",
                "Message": "Not Found"
            }
        }, {})

        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        with self.assertRaises(NotFoundError):
            storage.save_to_filename("destination/directory")

        mock_transfer_class.assert_called_with(mock_s3)
        mock_transfer.download_file.assert_called_with(
            "bucket", "some/file", "destination/directory")

    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    @mock.patch("boto3.session.Session", autospec=True)
    def test_save_to_filename_raises_original_exception_when_not_404(
            self, mock_session_class: mock.Mock, mock_transfer_class: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value

        mock_transfer = mock_transfer_class.return_value
        mock_transfer.download_file.side_effect = ClientError({
            "Error": {
                "Code": "403",
                "Message": "Forbidden"
            }
        }, {})

        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        with self.assertRaises(ClientError):
            storage.save_to_filename("destination/directory")

        mock_transfer_class.assert_called_with(mock_s3)
        mock_transfer.download_file.assert_called_with(
            "bucket", "some/file", "destination/directory")

    @mock.patch("os.makedirs")
    @mock.patch("os.path.exists")
    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    @mock.patch("boto3.session.Session", autospec=True)
    def test_save_to_directory(
            self, mock_session_class: mock.Mock, mock_transfer_class: mock.Mock,
            mock_path_exists: mock.Mock, mock_makedirs: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3_client = mock_session.client.return_value
        mock_s3_client.list_objects.return_value = {
            "Contents": [
                {
                    "Key": "directory/"
                },
                {
                    "Key": "directory/b/"
                },
                {
                    "Key": "directory/b/c.txt"
                },
                {
                    "Key": "directory/e.txt"
                },
                {
                    "Key": "directory/e/f/d.txt"
                },
                {
                    "Key": ""
                },
                {
                    "Key": "directory/g.txt"
                }
            ]
        }

        mock_path_exists.return_value = False
        path_mock_path_exists_calls: List[str] = []

        def mock_path_exists_side_effect(path: str) -> bool:
            if any(path in x for x in path_mock_path_exists_calls):
                return True
            else:
                path_mock_path_exists_calls.append(path)
                return False

        mock_path_exists.side_effect = mock_path_exists_side_effect

        storage = get_storage(
            "s3://access_key:access_secret@bucket/directory?region=US_EAST")

        storage.save_to_directory("save_to_directory")

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name="US_EAST")

        mock_s3_client.list_objects.assert_called_with(Bucket="bucket", Prefix="directory/")
        mock_makedirs.assert_has_calls(
            [mock.call("save_to_directory/b"), mock.call("save_to_directory/e/f")])
        mock_session.client.assert_called_with("s3")

        mock_s3_client.download_file.assert_has_calls([
            mock.call("bucket", "directory/b/c.txt", "save_to_directory/b/c.txt"),
            mock.call("bucket", "directory/e.txt", "save_to_directory/e.txt"),
            mock.call("bucket", "directory/e/f/d.txt", "save_to_directory/e/f/d.txt"),
            mock.call("bucket", "directory/g.txt", "save_to_directory/g.txt")])

    @mock.patch("storage.retry.time.sleep", autospec=True)
    @mock.patch("storage.retry.random.uniform", autospec=True)
    @mock.patch("os.makedirs")
    @mock.patch("os.path.exists")
    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    @mock.patch("boto3.session.Session", autospec=True)
    def test_save_to_directory_retries_failed_file_uploads(
            self, mock_session_class: mock.Mock, mock_transfer_class: mock.Mock,
            mock_path_exists: mock.Mock, mock_makedirs: mock.Mock, mock_uniform: mock.Mock,
            mock_sleep: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3_client = mock_session.client.return_value
        mock_s3_client.list_objects.return_value = {
            "Contents": [
                {
                    "Key": "directory/"
                },
                {
                    "Key": "directory/b/"
                },
                {
                    "Key": "directory/b/c.txt"
                },
                {
                    "Key": "directory/e.txt"
                },
                {
                    "Key": "directory/e/f/d.txt"
                },
                {
                    "Key": ""
                },
                {
                    "Key": "directory/g.txt"
                }
            ]
        }

        mock_path_exists.return_value = False
        path_mock_path_exists_calls: List[str] = []

        def mock_path_exists_side_effect(path: str) -> bool:
            if any(path in x for x in path_mock_path_exists_calls):
                return True
            else:
                path_mock_path_exists_calls.append(path)
                return False

        mock_path_exists.side_effect = mock_path_exists_side_effect

        mock_s3_client.download_file.side_effect = [
            None,
            IOError,
            None,
            None,
            None
        ]

        storage = get_storage(
            "s3://access_key:access_secret@bucket/directory?region=US_EAST")

        storage.save_to_directory("save_to_directory")

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name="US_EAST")

        mock_s3_client.list_objects.assert_called_with(Bucket="bucket", Prefix="directory/")
        mock_makedirs.assert_has_calls(
            [mock.call("save_to_directory/b"), mock.call("save_to_directory/e/f")])
        mock_session.client.assert_called_with("s3")

        self.assertEqual(5, mock_s3_client.download_file.call_count)
        mock_s3_client.download_file.assert_has_calls([
            mock.call("bucket", "directory/b/c.txt", "save_to_directory/b/c.txt"),
            mock.call("bucket", "directory/e.txt", "save_to_directory/e.txt"),
            mock.call("bucket", "directory/e.txt", "save_to_directory/e.txt"),
            mock.call("bucket", "directory/e/f/d.txt", "save_to_directory/e/f/d.txt"),
            mock.call("bucket", "directory/g.txt", "save_to_directory/g.txt")])

        mock_uniform.assert_called_once_with(0, 1)

        mock_sleep.assert_called_once_with(mock_uniform.return_value)

    @mock.patch("storage.retry.time.sleep", autospec=True)
    @mock.patch("storage.retry.random.uniform", autospec=True)
    @mock.patch("os.makedirs")
    @mock.patch("os.path.exists")
    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    @mock.patch("boto3.session.Session", autospec=True)
    def test_save_to_directory_fails_after_five_failed_file_download_retries(
            self, mock_session_class: mock.Mock, mock_transfer_class: mock.Mock,
            mock_path_exists: mock.Mock, mock_makedirs: mock.Mock, mock_uniform: mock.Mock,
            mock_sleep: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3_client = mock_session.client.return_value
        mock_s3_client.list_objects.return_value = {
            "Contents": [
                {
                    "Key": "directory/"
                },
                {
                    "Key": "directory/b/"
                },
                {
                    "Key": "directory/b/c.txt"
                },
                {
                    "Key": "directory/e.txt"
                },
                {
                    "Key": "directory/e/f/d.txt"
                },
                {
                    "Key": ""
                },
                {
                    "Key": "directory/g.txt"
                }
            ]
        }

        mock_path_exists.return_value = False
        path_mock_path_exists_calls: List[str] = []

        def mock_path_exists_side_effect(path: str) -> bool:
            if any(path in x for x in path_mock_path_exists_calls):
                return True
            else:
                path_mock_path_exists_calls.append(path)
                return False

        mock_path_exists.side_effect = mock_path_exists_side_effect

        mock_s3_client.download_file.side_effect = [
            RuntimeError,
            RuntimeError,
            RuntimeError,
            RuntimeError,
            IOError,
        ]

        storage = get_storage(
            "s3://access_key:access_secret@bucket/directory?region=US_EAST")

        with self.assertRaises(IOError):
            storage.save_to_directory("save_to_directory")

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name="US_EAST")

        mock_s3_client.list_objects.assert_called_with(Bucket="bucket", Prefix="directory/")
        mock_makedirs.assert_called_once_with("save_to_directory/b")
        mock_session.client.assert_called_with("s3")

        self.assertEqual(5, mock_s3_client.download_file.call_count)
        mock_s3_client.download_file.assert_has_calls([
            mock.call("bucket", "directory/b/c.txt", "save_to_directory/b/c.txt"),
            mock.call("bucket", "directory/b/c.txt", "save_to_directory/b/c.txt"),
            mock.call("bucket", "directory/b/c.txt", "save_to_directory/b/c.txt"),
            mock.call("bucket", "directory/b/c.txt", "save_to_directory/b/c.txt"),
            mock.call("bucket", "directory/b/c.txt", "save_to_directory/b/c.txt")])

        mock_uniform.assert_has_calls([
            mock.call(0, 1),
            mock.call(0, 3),
            mock.call(0, 7),
            mock.call(0, 15)
        ])

        self.assertEqual(4, mock_sleep.call_count)
        mock_sleep.assert_called_with(mock_uniform.return_value)

    @mock.patch("os.makedirs")
    @mock.patch("os.path.exists")
    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    @mock.patch("boto3.session.Session", autospec=True)
    def test_save_to_directory_raises_when_empty(
            self, mock_session_class: mock.Mock, mock_transfer_class: mock.Mock,
            mock_path_exists: mock.Mock, mock_makedirs: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3_client = mock_session.client.return_value
        mock_s3_client.list_objects.return_value = {}

        storage = get_storage(
            "s3://access_key:access_secret@bucket/directory?region=US_EAST")

        with self.assertRaises(NotFoundError):
            storage.save_to_directory("save_to_directory")

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name="US_EAST")

        mock_session.client.assert_called_with("s3")

        mock_s3_client.list_objects.assert_called_with(Bucket="bucket", Prefix="directory/")

    @mock.patch("storage.retry.time.sleep", autospec=True)
    @mock.patch("storage.retry.random.uniform", autospec=True)
    @mock.patch("os.makedirs")
    @mock.patch("os.path.exists")
    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    @mock.patch("boto3.session.Session", autospec=True)
    def test_save_to_directory_raises_not_found_error_when_file_does_not_exist(
            self, mock_session_class: mock.Mock, mock_transfer_class: mock.Mock,
            mock_path_exists: mock.Mock, mock_makedirs: mock.Mock, mock_uniform: mock.Mock,
            mock_sleep: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3_client = mock_session.client.return_value
        mock_s3_client.list_objects.return_value = {
            "Contents": [
                {
                    "Key": "directory/"
                },
                {
                    "Key": "directory/b/"
                },
                {
                    "Key": "directory/b/c.txt"
                },
                {
                    "Key": "directory/e.txt"
                },
                {
                    "Key": "directory/e/f/d.txt"
                },
                {
                    "Key": ""
                },
                {
                    "Key": "directory/g.txt"
                }
            ]
        }

        mock_path_exists.return_value = False
        path_mock_path_exists_calls: List[str] = []

        def mock_path_exists_side_effect(path: str) -> bool:
            if any(path in x for x in path_mock_path_exists_calls):
                return True
            else:
                path_mock_path_exists_calls.append(path)
                return False

        mock_path_exists.side_effect = mock_path_exists_side_effect

        mock_s3_client.download_file.side_effect = [
            ClientError({
                "Error": {
                    "Code": "404",
                    "Message": "Not Found"
                }
            }, {}),
            ClientError({
                "Error": {
                    "Code": "404",
                    "Message": "Not Found"
                }
            }, {}),
            ClientError({
                "Error": {
                    "Code": "404",
                    "Message": "Not Found"
                }
            }, {}),
            ClientError({
                "Error": {
                    "Code": "404",
                    "Message": "Not Found"
                }
            }, {}),
            ClientError({
                "Error": {
                    "Code": "404",
                    "Message": "Not Found"
                }
            }, {})
        ]

        storage = get_storage(
            "s3://access_key:access_secret@bucket/directory?region=US_EAST")

        with self.assertRaises(NotFoundError):
            storage.save_to_directory("save_to_directory")

        mock_s3_client.download_file.assert_has_calls([
            mock.call("bucket", "directory/b/c.txt", "save_to_directory/b/c.txt"),
            mock.call("bucket", "directory/b/c.txt", "save_to_directory/b/c.txt"),
            mock.call("bucket", "directory/b/c.txt", "save_to_directory/b/c.txt"),
            mock.call("bucket", "directory/b/c.txt", "save_to_directory/b/c.txt"),
            mock.call("bucket", "directory/b/c.txt", "save_to_directory/b/c.txt")
        ])

    @mock.patch("storage.retry.time.sleep", autospec=True)
    @mock.patch("storage.retry.random.uniform", autospec=True)
    @mock.patch("os.makedirs")
    @mock.patch("os.path.exists")
    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    @mock.patch("boto3.session.Session", autospec=True)
    def test_save_to_directory_raises_original_exception_when_not_404(
            self, mock_session_class: mock.Mock, mock_transfer_class: mock.Mock,
            mock_path_exists: mock.Mock, mock_makedirs: mock.Mock, mock_uniform: mock.Mock,
            mock_sleep: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3_client = mock_session.client.return_value
        mock_s3_client.list_objects.return_value = {
            "Contents": [
                {
                    "Key": "directory/"
                },
                {
                    "Key": "directory/b/"
                },
                {
                    "Key": "directory/b/c.txt"
                },
                {
                    "Key": "directory/e.txt"
                },
                {
                    "Key": "directory/e/f/d.txt"
                },
                {
                    "Key": ""
                },
                {
                    "Key": "directory/g.txt"
                }
            ]
        }

        mock_path_exists.return_value = False
        path_mock_path_exists_calls: List[str] = []

        def mock_path_exists_side_effect(path: str) -> bool:
            if any(path in x for x in path_mock_path_exists_calls):
                return True
            else:
                path_mock_path_exists_calls.append(path)
                return False

        mock_path_exists.side_effect = mock_path_exists_side_effect

        mock_s3_client.download_file.side_effect = [
            ClientError({
                "Error": {
                    "Code": "403",
                    "Message": "Forbidden"
                }
            }, {}),
            ClientError({
                "Error": {
                    "Code": "403",
                    "Message": "Forbidden"
                }
            }, {}),
            ClientError({
                "Error": {
                    "Code": "403",
                    "Message": "Forbidden"
                }
            }, {}),
            ClientError({
                "Error": {
                    "Code": "403",
                    "Message": "Forbidden"
                }
            }, {}),
            ClientError({
                "Error": {
                    "Code": "403",
                    "Message": "Forbidden"
                }
            }, {})
        ]

        storage = get_storage(
            "s3://access_key:access_secret@bucket/directory?region=US_EAST")

        with self.assertRaises(ClientError):
            storage.save_to_directory("save_to_directory")

    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    @mock.patch("boto3.session.Session", autospec=True)
    def test_load_from_directory(
            self, mock_session_class: mock.Mock, mock_transfer_class: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3_client = mock_session.client.return_value

        self.temp_directory = create_temp_nested_directory_with_files([".png", ".unknown", ""])

        storage = get_storage(
            "s3://access_key:access_secret@bucket/dir?region=US_EAST")

        storage.load_from_directory(self.temp_directory["temp_directory"]["path"])

        mock_s3_client.upload_file.assert_has_calls([
            mock.call(
                self.temp_directory["temp_input_two"]["path"], "bucket",
                os.path.join("dir", self.temp_directory["temp_input_two"]["name"]),
                ExtraArgs=None),
            mock.call(
                self.temp_directory["temp_input_one"]["path"], "bucket",
                os.path.join("dir", self.temp_directory["temp_input_one"]["name"]),
                ExtraArgs={"ContentType": "image/png"}),
            mock.call(
                self.temp_directory["nested_temp_input"]["path"], "bucket",
                os.path.join(
                    "dir", self.temp_directory["nested_temp_directory"]["name"],
                    self.temp_directory["nested_temp_input"]["name"]),
                ExtraArgs=None)
        ], any_order=True)

    @mock.patch("storage.retry.time.sleep", autospec=True)
    @mock.patch("storage.retry.random.uniform", autospec=True)
    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    @mock.patch("boto3.session.Session", autospec=True)
    def test_load_from_directory_retries_failed_file_uploads(
            self, mock_session_class: mock.Mock, mock_transfer_class: mock.Mock,
            mock_uniform: mock.Mock, mock_sleep: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3_client = mock_session.client.return_value

        mock_s3_client.upload_file.side_effect = [
            None,
            IOError,
            None,
            None
        ]

        self.temp_directory = create_temp_nested_directory_with_files()

        storage = get_storage(
            "s3://access_key:access_secret@bucket/dir?region=US_EAST")

        storage.load_from_directory(self.temp_directory["temp_directory"]["path"])

        self.assertEqual(4, mock_s3_client.upload_file.call_count)
        mock_s3_client.upload_file.assert_has_calls([
            mock.call(
                self.temp_directory["temp_input_two"]["path"], "bucket",
                os.path.join("dir", self.temp_directory["temp_input_two"]["name"]),
                ExtraArgs=None),
            mock.call(
                self.temp_directory["temp_input_one"]["path"], "bucket",
                os.path.join("dir", self.temp_directory["temp_input_one"]["name"]),
                ExtraArgs=None),
            mock.call(
                self.temp_directory["nested_temp_input"]["path"], "bucket",
                os.path.join(
                    "dir", self.temp_directory["nested_temp_directory"]["name"],
                    self.temp_directory["nested_temp_input"]["name"]),
                ExtraArgs=None)
        ], any_order=True)
        self.assertEqual(
            mock_s3_client.upload_file.call_args_list[1],
            mock_s3_client.upload_file.call_args_list[2])

        mock_uniform.assert_called_once_with(0, 1)

        mock_sleep.assert_called_once_with(mock_uniform.return_value)

    @mock.patch("storage.retry.time.sleep", autospec=True)
    @mock.patch("storage.retry.random.uniform", autospec=True)
    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    @mock.patch("boto3.session.Session", autospec=True)
    def test_load_from_directory_fails_after_five_failed_file_upload_retries(
            self, mock_session_class: mock.Mock, mock_transfer_class: mock.Mock,
            mock_uniform: mock.Mock, mock_sleep: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3_client = mock_session.client.return_value

        mock_s3_client.upload_file.side_effect = [
            IOError,
            IOError,
            IOError,
            IOError,
            RuntimeError
        ]

        self.temp_directory = create_temp_nested_directory_with_files()

        storage = get_storage(
            "s3://access_key:access_secret@bucket/dir?region=US_EAST")

        with self.assertRaises(RuntimeError):
            storage.load_from_directory(self.temp_directory["temp_directory"]["path"])

        self.assertEqual(5, mock_s3_client.upload_file.call_count)
        self.assertEqual(
            mock_s3_client.upload_file.call_args, mock_s3_client.upload_file.call_args_list[0])
        self.assertEqual(
            mock_s3_client.upload_file.call_args, mock_s3_client.upload_file.call_args_list[1])
        self.assertEqual(
            mock_s3_client.upload_file.call_args, mock_s3_client.upload_file.call_args_list[2])
        self.assertEqual(
            mock_s3_client.upload_file.call_args, mock_s3_client.upload_file.call_args_list[3])

        mock_uniform.assert_has_calls([
            mock.call(0, 1),
            mock.call(0, 3),
            mock.call(0, 7),
            mock.call(0, 15)
        ])

        self.assertEqual(4, mock_sleep.call_count)
        mock_sleep.assert_called_with(mock_uniform.return_value)

    @mock.patch("boto3.session.Session", autospec=True)
    def test_delete(self, mock_session_class: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value
        mock_s3.delete_object.return_value = {
            "DeleteMarker": True
        }

        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        storage.delete()

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name="US_EAST")

        mock_session.client.assert_called_with("s3")

        mock_s3.delete_object.assert_called_with(Bucket="bucket", Key="some/file")

    @mock.patch("boto3.session.Session", autospec=True)
    def test_delete_raises_when_file_does_not_exist(self, mock_session_class: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value
        mock_s3.delete_object.return_value = {}

        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        with self.assertRaises(NotFoundError):
            storage.delete()

        mock_s3.delete_object.assert_called_with(Bucket="bucket", Key="some/file")

    @mock.patch("boto3.session.Session", autospec=True)
    def test_delete_directory(self, mock_session_class: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value

        mock_s3.list_objects.return_value = {
            "Contents": [
                {
                    "Key": "some/dir/",
                    "LastModified": "DATE-TIME",
                    "ETag": "tag",
                    "Size": 123,
                    "StorageClass": "STANDARD"
                },
                {
                    "Key": "some/dir/b/",
                    "LastModified": "DATE-TIME",
                    "ETag": "tag",
                    "Size": 123,
                    "StorageClass": "STANDARD"
                },
                {
                    "Key": "some/dir/b/c.txt",
                    "LastModified": "DATE-TIME",
                    "ETag": "tag",
                    "Size": 123,
                    "StorageClass": "STANDARD"
                },
                {
                    "Key": "some/dir/e.txt",
                    "LastModified": "DATE-TIME",
                    "ETag": "tag",
                    "Size": 123,
                    "StorageClass": "STANDARD"
                },
                {
                    "Key": "some/dir/e/f/d.txt",
                    "LastModified": "DATE-TIME",
                    "ETag": "tag",
                    "Size": 123,
                    "StorageClass": "STANDARD"
                },
                {
                    "Key": "some/dir/g.txt",
                    "LastModified": "DATE-TIME",
                    "ETag": "tag",
                    "Size": 123,
                    "StorageClass": "STANDARD"
                }
            ]
        }

        storage = get_storage("s3://access_key:access_secret@bucket/some/dir")

        storage.delete_directory()

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name=None)

        mock_session.client.assert_called_with("s3")

        mock_s3.list_objects.assert_called_once_with(Bucket="bucket", Prefix="some/dir/")
        mock_s3.delete_objects.assert_called_once_with(
            Bucket="bucket",
            Delete={
                "Objects": [
                    {
                        "Key": "some/dir/"
                    },
                    {
                        "Key": "some/dir/b/"
                    },
                    {
                        "Key": "some/dir/b/c.txt"
                    },
                    {
                        "Key": "some/dir/e.txt"
                    },
                    {
                        "Key": "some/dir/e/f/d.txt"
                    },
                    {
                        "Key": "some/dir/g.txt"
                    }
                ]
            })

    @mock.patch("boto3.session.Session", autospec=True)
    def test_delete_directory_raises_when_empty(self, mock_session_class: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value

        mock_s3.list_objects.return_value = {}

        storage = get_storage("s3://access_key:access_secret@bucket/some/dir")

        with self.assertRaises(NotFoundError):
            storage.delete_directory()

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name=None)

        mock_session.client.assert_called_with("s3")

        mock_s3.list_objects.assert_called_once_with(Bucket="bucket", Prefix="some/dir/")
        mock_s3.delete_objects.assert_not_called()

    @mock.patch("boto3.session.Session", autospec=True)
    def test_delete_directory_raises_not_found_error_when_files_do_not_exist(
            self, mock_session_class: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value

        mock_s3.list_objects.return_value = {
            "Contents": [
                {
                    "Key": "some/dir/",
                    "LastModified": "DATE-TIME",
                    "ETag": "tag",
                    "Size": 123,
                    "StorageClass": "STANDARD"
                }
            ]
        }

        mock_s3.delete_objects.side_effect = ClientError({
            "Error": {
                "Code": "404",
                "Message": "Not Found"
            }
        }, {})

        storage = get_storage("s3://access_key:access_secret@bucket/some/dir")

        with self.assertRaises(NotFoundError):
            storage.delete_directory()

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name=None)

        mock_session.client.assert_called_with("s3")

        mock_s3.list_objects.assert_called_once_with(Bucket="bucket", Prefix="some/dir/")
        mock_s3.delete_objects.assert_called_once()

    @mock.patch("boto3.session.Session", autospec=True)
    def test_delete_directory_raises_original_exception_when_not_404(
            self, mock_session_class: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value

        mock_s3.list_objects.return_value = {
            "Contents": [
                {
                    "Key": "some/dir/",
                    "LastModified": "DATE-TIME",
                    "ETag": "tag",
                    "Size": 123,
                    "StorageClass": "STANDARD"
                }
            ]
        }

        mock_s3.delete_objects.side_effect = ClientError({
            "Error": {
                "Code": "403",
                "Message": "Forbidden"
            }
        }, {})

        storage = get_storage("s3://access_key:access_secret@bucket/some/dir")

        with self.assertRaises(ClientError):
            storage.delete_directory()

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name=None)

        mock_session.client.assert_called_with("s3")

        mock_s3.list_objects.assert_called_once_with(Bucket="bucket", Prefix="some/dir/")
        mock_s3.delete_objects.assert_called_once()

    @mock.patch("boto3.session.Session", autospec=True)
    def test_get_download_url_calls_boto_generate_presigned_url_with_correct_data(
            self, mock_session_class: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        url = "s3://access_key:access_secret@some_bucket/"
        key = "some/file"
        mock_session.client.return_value.generate_presigned_url.return_value = "".join(
            ["http://fake.url/", key])

        storage = get_storage("".join([url, key, "?region=US_EAST"]))
        storage.get_download_url()

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name="US_EAST"
        )

        mock_session.client.return_value.generate_presigned_url.assert_called_with(
            "get_object",
            Params={"Bucket": "some_bucket", "Key": "some/file"},
            ExpiresIn=60
        )

    @mock.patch("boto3.session.Session", autospec=True)
    def test_get_download_url_calls_boto_generate_presigned_url_custom_expiration(
            self, mock_session_class: mock.Mock) -> None:
        mock_session = mock_session_class.return_value
        url = "s3://access_key:access_secret@some_bucket/"
        key = "some/file"
        mock_session.client.return_value.generate_presigned_url.return_value = "".join(
            ["http://fake.url/", key])

        storage = get_storage("".join([url, key, "?region=US_EAST"]))
        storage.get_download_url(seconds=1000)

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name="US_EAST"
        )

        mock_session.client.return_value.generate_presigned_url.assert_called_with(
            "get_object",
            Params={"Bucket": "some_bucket", "Key": "some/file"},
            ExpiresIn=1000
        )

    def test_get_sanitized_uri_returns_storage_uri_without_username_and_password(self) -> None:
        url = "s3://access_key:access_secret@some_bucket/"
        key = "some/filename"

        storage = get_storage("".join([url, key, "?region=US_EAST"]))
        sanitized_uri = storage.get_sanitized_uri()

        self.assertEqual("s3://some_bucket/some/filename?region=US_EAST", sanitized_uri)

    def test_s3_storage_rejects_multiple_query_values_for_region_setting(self) -> None:
        self.assert_rejects_multiple_query_values(
            "/foo/bar/object.mp4", "region", values=["US_EAST", "US_WEST"])
