import json
import os
from unittest import mock, TestCase
from urllib.parse import quote

from typing import cast, Optional
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

        config_class_patcher = mock.patch("botocore.config.Config", autospec=True)
        self.mock_config_class = config_class_patcher.start()
        self.addCleanup(config_class_patcher.stop)
        self.mock_config = self.mock_config_class.return_value

        session_class_patcher = mock.patch("boto3.session.Session", autospec=True)
        self.mock_session_class = session_class_patcher.start()
        self.addCleanup(session_class_patcher.stop)
        self.mock_session = self.mock_session_class.return_value

    def tearDown(self) -> None:
        super().tearDown()
        if self.temp_directory is not None:
            cleanup_nested_directory(self.temp_directory)

    def _generate_storage_uri(
            self, object_path: str, parameters: Optional[dict[str, str]] = None) -> str:
        return "s3://access_key:access_secret@bucket/some/file"

    def create_json_credentials(
        self,
        key_id: Optional[str],
        access_secret: Optional[str],
        *,
        version: Optional[int] = 1,
        role: Optional[str] = None,
        role_session_name: Optional[str] = None,
        external_id: Optional[str] = None
    ) -> str:
        credentials: dict[str, object] = {}

        if version is not None:
            credentials["version"] = version

        if key_id is not None:
            credentials["key_id"] = key_id

        if access_secret is not None:
            credentials["access_secret"] = access_secret

        if role is not None:
            credentials["role"] = role

        if role_session_name is not None:
            credentials["role_session_name"] = role_session_name

        if external_id is not None:
            credentials["external_id"] = external_id

        return quote(json.dumps(credentials, separators=(",", ":")), safe="")

    def test_requires_username_in_uri(self) -> None:
        with self.assertRaises(InvalidStorageUri):
            get_storage("s3://hostname/path")

    def test_requires_hostname_in_uri(self) -> None:
        with self.assertRaises(InvalidStorageUri):
            get_storage("s3://username:password@/path")

    def test_s3storage_init_sets_correct_keyname(self) -> None:
        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        self.assertEqual("some/file", cast(S3Storage, storage)._keyname)

    def test_handles_urlencoded_keys(self) -> None:
        encoded_key = quote("access/key", safe="")
        encoded_secret = quote("access/secret", safe="")

        storage = get_storage(
            "s3://{0}:{1}@bucket/some/file?region=US_EAST".format(encoded_key, encoded_secret))

        cast(S3Storage, storage)._connect()

        self.mock_session_class.assert_called_with(
            aws_access_key_id="access/key",
            aws_secret_access_key="access/secret",
            aws_session_token=None,
            region_name="US_EAST")

    def test_accepts_json_encoded_credentials_in_username(self) -> None:
        credentials = self.create_json_credentials("ACCESS/KEY", "ACCESS/SECRET")

        storage = get_storage(f"s3://{credentials}@bucket/some/file")

        cast(S3Storage, storage)._connect()

        self.mock_session_class.assert_called_with(
            aws_access_key_id="ACCESS/KEY",
            aws_secret_access_key="ACCESS/SECRET",
            aws_session_token=None,
            region_name=None)

    def test_requires_json_encoded_credentials_to_have_version(self) -> None:
        credentials = self.create_json_credentials("ACCESS-KEY", "ACCESS-SECRET", version=None)

        storage = get_storage(f"s3://{credentials}@bucket/some/file")

        with self.assertRaises(InvalidStorageUri):
            cast(S3Storage, storage)._connect()

    def test_requires_json_encoded_credentials_version_to_be_1(self) -> None:
        credentials = self.create_json_credentials("ACCESS-KEY", "ACCESS-SECRET", version=42)

        storage = get_storage(f"s3://{credentials}@bucket/some/file")

        with self.assertRaises(InvalidStorageUri):
            cast(S3Storage, storage)._connect()

    def test_requires_json_encoded_credentials_to_have_key_id(self) -> None:
        credentials = self.create_json_credentials(None, "ACCESS-SECRET")

        storage = get_storage(f"s3://{credentials}@bucket/some/file")

        with self.assertRaises(InvalidStorageUri):
            cast(S3Storage, storage)._connect()

    def test_requires_json_encoded_credentials_to_have_access_secret(self) -> None:
        credentials = self.create_json_credentials("ACCESS-KEY", None)

        storage = get_storage(f"s3://{credentials}@bucket/some/file")

        with self.assertRaises(InvalidStorageUri):
            cast(S3Storage, storage)._connect()

    @mock.patch("boto3.client", autospec=True)
    def test_assumes_role_when_json_encoded_credentials_contains_role(
        self,
        mock_boto3_client: mock.Mock
    ) -> None:
        mock_sts_client = mock_boto3_client.return_value
        mock_sts_client.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "ASSUMED-KEY-ID",
                "SecretAccessKey": "ASSUMED-SECRET-KEY",
                "SessionToken": "ASSUMED-SESSION-TOKEN"
            }
        }

        credentials = self.create_json_credentials(
            "ACCESS-KEY", "ACCESS-SECRET", role="ROLE", role_session_name="ROLE-SESSION-NAME")

        storage = get_storage(f"s3://{credentials}@bucket/some/file")

        cast(S3Storage, storage)._connect()

        mock_boto3_client.assert_called_once_with(
            "sts", aws_access_key_id="ACCESS-KEY", aws_secret_access_key="ACCESS-SECRET")

        mock_sts_client.assume_role.assert_called_once_with(
            RoleArn="ROLE", RoleSessionName="ROLE-SESSION-NAME", ExternalId=None)

        self.mock_session_class.assert_called_with(
            aws_access_key_id="ASSUMED-KEY-ID",
            aws_secret_access_key="ASSUMED-SECRET-KEY",
            aws_session_token="ASSUMED-SESSION-TOKEN",
            region_name=None)

    @mock.patch("boto3.client", autospec=True)
    def test_requires_role_session_name_when_json_encoded_credentials_contains_role(
        self,
        mock_boto3_client: mock.Mock
    ) -> None:
        mock_sts_client = mock_boto3_client.return_value
        mock_sts_client.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "ASSUMED-KEY-ID",
                "SecretAccessKey": "ASSUMED-SECRET-KEY",
                "SessionToken": "ASSUMED-SESSION-TOKEN"
            }
        }

        credentials = self.create_json_credentials(
            "ACCESS-KEY", "ACCESS-SECRET", role="ROLE", role_session_name=None)

        storage = get_storage(f"s3://{credentials}@bucket/some/file")

        with self.assertRaises(InvalidStorageUri):
            cast(S3Storage, storage)._connect()

    @mock.patch("boto3.client", autospec=True)
    def test_includes_external_id_when_assuming_role_if_provided_in_credentials(
        self,
        mock_boto3_client: mock.Mock
    ) -> None:
        mock_sts_client = mock_boto3_client.return_value
        mock_sts_client.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "ASSUMED-KEY-ID",
                "SecretAccessKey": "ASSUMED-SECRET-KEY",
                "SessionToken": "ASSUMED-SESSION-TOKEN"
            }
        }

        credentials = self.create_json_credentials(
            "ACCESS-KEY", "ACCESS-SECRET", role="ROLE", role_session_name="ROLE-SESSION-NAME",
            external_id="EXTERNAL-ID")

        storage = get_storage(f"s3://{credentials}@bucket/some/file")

        cast(S3Storage, storage)._connect()

        mock_sts_client.assume_role.assert_called_once_with(
            RoleArn="ROLE", RoleSessionName="ROLE-SESSION-NAME", ExternalId="EXTERNAL-ID")

    def test_load_from_file(self) -> None:
        mock_s3 = self.mock_session.client.return_value

        mock_file = mock.Mock()

        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        storage.load_from_file(mock_file)

        self.mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            aws_session_token=None,
            region_name="US_EAST")

        self.mock_config_class.assert_called_with(signature_version="v4")
        self.mock_session.client.assert_called_with("s3", config=self.mock_config)

        mock_s3.put_object.assert_called_with(Bucket="bucket", Key="some/file", Body=mock_file)

    def test_load_from_file_guesses_content_type_based_on_filename(self) -> None:
        mock_s3 = self.mock_session.client.return_value

        mock_file = mock.Mock()

        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/whatever.jpg")

        storage.load_from_file(mock_file)

        mock_s3.put_object.assert_called_with(
            Bucket="bucket", Key="some/whatever.jpg", Body=mock_file, ContentType="image/jpeg")

    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    def test_load_from_filename(self, mock_transfer_class: mock.Mock) -> None:
        mock_s3 = self.mock_session.client.return_value
        mock_transfer = mock_transfer_class.return_value

        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        storage.load_from_filename("source/file")

        self.mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            aws_session_token=None,
            region_name="US_EAST")

        self.mock_config_class.assert_called_with(signature_version="v4")
        self.mock_session.client.assert_called_with("s3", config=self.mock_config)

        mock_transfer_class.assert_called_with(mock_s3)

        mock_transfer.upload_file.assert_called_with(
            "source/file", "bucket", "some/file", extra_args=None)

    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    def test_load_from_filename_guesses_content_type_based_on_filename(
        self,
        mock_transfer_class: mock.Mock
    ) -> None:
        mock_transfer = mock_transfer_class.return_value

        storage = get_storage("s3://access_key:access_secret@bucket/some/file")

        storage.load_from_filename("source/whatever.jpeg")

        mock_transfer.upload_file.assert_called_with(
            "source/whatever.jpeg", "bucket", "some/file", extra_args={"ContentType": "image/jpeg"})

    def test_save_to_file(self) -> None:
        mock_s3 = self.mock_session.client.return_value

        mock_body = mock.Mock()
        mock_body.read.side_effect = [b"some", b"file", b"contents", None]
        mock_s3.get_object.return_value = {
            "Body": mock_body
        }

        mock_file = mock.Mock()

        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")
        storage.save_to_file(mock_file)

        self.mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            aws_session_token=None,
            region_name="US_EAST")

        self.mock_config_class.assert_called_with(signature_version="v4")
        self.mock_session.client.assert_called_with("s3", config=self.mock_config)
        mock_s3.get_object.assert_called_with(Bucket="bucket", Key="some/file")
        mock_file.write.assert_has_calls([
            mock.call(b"some"),
            mock.call(b"file"),
            mock.call(b"contents")
        ], any_order=False)

    def test_save_to_file_raises_when_file_does_not_exist(self) -> None:
        mock_s3 = self.mock_session.client.return_value
        mock_s3.get_object.return_value = {}
        mock_file = mock.Mock()

        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        with self.assertRaises(NotFoundError):
            storage.save_to_file(mock_file)

        self.mock_config_class.assert_called_with(signature_version="v4")
        self.mock_session.client.assert_called_with("s3", config=self.mock_config)
        mock_s3.get_object.assert_called_with(Bucket="bucket", Key="some/file")
        mock_file.write.assert_not_called()

    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    def test_save_to_filename(self, mock_transfer_class: mock.Mock) -> None:
        mock_s3 = self.mock_session.client.return_value

        mock_transfer = mock_transfer_class.return_value

        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        storage.save_to_filename("destination/file")

        self.mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            aws_session_token=None,
            region_name="US_EAST")

        self.mock_config_class.assert_called_with(signature_version="v4")
        self.mock_session.client.assert_called_with("s3", config=self.mock_config)

        mock_transfer_class.assert_called_with(mock_s3)
        mock_transfer.download_file.assert_called_with("bucket", "some/file", "destination/file")

    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    def test_save_to_filename_raises_not_found_error_when_file_does_not_exist(
        self,
        mock_transfer_class: mock.Mock
    ) -> None:
        mock_s3 = self.mock_session.client.return_value

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
    def test_save_to_filename_raises_original_exception_when_not_404(
        self,
        mock_transfer_class: mock.Mock
    ) -> None:
        mock_s3 = self.mock_session.client.return_value

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
    def test_save_to_directory(
        self,
        mock_transfer_class: mock.Mock,
        mock_path_exists: mock.Mock,
        mock_makedirs: mock.Mock
    ) -> None:
        mock_s3_client = self.mock_session.client.return_value
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
        path_mock_path_exists_calls: list[str] = []

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

        self.mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            aws_session_token=None,
            region_name="US_EAST")

        mock_s3_client.list_objects.assert_called_with(Bucket="bucket", Prefix="directory/")
        mock_makedirs.assert_has_calls(
            [mock.call("save_to_directory/b"), mock.call("save_to_directory/e/f")])
        self.mock_config_class.assert_called_with(signature_version="v4")
        self.mock_session.client.assert_called_with("s3", config=self.mock_config)

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
    def test_save_to_directory_retries_failed_file_uploads(
        self,
        mock_transfer_class: mock.Mock,
        mock_path_exists: mock.Mock,
        mock_makedirs: mock.Mock,
        mock_uniform: mock.Mock,
        mock_sleep: mock.Mock
    ) -> None:
        mock_s3_client = self.mock_session.client.return_value
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
        path_mock_path_exists_calls: list[str] = []

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

        self.mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            aws_session_token=None,
            region_name="US_EAST")

        mock_s3_client.list_objects.assert_called_with(Bucket="bucket", Prefix="directory/")
        mock_makedirs.assert_has_calls(
            [mock.call("save_to_directory/b"), mock.call("save_to_directory/e/f")])
        self.mock_config_class.assert_called_with(signature_version="v4")
        self.mock_session.client.assert_called_with("s3", config=self.mock_config)

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
    def test_save_to_directory_fails_after_five_failed_file_download_retries(
        self,
        mock_transfer_class: mock.Mock,
        mock_path_exists: mock.Mock,
        mock_makedirs: mock.Mock,
        mock_uniform: mock.Mock,
        mock_sleep: mock.Mock
    ) -> None:
        mock_s3_client = self.mock_session.client.return_value
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
        path_mock_path_exists_calls: list[str] = []

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

        self.mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            aws_session_token=None,
            region_name="US_EAST")

        mock_s3_client.list_objects.assert_called_with(Bucket="bucket", Prefix="directory/")
        mock_makedirs.assert_called_once_with("save_to_directory/b")
        self.mock_config_class.assert_called_with(signature_version="v4")
        self.mock_session.client.assert_called_with("s3", config=self.mock_config)

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
    def test_save_to_directory_raises_when_empty(
        self,
        mock_transfer_class: mock.Mock,
        mock_path_exists: mock.Mock,
        mock_makedirs: mock.Mock
    ) -> None:
        mock_s3_client = self.mock_session.client.return_value
        mock_s3_client.list_objects.return_value = {}

        storage = get_storage(
            "s3://access_key:access_secret@bucket/directory?region=US_EAST")

        with self.assertRaises(NotFoundError):
            storage.save_to_directory("save_to_directory")

        self.mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            aws_session_token=None,
            region_name="US_EAST")

        self.mock_config_class.assert_called_with(signature_version="v4")
        self.mock_session.client.assert_called_with("s3", config=self.mock_config)

        mock_s3_client.list_objects.assert_called_with(Bucket="bucket", Prefix="directory/")

    @mock.patch("storage.retry.time.sleep", autospec=True)
    @mock.patch("storage.retry.random.uniform", autospec=True)
    @mock.patch("os.makedirs")
    @mock.patch("os.path.exists")
    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    def test_save_to_directory_raises_not_found_error_when_file_does_not_exist(
        self,
        mock_transfer_class: mock.Mock,
        mock_path_exists: mock.Mock,
        mock_makedirs: mock.Mock,
        mock_uniform: mock.Mock,
        mock_sleep: mock.Mock
    ) -> None:
        mock_s3_client = self.mock_session.client.return_value
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
        path_mock_path_exists_calls: list[str] = []

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
    def test_save_to_directory_raises_original_exception_when_not_404(
        self,
        mock_transfer_class: mock.Mock,
        mock_path_exists: mock.Mock,
        mock_makedirs: mock.Mock,
        mock_uniform: mock.Mock,
        mock_sleep: mock.Mock
    ) -> None:
        mock_s3_client = self.mock_session.client.return_value
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
        path_mock_path_exists_calls: list[str] = []

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
    def test_load_from_directory(self, mock_transfer_class: mock.Mock) -> None:
        mock_s3_client = self.mock_session.client.return_value

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
    def test_load_from_directory_retries_failed_file_uploads(
        self,
        mock_transfer_class: mock.Mock,
        mock_uniform: mock.Mock,
        mock_sleep: mock.Mock
    ) -> None:
        mock_s3_client = self.mock_session.client.return_value

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
    def test_load_from_directory_fails_after_five_failed_file_upload_retries(
        self,
        mock_transfer_class: mock.Mock,
        mock_uniform: mock.Mock,
        mock_sleep: mock.Mock
    ) -> None:
        mock_s3_client = self.mock_session.client.return_value

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

    def test_delete(self) -> None:
        mock_s3 = self.mock_session.client.return_value
        mock_s3.delete_object.return_value = {
            "DeleteMarker": True
        }

        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        storage.delete()

        self.mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            aws_session_token=None,
            region_name="US_EAST")

        self.mock_config_class.assert_called_with(signature_version="v4")
        self.mock_session.client.assert_called_with("s3", config=self.mock_config)

        mock_s3.delete_object.assert_called_with(Bucket="bucket", Key="some/file")

    def test_delete_raises_when_file_does_not_exist(self) -> None:
        mock_s3 = self.mock_session.client.return_value
        mock_s3.delete_object.return_value = {}

        storage = get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        with self.assertRaises(NotFoundError):
            storage.delete()

        mock_s3.delete_object.assert_called_with(Bucket="bucket", Key="some/file")

    def test_delete_directory(self) -> None:
        mock_s3 = self.mock_session.client.return_value

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

        self.mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            aws_session_token=None,
            region_name=None)

        self.mock_config_class.assert_called_with(signature_version="v4")
        self.mock_session.client.assert_called_with("s3", config=self.mock_config)

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

    def test_delete_directory_raises_when_empty(self) -> None:
        mock_s3 = self.mock_session.client.return_value

        mock_s3.list_objects.return_value = {}

        storage = get_storage("s3://access_key:access_secret@bucket/some/dir")

        with self.assertRaises(NotFoundError):
            storage.delete_directory()

        self.mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            aws_session_token=None,
            region_name=None)

        self.mock_config_class.assert_called_with(signature_version="v4")
        self.mock_session.client.assert_called_with("s3", config=self.mock_config)

        mock_s3.list_objects.assert_called_once_with(Bucket="bucket", Prefix="some/dir/")
        mock_s3.delete_objects.assert_not_called()

    def test_delete_directory_raises_not_found_error_when_files_do_not_exist(self) -> None:
        mock_s3 = self.mock_session.client.return_value

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

        self.mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            aws_session_token=None,
            region_name=None)

        self.mock_config_class.assert_called_with(signature_version="v4")
        self.mock_session.client.assert_called_with("s3", config=self.mock_config)

        mock_s3.list_objects.assert_called_once_with(Bucket="bucket", Prefix="some/dir/")
        mock_s3.delete_objects.assert_called_once()

    def test_delete_directory_raises_original_exception_when_not_404(self) -> None:
        mock_s3 = self.mock_session.client.return_value

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

        self.mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            aws_session_token=None,
            region_name=None)

        self.mock_config_class.assert_called_with(signature_version="v4")
        self.mock_session.client.assert_called_with("s3", config=self.mock_config)

        mock_s3.list_objects.assert_called_once_with(Bucket="bucket", Prefix="some/dir/")
        mock_s3.delete_objects.assert_called_once()

    def test_get_download_url_calls_boto_generate_presigned_url_with_correct_data(self) -> None:
        url = "s3://access_key:access_secret@some_bucket/"
        key = "some/file"
        self.mock_session.client.return_value.generate_presigned_url.return_value = "".join(
            ["http://fake.url/", key])

        storage = get_storage("".join([url, key, "?region=US_EAST"]))
        storage.get_download_url()

        self.mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            aws_session_token=None,
            region_name="US_EAST"
        )

        self.mock_session.client.return_value.generate_presigned_url.assert_called_with(
            "get_object",
            Params={"Bucket": "some_bucket", "Key": "some/file"},
            ExpiresIn=60
        )

    def test_get_download_url_calls_boto_generate_presigned_url_custom_expiration(self) -> None:
        url = "s3://access_key:access_secret@some_bucket/"
        key = "some/file"
        self.mock_session.client.return_value.generate_presigned_url.return_value = "".join(
            ["http://fake.url/", key])

        storage = get_storage("".join([url, key, "?region=US_EAST"]))
        storage.get_download_url(seconds=1000)

        self.mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            aws_session_token=None,
            region_name="US_EAST"
        )

        self.mock_session.client.return_value.generate_presigned_url.assert_called_with(
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
