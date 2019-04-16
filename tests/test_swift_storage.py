import os
from StringIO import StringIO
import tempfile
from unittest import TestCase

import mock

import storage as storagelib

from tests.helpers import create_temp_nested_directory_with_files


EXPECTED_CHUNK_SIZE = 32 * 1024 * 1024


class TestSwiftStorage(TestCase):

    class RackspaceObject:
        def __init__(self, name, content_type):
            self.name = name
            self.content_type = content_type

    def setUp(self):
        self.params = {
            "username": "user",
            "password": "password",
            "container": "container",
            "file": "file",
            "region": "region",
            "tenant_id": "1234567890",
            "auth_endpoint": "http://identity.server.com:1234/v2/",
            "api_key": "0987654321",
            "public": True,
            "download_url_key": "super_secret_key"
        }

    def _assert_login_correct(self, mock_create_context, mock_timeout, username=None, password=None,
                              region=None, public=True, tenant_id=None, api_key=None):
        mock_timeout.assert_called_with(storagelib.storage.DEFAULT_SWIFT_TIMEOUT, mock.ANY)
        mock_context = mock_create_context.return_value
        mock_create_context.assert_called_with(id_type="pyrax.base_identity.BaseIdentity",
                                               username=username, password=password,
                                               tenant_id=tenant_id, api_key=api_key)
        mock_context.authenticate.assert_called_with()
        mock_context.get_client.assert_called_with("swift", region, public=public)
        self.assertEqual(
            storagelib.storage.DEFAULT_SWIFT_TIMEOUT,
            mock_context.get_client.return_value.timeout)

    def _assert_default_login_correct(
            self, mock_create_context, mock_timeout, api_key=False, public=True):
        self._assert_login_correct(
            mock_create_context,
            mock_timeout,
            api_key=self.params["api_key"] if api_key is True else None,
            password=self.params["password"],
            public=public,
            region=self.params["region"],
            username=self.params["username"],
            tenant_id=self.params["tenant_id"]
        )

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_authenticates_with_full_uri(self, mock_timeout, mock_create_context):
        mock_context = mock_create_context.return_value
        mock_swift = mock_context.get_client.return_value

        temp_output = tempfile.NamedTemporaryFile()
        mock_swift.fetch_object.return_value = ["FOOBAR", ]

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s&api_key=%(api_key)s" \
              "&tenant_id=%(tenant_id)s&download_url_key=%(download_url_key)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.save_to_filename(temp_output.name)

        self._assert_default_login_correct(mock_create_context, mock_timeout, api_key=True)
        mock_swift.fetch_object.assert_called_with(
            self.params["container"], self.params["file"], chunk_size=EXPECTED_CHUNK_SIZE)

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_authenticates_with_partial_uri(self, mock_timeout, mock_create_context):
        mock_context = mock_create_context.return_value
        mock_swift = mock_context.get_client.return_value

        temp_output = tempfile.NamedTemporaryFile()
        mock_swift.fetch_object.return_value = ["FOOBAR", ]

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.save_to_filename(temp_output.name)

        self._assert_default_login_correct(mock_create_context, mock_timeout)
        mock_swift.fetch_object.assert_called_with(self.params["container"],
                                                   self.params["file"],
                                                   chunk_size=EXPECTED_CHUNK_SIZE)

    @mock.patch("pyrax.create_context")
    def test_swift_authenticate_fails_with_missing_params(self, mock_create_context):
        mock_context = mock_create_context.return_value
        mock_swift = mock_context.get_client.return_value

        temp_output = tempfile.NamedTemporaryFile()
        mock_swift.fetch_object.return_value = ["FOOBAR", ]

        # uri with missing auth_endpoint... should fail.
        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "region=%(region)s&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        with self.assertRaises(storagelib.storage.InvalidStorageUri):
            storage.save_to_filename(temp_output.name)

        # uri with missing region... should fail.
        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        with self.assertRaises(storagelib.storage.InvalidStorageUri):
            storage.save_to_filename(temp_output.name)

        # uri with missing tenant_id... should fail.
        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" % self.params
        storage = storagelib.get_storage(uri)
        with self.assertRaises(storagelib.storage.InvalidStorageUri):
            storage.save_to_filename(temp_output.name)

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_save_to_filename(self, mock_timeout, mock_create_context):
        mock_context = mock_create_context.return_value
        mock_swift = mock_context.get_client.return_value

        temp_output = tempfile.NamedTemporaryFile()
        mock_swift.fetch_object.return_value = ["FOOBAR", ]

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.save_to_filename(temp_output.name)

        self._assert_default_login_correct(mock_create_context, mock_timeout)
        mock_swift.fetch_object.assert_called_with(
            self.params["container"], self.params["file"], chunk_size=EXPECTED_CHUNK_SIZE)

        with open(temp_output.name) as output_fp:
            self.assertEqual("FOOBAR", output_fp.read())

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_save_to_file(self, mock_timeout, mock_create_context):
        mock_context = mock_create_context.return_value
        mock_swift = mock_context.get_client.return_value

        mock_swift.fetch_object.return_value = iter(["foo", "bar"])

        out_file = StringIO()

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.save_to_file(out_file)

        self._assert_default_login_correct(mock_create_context, mock_timeout)
        mock_swift.fetch_object.assert_called_with(
            self.params["container"], self.params["file"], chunk_size=EXPECTED_CHUNK_SIZE)

        self.assertEqual("foobar", out_file.getvalue())

    @mock.patch("os.path.exists", return_value=False)
    @mock.patch("os.makedirs")
    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_save_to_directory(
            self, mock_timeout, mock_create_context, mock_makedirs, mock_path_exists):

        expected_jpg = self.RackspaceObject("file/a/0.jpg", "image/jpg")
        expected_mp4 = self.RackspaceObject("file/a/b/c/1.mp4", "video/mp4")

        expected_files = [
            expected_jpg,
            expected_mp4
        ]
        mock_context = mock_create_context.return_value
        mock_swift = mock_context.get_client.return_value

        mock_swift.list_container_objects.return_value = expected_files

        uri = "swift://{username}:{password}@{container}/{file}?" \
              "auth_endpoint={auth_endpoint}&region={region}" \
              "&tenant_id={tenant_id}".format(**self.params)

        storagelib.get_storage(uri).save_to_directory("/tmp/cat/pants")

        self._assert_default_login_correct(mock_create_context, mock_timeout)
        mock_swift.list_container_objects.assert_called_with(
            self.params["container"], prefix=self.params["file"])

        mock_makedirs.assert_has_calls([
            mock.call("/tmp/cat/pants/a"), mock.call("/tmp/cat/pants/a/b/c")])

        mock_swift.download_object.assert_any_call(
            self.params["container"], expected_jpg, "/tmp/cat/pants/a", structure=False)
        mock_swift.download_object.assert_any_call(
            self.params["container"], expected_mp4, "/tmp/cat/pants/a/b/c", structure=False)

    @mock.patch("storage.retry.time.sleep", autospec=True)
    @mock.patch("storage.retry.random.uniform", autospec=True)
    @mock.patch("os.path.exists", return_value=False)
    @mock.patch("os.makedirs")
    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_save_to_directory_retries_failed_file_downloads(
            self, mock_timeout, mock_create_context, mock_makedirs, mock_path_exists, mock_uniform,
            mock_sleep):

        expected_jpg = self.RackspaceObject("file/a/0.jpg", "image/jpg")
        expected_mp4 = self.RackspaceObject("file/a/b/c/1.mp4", "video/mp4")

        expected_files = [
            expected_jpg,
            expected_mp4
        ]
        mock_context = mock_create_context.return_value
        mock_swift = mock_context.get_client.return_value

        mock_swift.list_container_objects.return_value = expected_files

        mock_swift.download_object.side_effect = [
            None,
            storagelib.storage.TimeoutError,
            None
        ]

        uri = "swift://{username}:{password}@{container}/{file}?" \
              "auth_endpoint={auth_endpoint}&region={region}" \
              "&tenant_id={tenant_id}".format(**self.params)

        storagelib.get_storage(uri).save_to_directory("/tmp/cat/pants")

        self._assert_default_login_correct(mock_create_context, mock_timeout)
        mock_swift.list_container_objects.assert_called_with(
            self.params["container"], prefix=self.params["file"])

        mock_makedirs.assert_has_calls([
            mock.call("/tmp/cat/pants/a"), mock.call("/tmp/cat/pants/a/b/c")])

        self.assertEqual(3, mock_swift.download_object.call_count)
        mock_swift.download_object.assert_has_calls([
            mock.call(self.params["container"], expected_jpg, "/tmp/cat/pants/a", structure=False),
            mock.call(
                self.params["container"], expected_mp4, "/tmp/cat/pants/a/b/c", structure=False),
            mock.call(
                self.params["container"], expected_mp4, "/tmp/cat/pants/a/b/c", structure=False)
        ])

        mock_uniform.assert_called_once_with(0, 1)

        mock_sleep.assert_called_once_with(mock_uniform.return_value)

    @mock.patch("storage.retry.time.sleep", autospec=True)
    @mock.patch("storage.retry.random.uniform", autospec=True)
    @mock.patch("os.path.exists", return_value=False)
    @mock.patch("os.makedirs")
    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_save_to_directory_fails_after_five_failed_file_download_retries(
            self, mock_timeout, mock_create_context, mock_makedirs, mock_path_exists, mock_uniform,
            mock_sleep):

        expected_jpg = self.RackspaceObject("file/a/0.jpg", "image/jpg")
        expected_mp4 = self.RackspaceObject("file/a/b/c/1.mp4", "video/mp4")

        expected_files = [
            expected_jpg,
            expected_mp4
        ]
        mock_context = mock_create_context.return_value
        mock_swift = mock_context.get_client.return_value

        mock_swift.list_container_objects.return_value = expected_files

        mock_swift.download_object.side_effect = [
            storagelib.storage.TimeoutError,
            IOError,
            IOError,
            IOError,
            RuntimeError,
        ]

        uri = "swift://{username}:{password}@{container}/{file}?" \
              "auth_endpoint={auth_endpoint}&region={region}" \
              "&tenant_id={tenant_id}".format(**self.params)

        with self.assertRaises(RuntimeError):
            storagelib.get_storage(uri).save_to_directory("/tmp/cat/pants")

        self._assert_default_login_correct(mock_create_context, mock_timeout)
        mock_swift.list_container_objects.assert_called_with(
            self.params["container"], prefix=self.params["file"])

        mock_makedirs.assert_called_once_with("/tmp/cat/pants/a")

        self.assertEqual(5, mock_swift.download_object.call_count)
        mock_swift.download_object.assert_has_calls([
            mock.call(self.params["container"], expected_jpg, "/tmp/cat/pants/a", structure=False),
            mock.call(self.params["container"], expected_jpg, "/tmp/cat/pants/a", structure=False),
            mock.call(self.params["container"], expected_jpg, "/tmp/cat/pants/a", structure=False),
            mock.call(self.params["container"], expected_jpg, "/tmp/cat/pants/a", structure=False),
            mock.call(self.params["container"], expected_jpg, "/tmp/cat/pants/a", structure=False)
        ])

        mock_uniform.assert_has_calls([
            mock.call(0, 1),
            mock.call(0, 3),
            mock.call(0, 7),
            mock.call(0, 15)
        ])

        self.assertEqual(4, mock_sleep.call_count)
        mock_sleep.assert_called_with(mock_uniform.return_value)

    @mock.patch("os.path.exists", return_value=False)
    @mock.patch("os.makedirs")
    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_save_to_directory_works_with_empty_directories(
            self, mock_timeout, mock_create_context, mock_makedirs, mock_path_exists):

        expected_jpg = self.RackspaceObject("a/0.jpg", "image/jpg")
        expected_mp4 = self.RackspaceObject("a/b/c/1.mp4", "video/mp4")

        expected_files = [
            self.RackspaceObject("nothing", "application/directory"),
            expected_jpg,
            self.RackspaceObject("to see", "application/directory"),
            self.RackspaceObject("here", "application/directory"),
            expected_mp4
        ]
        mock_context = mock_create_context.return_value
        mock_swift = mock_context.get_client.return_value

        mock_swift.list_container_objects.return_value = expected_files
        mock_path_exists.return_value = False

        uri = "swift://{username}:{password}@{container}/{file}?" \
              "auth_endpoint={auth_endpoint}&region={region}" \
              "&tenant_id={tenant_id}".format(**self.params)

        storagelib.get_storage(uri).save_to_directory("/tmp/cat/pants")

        self._assert_default_login_correct(mock_create_context, mock_timeout)
        mock_swift.list_container_objects.assert_called_with(
            self.params["container"], prefix=self.params["file"])

        mock_makedirs.assert_has_calls([
            mock.call("/tmp/cat/pants/nothing"),
            mock.call("/tmp/cat/pants/to see"),
            mock.call("/tmp/cat/pants/here"),
        ])

        mock_swift.download_object.assert_any_call(
            self.params["container"], expected_jpg, "a", structure=False)
        mock_swift.download_object.assert_any_call(
            self.params["container"], expected_mp4, "a/b/c", structure=False)

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_load_from_filename(self, mock_timeout, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write("FOOBAR")
        temp_input.flush()

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.load_from_filename(temp_input.name)

        self._assert_default_login_correct(mock_create_context, mock_timeout)
        mock_swift.upload_file.assert_called_with(
            self.params["container"], temp_input.name, self.params["file"])

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_load_from_filename_provides_content_type(
            self, mock_timeout, mock_create_context):
        self.params["file"] = "foobar.mp4"
        mock_swift = mock_create_context.return_value.get_client.return_value

        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write("FOOBAR")
        temp_input.flush()

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params

        storage = storagelib.get_storage(uri)
        storage.load_from_filename(temp_input.name)

        self._assert_default_login_correct(mock_create_context, mock_timeout)
        mock_swift.upload_file.assert_called_with(
            self.params["container"], temp_input.name, self.params["file"],
            content_type="video/mp4")

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_load_from_file(self, mock_timeout, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        mock_input = mock.Mock()

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.load_from_file(mock_input)

        self._assert_default_login_correct(mock_create_context, mock_timeout)
        mock_swift.upload_file.assert_called_with(
            self.params["container"], mock_input, self.params["file"])

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_load_from_directory(self, mock_timeout, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://{username}:{password}@{container}/{file}?" \
              "auth_endpoint={auth_endpoint}&region={region}" \
              "&tenant_id={tenant_id}".format(**self.params)

        storage = storagelib.get_storage(uri)

        temp_directory = create_temp_nested_directory_with_files()
        storage.load_from_directory(temp_directory["temp_directory"]["path"])

        self._assert_default_login_correct(mock_create_context, mock_timeout)

        mock_swift.upload_file.assert_has_calls([
            mock.call(
                self.params["container"], temp_directory["temp_input_two"]["path"],
                os.path.join(self.params["file"], temp_directory["temp_input_two"]["name"])),
            mock.call(
                self.params["container"], temp_directory["temp_input_one"]["path"],
                os.path.join(self.params["file"], temp_directory["temp_input_one"]["name"])),
            mock.call(
                self.params["container"], temp_directory["nested_temp_input"]["path"],
                os.path.join(
                    self.params["file"], temp_directory["nested_temp_directory"]["name"],
                    temp_directory["nested_temp_input"]["name"]))
        ], any_order=True)

    @mock.patch("storage.retry.time.sleep", autospec=True)
    @mock.patch("storage.retry.random.uniform", autospec=True)
    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_load_from_directory_retries_failed_file_uploads(
            self, mock_timeout, mock_create_context, mock_uniform, mock_sleep):
        mock_swift = mock_create_context.return_value.get_client.return_value

        mock_swift.upload_file.side_effect = [
            None,
            storagelib.storage.TimeoutError,
            None,
            None
        ]

        uri = "swift://{username}:{password}@{container}/{file}?" \
              "auth_endpoint={auth_endpoint}&region={region}" \
              "&tenant_id={tenant_id}".format(**self.params)

        storage = storagelib.get_storage(uri)

        temp_directory = create_temp_nested_directory_with_files()
        storage.load_from_directory(temp_directory["temp_directory"]["path"])

        self._assert_default_login_correct(mock_create_context, mock_timeout)

        self.assertEqual(4, mock_swift.upload_file.call_count)
        mock_swift.upload_file.assert_has_calls([
            mock.call(
                self.params["container"], temp_directory["temp_input_two"]["path"],
                os.path.join(self.params["file"], temp_directory["temp_input_two"]["name"])),
            mock.call(
                self.params["container"], temp_directory["temp_input_one"]["path"],
                os.path.join(self.params["file"], temp_directory["temp_input_one"]["name"])),
            mock.call(
                self.params["container"], temp_directory["nested_temp_input"]["path"],
                os.path.join(
                    self.params["file"], temp_directory["nested_temp_directory"]["name"],
                    temp_directory["nested_temp_input"]["name"]))
        ], any_order=True)
        self.assertEqual(
            mock_swift.upload_file.call_args_list[1], mock_swift.upload_file.call_args_list[2])

        mock_uniform.assert_called_once_with(0, 1)

        mock_sleep.assert_called_once_with(mock_uniform.return_value)

    @mock.patch("storage.retry.time.sleep", autospec=True)
    @mock.patch("storage.retry.random.uniform", autospec=True)
    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_load_from_directory_fails_after_five_failed_file_upload_retries(
            self, mock_timeout, mock_create_context, mock_uniform, mock_sleep):
        mock_swift = mock_create_context.return_value.get_client.return_value

        mock_swift.upload_file.side_effect = [
            storagelib.storage.TimeoutError,
            storagelib.storage.TimeoutError,
            IOError,
            IOError,
            IOError
        ]

        uri = "swift://{username}:{password}@{container}/{file}?" \
              "auth_endpoint={auth_endpoint}&region={region}" \
              "&tenant_id={tenant_id}".format(**self.params)

        storage = storagelib.get_storage(uri)

        temp_directory = create_temp_nested_directory_with_files()

        with self.assertRaises(IOError):
            storage.load_from_directory(temp_directory["temp_directory"]["path"])

        self._assert_default_login_correct(mock_create_context, mock_timeout)

        self.assertEqual(5, mock_swift.upload_file.call_count)
        self.assertEqual(
            mock_swift.upload_file.call_args, mock_swift.upload_file.call_args_list[0])
        self.assertEqual(
            mock_swift.upload_file.call_args, mock_swift.upload_file.call_args_list[1])
        self.assertEqual(
            mock_swift.upload_file.call_args, mock_swift.upload_file.call_args_list[2])
        self.assertEqual(
            mock_swift.upload_file.call_args, mock_swift.upload_file.call_args_list[3])

        mock_uniform.assert_has_calls([
            mock.call(0, 1),
            mock.call(0, 3),
            mock.call(0, 7),
            mock.call(0, 15)
        ])

        self.assertEqual(4, mock_sleep.call_count)
        mock_sleep.assert_called_with(mock_uniform.return_value)

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_delete(self, mock_timeout, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.delete()

        self._assert_default_login_correct(mock_create_context, mock_timeout)
        mock_swift.delete_object.assert_called_with(self.params["container"], self.params["file"])

    @mock.patch("pyrax.create_context")
    def test_swift_delete_directory(self, mock_create_context):

        expected_files = [
            self.RackspaceObject("file/a/0.txt", "text/plain"),
            self.RackspaceObject("file/a/b/1.mp4", "video/mp4"),
            self.RackspaceObject("file/a/b/c/2.mp4", "video/mp4")
        ]
        mock_swift = mock_create_context.return_value.get_client.return_value

        mock_swift.list_container_objects.return_value = expected_files

        uri = "swift://{username}:{password}@{container}/{file}?" \
              "auth_endpoint={auth_endpoint}&region={region}" \
              "&tenant_id={tenant_id}".format(**self.params)

        storage = storagelib.get_storage(uri)
        storage.delete_directory()

        mock_swift.delete_object.assert_has_calls([
            mock.call(self.params["container"], "file/a/b/c/2.mp4"),
            mock.call(self.params["container"], "file/a/b/1.mp4"),
            mock.call(self.params["container"], "file/a/0.txt")
        ], any_order=True)

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_uses_servicenet_when_requested(self, mock_timeout, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s&public=False" % self.params
        storage = storagelib.get_storage(uri)
        storage.delete()

        self._assert_default_login_correct(mock_create_context, mock_timeout, public=False)
        mock_swift.delete_object.assert_called_with(self.params["container"], self.params["file"])

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_get_download_url(self, mock_timeout, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s&download_url_key=%(download_url_key)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.get_download_url()

        self._assert_default_login_correct(mock_create_context, mock_timeout)
        mock_swift.get_temp_url.assert_called_with(
            self.params["container"], self.params["file"],
            seconds=60, method="GET", key="super_secret_key")

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_get_download_url_without_temp_url_key(self, mock_timeout, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.get_download_url()

        self._assert_default_login_correct(mock_create_context, mock_timeout)
        mock_swift.get_temp_url.assert_called_with(
            self.params["container"], self.params["file"], seconds=60, method="GET", key=None)

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_get_download_url_with_override(self, mock_timeout, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s&download_url_key=%(download_url_key)s" % self.params
        storage = storagelib.get_storage(uri)

        storage.get_download_url(key="NOT-THE-URI-KEY")

        self._assert_default_login_correct(mock_create_context, mock_timeout)
        mock_swift.get_temp_url.assert_called_with(
            self.params["container"], self.params["file"],
            seconds=60, method="GET", key="NOT-THE-URI-KEY")

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_swift_get_download_url_with_non_default_expiration(
            self, mock_timeout, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.get_download_url(seconds=10 * 60)

        self._assert_default_login_correct(mock_create_context, mock_timeout)
        mock_swift.get_temp_url.assert_called_with(
            self.params["container"], self.params["file"],
            seconds=600, method="GET", key=None)

    @mock.patch("pyrax.create_context")
    def test_swift_get_download_url_encodes_object_names_with_spaces(self, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        updated_params = self.params.copy()
        updated_params["file"] = "filename with spaces.txt"

        uri = "swift://{username}:{password}@{container}/{file}?" \
            "auth_endpoint={auth_endpoint}&region={region}" \
            "&tenant_id={tenant_id}".format(**updated_params)

        mock_swift.get_temp_url.return_value = "http://cloudfiles.com/path/to/{file}" \
            "?param1=12345&param2=67890".format(**updated_params)

        storage = storagelib.get_storage(uri)
        download_url = storage.get_download_url()

        mock_swift.get_temp_url.assert_called_with(
            updated_params["container"], updated_params["file"],
            seconds=60, method="GET", key=None)

        self.assertEqual(
            download_url,
            "http://cloudfiles.com/path/to/filename%20with%20spaces.txt?param1=12345&param2=67890")


class TestRegisterSwiftProtocol(TestCase):

    def setUp(self):
        self.scheme = "myscheme"
        self.auth_endpoint = "http://identity.server.com:1234/v2.0/"

    def test_register_swift_protocol_updates_storage_types(self):

        @storagelib.register_swift_protocol(scheme=self.scheme, auth_endpoint=self.auth_endpoint)
        class MyStorageClass(storagelib.swift_storage.SwiftStorage):
            pass

        self.assertIn(self.scheme, storagelib.storage._STORAGE_TYPES)

        uri = "{0}://username:password@containter/object?region=region-a".format(self.scheme)
        store_obj = storagelib.get_storage(uri)
        self.assertIsInstance(store_obj, MyStorageClass)

    @mock.patch("pyrax.create_context")
    def test_register_allows_override_of_auth_url(self, mock_create_context):
        @storagelib.register_swift_protocol(scheme=self.scheme, auth_endpoint=self.auth_endpoint)
        class MyStorageClass(storagelib.swift_storage.SwiftStorage):
            pass

        uri = "{0}://username:password@container/object?region=region-a&tenant_id=tennant".format(
            self.scheme)

        storage = storagelib.get_storage(uri)

        storage._authenticate()

        self.assertEqual(
            self.auth_endpoint, mock_create_context.return_value.auth_endpoint)

    def test_register_swift_protocol_only_wraps_swiftstorage_subclasses(self):

        # wrapped class must be an instance of SwiftStorage
        with self.assertRaises(Exception):
            @storagelib.register_swift_protocol(scheme=self.scheme,
                                                auth_endpoint=self.auth_endpoint)
            class NonStorageClass(object):
                pass


class TestRackspaceStorage(TestCase):
    def _assert_login_correct(
            self, mock_create_context, mock_timeout, username, password, region, public):
        mock_timeout.assert_called_with(storagelib.storage.DEFAULT_SWIFT_TIMEOUT, mock.ANY)
        mock_context = mock_create_context.return_value
        mock_create_context.assert_called_with("rackspace", username=username, password=password)
        mock_context.authenticate.assert_called_with()
        mock_context.get_client.assert_called_with("cloudfiles", region, public=public)
        self.assertEqual(
            storagelib.storage.DEFAULT_SWIFT_TIMEOUT,
            mock_context.get_client.return_value.timeout)

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_rackspace_authenticate_with_defaults(self, mock_timeout, mock_create_context):
        uri = "cloudfiles://{username}:{api_key}@{container}/{file}".format(
            username="username", api_key="apikey", container="container", file="file.txt")

        storage = storagelib.get_storage(uri)
        storage.delete()

        self._assert_login_correct(
            mock_create_context, mock_timeout, username="username", password="apikey", region="DFW",
            public=True)

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_rackspace_authenticate_with_public_false(self, mock_timeout, mock_create_context):
        uri = "cloudfiles://{username}:{api_key}@{container}/{file}?public=False".format(
            username="username", api_key="apikey", container="container", file="file.txt")

        storage = storagelib.get_storage(uri)
        storage.delete()

        self._assert_login_correct(
            mock_create_context, mock_timeout, username="username", password="apikey", region="DFW",
            public=False)

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_rackspace_authenticate_with_region(self, mock_timeout, mock_create_context):
        uri = "cloudfiles://{username}:{api_key}@{container}/{file}?region=ORD".format(
            username="username", api_key="apikey", container="container", file="file.txt")

        storage = storagelib.get_storage(uri)
        storage.delete()

        self._assert_login_correct(
            mock_create_context, mock_timeout, username="username", password="apikey", region="ORD",
            public=True)

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_rackspace_uses_download_url_key_when_provided_in_url(
            self, mock_timeout, mock_create_context):
        uri = "cloudfiles://{username}:{api_key}@{container}/{file}?download_url_key={key}".format(
            username="username", api_key="apikey", container="container", file="file.txt",
            key="super_secret_key")

        mock_cloudfiles = mock_create_context.return_value.get_client.return_value

        storage = storagelib.get_storage(uri)
        storage.get_download_url()

        self._assert_login_correct(
            mock_create_context, mock_timeout, username="username", password="apikey", region="DFW",
            public=True)

        mock_cloudfiles.get_temp_url.assert_called_once_with(
            "container", "file.txt", seconds=60, method="GET", key="super_secret_key")

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_rackspace_uses_download_url_key_when_provided_call(
            self, mock_timeout, mock_create_context):
        uri = "cloudfiles://{username}:{api_key}@{container}/{file}?download_url_key={key}".format(
            username="username", api_key="apikey", container="container", file="file.txt",
            key="super_secret_key")

        mock_cloudfiles = mock_create_context.return_value.get_client.return_value

        storage = storagelib.get_storage(uri)
        storage.get_download_url(key="other_key")

        self._assert_login_correct(
            mock_create_context, mock_timeout, username="username", password="apikey", region="DFW",
            public=True)

        mock_cloudfiles.get_temp_url.assert_called_once_with(
            "container", "file.txt", seconds=60, method="GET", key="other_key")

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_rackspace_fetches_download_url_key_when_not_provided(
            self, mock_timeout, mock_create_context):
        uri = "cloudfiles://{username}:{api_key}@{container}/{file}".format(
            username="username", api_key="apikey", container="container", file="file.txt")

        mock_cloudfiles = mock_create_context.return_value.get_client.return_value

        mock_cloudfiles.get_account_metadata.return_value = {
            "some_metadata": "values",
            "temp_url_key": "secret_key_from_server"
        }

        storage = storagelib.get_storage(uri)
        storage.get_download_url()

        self._assert_login_correct(
            mock_create_context, mock_timeout, username="username", password="apikey", region="DFW",
            public=True)

        mock_cloudfiles.get_temp_url.assert_called_once_with(
            "container", "file.txt", seconds=60, method="GET", key="secret_key_from_server")

    @mock.patch("pyrax.create_context")
    @mock.patch("storage.swift_storage.timeout", wraps=storagelib.swift_storage.timeout)
    def test_rackspace_ignores_case_of_metadata_when_fetching_download_url_key(
            self, mock_timeout, mock_create_context):
        uri = "cloudfiles://{username}:{api_key}@{container}/{file}".format(
            username="username", api_key="apikey", container="container", file="file.txt")

        mock_cloudfiles = mock_create_context.return_value.get_client.return_value

        mock_cloudfiles.get_account_metadata.return_value = {
            "some_metadata": "values",
            "Temp_Url_Key": "secret_key_from_server"
        }

        storage = storagelib.get_storage(uri)
        storage.get_download_url()

        self._assert_login_correct(
            mock_create_context, mock_timeout, username="username", password="apikey", region="DFW",
            public=True)

        mock_cloudfiles.get_temp_url.assert_called_once_with(
            "container", "file.txt", seconds=60, method="GET", key="secret_key_from_server")
