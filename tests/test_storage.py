import mock
import os.path
import storage as storagelib
from storage.storage import DownloadUrlBaseUndefinedError
import tempfile
import urllib
from unittest import TestCase
from StringIO import StringIO


EXPECTED_CHUNK_SIZE = 32 * 1024 * 1024


def create_temp_nested_directory_with_files():
    # temp_directory/
    #   temp_input_one
    #   temp_input_two
    #   nested_temp_directory/
    #      nested_temp_input
    temp_dir = {}
    temp_dir["temp_directory"] = {"path": tempfile.mkdtemp()}
    temp_dir["temp_input_one"] = {
        "file": tempfile.NamedTemporaryFile(dir=temp_dir["temp_directory"]["path"])}
    temp_dir["temp_input_one"]["path"] = temp_dir["temp_input_one"]["file"].name
    temp_dir["temp_input_one"]["name"] = os.path.basename(temp_dir["temp_input_one"]["file"].name)

    temp_dir["temp_input_one"]["file"].write("FOO")
    temp_dir["temp_input_one"]["file"].flush()

    temp_dir["temp_input_two"] = {
        "file": tempfile.NamedTemporaryFile(dir=temp_dir["temp_directory"]["path"])}
    temp_dir["temp_input_two"]["path"] = temp_dir["temp_input_two"]["file"].name
    temp_dir["temp_input_two"]["name"] = os.path.basename(temp_dir["temp_input_two"]["file"].name)
    temp_dir["temp_input_two"]["file"].write("BAR")
    temp_dir["temp_input_two"]["file"].flush()

    temp_dir["nested_temp_directory"] = {
        "path": tempfile.mkdtemp(dir=temp_dir["temp_directory"]["path"])}
    temp_dir["nested_temp_directory"]["name"] = os.path.basename(
        temp_dir["nested_temp_directory"]["path"])

    temp_dir["nested_temp_input"] = {
        "file": tempfile.NamedTemporaryFile(dir=temp_dir["nested_temp_directory"]["path"])}
    temp_dir["nested_temp_input"]["path"] = temp_dir["nested_temp_input"]["file"].name
    temp_dir["nested_temp_input"]["name"] = os.path.basename(
        temp_dir["nested_temp_input"]["file"].name)

    temp_dir["nested_temp_input"]["file"].write("FOOBAR")
    temp_dir["nested_temp_input"]["file"].flush()

    return temp_dir


def create_mock_ftp_directory_listing(directory_listings):
    def mock_directory_listing(_, callback):
        if len(directory_listings):
            listing = directory_listings.pop(0)

            if isinstance(listing, basestring):
                callback(listing)
            else:
                map(callback, listing)

    return mock_directory_listing


class TestRegisterStorageProtocol(TestCase):

    def setUp(self):
        self.scheme = "myscheme"

    def test_register_storage_protocol_updates_storage_types(self):

        @storagelib.register_storage_protocol(scheme=self.scheme)
        class MyStorageClass(storagelib.storage.Storage):
            pass

        self.assertIn(self.scheme, storagelib.storage._STORAGE_TYPES)

        uri = "{0}://some/uri/path".format(self.scheme)
        store_obj = storagelib.get_storage(uri)
        self.assertIsInstance(store_obj, MyStorageClass)


class TestLocalStorage(TestCase):

    def test_local_storage_save_to_filename(self):
        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write("FOOBAR")
        temp_input.flush()

        temp_output = tempfile.NamedTemporaryFile()

        storage = storagelib.get_storage("file://%s" % (temp_input.name))
        storage.save_to_filename(temp_output.name)

        with open(temp_output.name) as temp_output_fp:
            self.assertEqual("FOOBAR", temp_output_fp.read())

    @mock.patch("os.makedirs", autospec=True)
    def test_local_storage_load_from_filename(self, mock_makedirs):
        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write("FOOBAR")
        temp_input.flush()

        temp_output = tempfile.NamedTemporaryFile()
        storage = storagelib.get_storage("file://%s" % (temp_output.name))
        storage.load_from_filename(temp_input.name)

        self.assertEqual(0, mock_makedirs.call_count)

        with open(temp_output.name) as temp_output_fp:
            self.assertEqual("FOOBAR", temp_output_fp.read())

    def test_local_storage_save_to_directory(self):
        temp_directory = create_temp_nested_directory_with_files()

        storage = storagelib.get_storage(
            "file://{0}".format(temp_directory["temp_directory"]["path"]))

        temp_output_dir = tempfile.mkdtemp()
        destination_directory_path = os.path.join(temp_output_dir, "tmp")
        storage.save_to_directory(destination_directory_path)

        destination_input_one_path = os.path.join(
            temp_output_dir, destination_directory_path, temp_directory["temp_input_one"]["name"])
        destination_input_two_path = os.path.join(
            temp_output_dir, destination_directory_path, temp_directory["temp_input_two"]["name"])
        nested_temp_input_path = os.path.join(
            temp_output_dir, destination_directory_path,
            temp_directory["nested_temp_directory"]["name"],
            temp_directory["nested_temp_input"]["name"])

        self.assertTrue(os.path.exists(destination_input_one_path))
        self.assertTrue(os.path.exists(destination_input_two_path))
        self.assertTrue(os.path.exists(nested_temp_input_path))

        with open(destination_input_one_path) as temp_output_fp:
            self.assertEqual("FOO", temp_output_fp.read())

        with open(destination_input_two_path) as temp_output_fp:
            self.assertEqual("BAR", temp_output_fp.read())

        with open(nested_temp_input_path) as temp_output_fp:
            self.assertEqual("FOOBAR", temp_output_fp.read())

    def test_local_storage_load_from_directory(self):
        temp_directory = create_temp_nested_directory_with_files()

        temp_output_dir = tempfile.mkdtemp()
        storage = storagelib.get_storage("file://{0}/{1}".format(temp_output_dir, "tmp"))

        storage.load_from_directory(temp_directory["temp_directory"]["path"])

        destination_directory_path = os.path.join(
            temp_output_dir, "tmp")
        destination_input_one_path = os.path.join(
            temp_output_dir, destination_directory_path, temp_directory["temp_input_one"]["name"])
        destination_input_two_path = os.path.join(
            temp_output_dir, destination_directory_path, temp_directory["temp_input_two"]["name"])
        nested_temp_input_path = os.path.join(
            temp_output_dir, destination_directory_path,
            temp_directory["nested_temp_directory"]["path"],
            temp_directory["nested_temp_input"]["name"])

        self.assertTrue(os.path.exists(destination_input_one_path))
        self.assertTrue(os.path.exists(destination_input_two_path))
        self.assertTrue(os.path.exists(nested_temp_input_path))

        with open(destination_input_one_path) as temp_output_fp:
            self.assertEqual("FOO", temp_output_fp.read())

        with open(destination_input_two_path) as temp_output_fp:
            self.assertEqual("BAR", temp_output_fp.read())

        with open(nested_temp_input_path) as temp_output_fp:
            self.assertEqual("FOOBAR", temp_output_fp.read())

    @mock.patch("shutil.copy", autospec=True)
    @mock.patch("os.makedirs", autospec=True)
    @mock.patch("os.path.exists", autospec=True)
    def test_load_from_file_creates_intermediate_dirs(self, mock_exists, mock_makedirs, mock_copy):
        mock_exists.return_value = False

        storage = storagelib.get_storage("file:///foo/bar/file")
        storage.load_from_filename("input_file")

        mock_exists.assert_called_with("/foo/bar")
        mock_makedirs.assert_called_with("/foo/bar")
        mock_copy.assert_called_with("input_file", "/foo/bar/file")

    @mock.patch("os.remove", autospec=True)
    def test_local_storage_delete(self, mock_remove):
        storage = storagelib.get_storage("file:///folder/file")
        storage.delete()

        mock_remove.assert_called_with("/folder/file")

    @mock.patch("shutil.rmtree", autospec=True)
    @mock.patch("os.remove", autospec=True)
    def test_local_storage_delete_directory(self, mock_remove, mock_rmtree):
        temp_directory = create_temp_nested_directory_with_files()

        storage = storagelib.get_storage(
            "file://{0}".format(temp_directory["temp_directory"]["path"]))
        storage.delete_directory()

        self.assertFalse(mock_remove.called)
        mock_rmtree.assert_called_once_with(temp_directory["temp_directory"]["path"], True)

    def test_local_storage_save_to_file(self):
        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write("FOOBAR")
        temp_input.flush()

        out_file = StringIO()

        storage = storagelib.get_storage("file://%s" % (temp_input.name))
        storage.save_to_file(out_file)

        self.assertEqual("FOOBAR", out_file.getvalue())

    def test_local_storage_load_from_file(self):
        in_file = StringIO("foobar")
        temp_output = tempfile.NamedTemporaryFile()

        storage = storagelib.get_storage("file://{0}".format(temp_output.name))
        storage.load_from_file(in_file)

        with open(temp_output.name) as temp_output_fp:
            self.assertEqual("foobar", temp_output_fp.read())

    @mock.patch("os.makedirs")
    @mock.patch("os.path.exists")
    @mock.patch("__builtin__.open")
    def test_load_from_file_creates_dirs_if_not_present(
            self, mock_open, mock_exists, mock_makedirs):
        mock_exists.return_value = False
        in_file = StringIO("foobar")

        mock_file = mock_open.return_value.__enter__.return_value
        mock_file.read.side_effect = ["FOOBAR", None]

        out_storage = storagelib.get_storage("file:///foobar/is/out")
        out_storage.load_from_file(in_file)

        mock_open.assert_has_calls([
            mock.call("/foobar/is/out", "wb")
        ])

        mock_exists.assert_called_with("/foobar/is")
        mock_makedirs.assert_called_with("/foobar/is")
        mock_file.write.assert_called_with("foobar")
        self.assertEqual(1, mock_open.return_value.__exit__.call_count)

    @mock.patch("os.makedirs")
    @mock.patch("os.path.exists")
    @mock.patch("__builtin__.open")
    def test_load_from_file_does_not_create_dirs_if_present(
            self, mock_open, mock_exists, mock_makedirs):
        mock_exists.return_value = True
        in_file = StringIO("foobar")

        out_storage = storagelib.get_storage("file:///foobar/is/out")
        out_storage.load_from_file(in_file)

        mock_exists.assert_called_with("/foobar/is")
        self.assertEqual(0, mock_makedirs.call_count)

    def test_local_storage_get_download_url(self):
        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write("FOOBAR")
        temp_input.flush()

        download_url_base = "http://host:123/path/to/"
        download_url_base_encoded = urllib.quote_plus(download_url_base)

        storage_uri = "file://{fpath}?download_url_base={download_url_base}".format(
            fpath=temp_input.name,
            download_url_base=download_url_base_encoded)

        out_storage = storagelib.get_storage(storage_uri)
        temp_url = out_storage.get_download_url()

        self.assertEqual(
            "http://host:123/path/to/{}".format(os.path.basename(temp_input.name)), temp_url)

    def test_local_storage_get_download_url_ignores_args(self):
        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write("FOOBAR")
        temp_input.flush()

        download_url_base = "http://host:123/path/to/"
        download_url_base_encoded = urllib.quote_plus(download_url_base)

        storage_uri = "file://{fpath}?download_url_base={download_url_base}".format(
            fpath=temp_input.name,
            download_url_base=download_url_base_encoded)

        out_storage = storagelib.get_storage(storage_uri)
        temp_url = out_storage.get_download_url(seconds=900)

        self.assertEqual(
            "http://host:123/path/to/{}".format(os.path.basename(temp_input.name)), temp_url)

        temp_url = out_storage.get_download_url(key="secret")

        self.assertEqual(
            "http://host:123/path/to/{}".format(os.path.basename(temp_input.name)), temp_url)

    def test_local_storage_get_download_url_returns_none_on_empty_base(self):
        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write("FOOBAR")
        temp_input.flush()

        # blank download_url_base
        storage_uri = "file://{fpath}?download_url_base=".format(fpath=temp_input.name)

        out_storage = storagelib.get_storage(storage_uri)

        with self.assertRaises(DownloadUrlBaseUndefinedError):
            out_storage.get_download_url()

        # no download_url_base
        storage_uri = "file://{fpath}".format(fpath=temp_input.name)
        out_storage = storagelib.get_storage(storage_uri)

        with self.assertRaises(DownloadUrlBaseUndefinedError):
            out_storage.get_download_url()


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

    def _assert_login_correct(self, mock_create_context, username=None, password=None, region=None,
                              public=True, tenant_id=None, api_key=None):
        mock_context = mock_create_context.return_value
        mock_create_context.assert_called_with(id_type="pyrax.base_identity.BaseIdentity",
                                               username=username, password=password,
                                               tenant_id=tenant_id, api_key=api_key)
        mock_context.authenticate.assert_called_with()
        mock_context.get_client.assert_called_with("swift", region, public=public)

    def _assert_default_login_correct(self, mock_create_context, api_key=False, public=True):
        self._assert_login_correct(
            mock_create_context,
            api_key=self.params["api_key"] if api_key is True else None,
            password=self.params["password"],
            public=public,
            region=self.params["region"],
            username=self.params["username"],
            tenant_id=self.params["tenant_id"]
        )

    @mock.patch("pyrax.create_context")
    def test_swift_authenticates_with_full_uri(self, mock_create_context):
        mock_context = mock_create_context.return_value
        mock_swift = mock_context.get_client.return_value

        temp_output = tempfile.NamedTemporaryFile()
        mock_swift.fetch_object.return_value = ["FOOBAR", ]

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s&api_key=%(api_key)s" \
              "&tenant_id=%(tenant_id)s&download_url_key=%(download_url_key)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.save_to_filename(temp_output.name)

        self._assert_default_login_correct(mock_create_context, api_key=True)
        mock_swift.fetch_object.assert_called_with(
            self.params["container"], self.params["file"], chunk_size=EXPECTED_CHUNK_SIZE)

    @mock.patch("pyrax.create_context")
    def test_swift_authenticates_with_partial_uri(self, mock_create_context):
        mock_context = mock_create_context.return_value
        mock_swift = mock_context.get_client.return_value

        temp_output = tempfile.NamedTemporaryFile()
        mock_swift.fetch_object.return_value = ["FOOBAR", ]

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.save_to_filename(temp_output.name)

        self._assert_default_login_correct(mock_create_context)
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
    def test_swift_save_to_filename(self, mock_create_context):
        mock_context = mock_create_context.return_value
        mock_swift = mock_context.get_client.return_value

        temp_output = tempfile.NamedTemporaryFile()
        mock_swift.fetch_object.return_value = ["FOOBAR", ]

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.save_to_filename(temp_output.name)

        self._assert_default_login_correct(mock_create_context)
        mock_swift.fetch_object.assert_called_with(
            self.params["container"], self.params["file"], chunk_size=EXPECTED_CHUNK_SIZE)

        with open(temp_output.name) as output_fp:
            self.assertEqual("FOOBAR", output_fp.read())

    @mock.patch("pyrax.create_context")
    def test_swift_save_to_file(self, mock_create_context):
        mock_context = mock_create_context.return_value
        mock_swift = mock_context.get_client.return_value

        mock_swift.fetch_object.return_value = iter(["foo", "bar"])

        out_file = StringIO()

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.save_to_file(out_file)

        self._assert_default_login_correct(mock_create_context)
        mock_swift.fetch_object.assert_called_with(
            self.params["container"], self.params["file"], chunk_size=EXPECTED_CHUNK_SIZE)

        self.assertEqual("foobar", out_file.getvalue())

    @mock.patch("os.path.exists", return_value=False)
    @mock.patch("os.makedirs")
    @mock.patch("pyrax.create_context")
    def test_swift_save_to_directory(self, mock_create_context, mock_makedirs, mock_path_exists):

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

        self._assert_default_login_correct(mock_create_context)
        mock_swift.list_container_objects.assert_called_with(
            self.params["container"], prefix=self.params["file"])

        mock_makedirs.assert_has_calls([
            mock.call("/tmp/cat/pants/a"), mock.call("/tmp/cat/pants/a/b/c")])

        mock_swift.download_object.assert_any_call(
            self.params["container"], expected_jpg, "/tmp/cat/pants/a", structure=False)
        mock_swift.download_object.assert_any_call(
            self.params["container"], expected_mp4, "/tmp/cat/pants/a/b/c", structure=False)

    @mock.patch("os.path.exists", return_value=False)
    @mock.patch("os.makedirs")
    @mock.patch("pyrax.create_context")
    def test_swift_save_to_directory_works_with_empty_directories(
            self, mock_create_context, mock_makedirs, mock_path_exists):

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

        self._assert_default_login_correct(mock_create_context)
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
    def test_swift_load_from_filename(self, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write("FOOBAR")
        temp_input.flush()

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.load_from_filename(temp_input.name)

        self._assert_default_login_correct(mock_create_context)
        mock_swift.upload_file.assert_called_with(
            self.params["container"], temp_input.name, self.params["file"])

    @mock.patch("pyrax.create_context")
    def test_swift_load_from_filename_provides_content_type(self, mock_create_context):
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

        self._assert_default_login_correct(mock_create_context)
        mock_swift.upload_file.assert_called_with(
            self.params["container"], temp_input.name, self.params["file"],
            content_type="video/mp4")

    @mock.patch("pyrax.create_context")
    def test_swift_load_from_file(self, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        mock_input = mock.Mock()

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.load_from_file(mock_input)

        self._assert_default_login_correct(mock_create_context)
        mock_swift.upload_file.assert_called_with(
            self.params["container"], mock_input, self.params["file"])

    @mock.patch("pyrax.create_context")
    def test_swift_load_from_directory(self, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://{username}:{password}@{container}/{file}?" \
              "auth_endpoint={auth_endpoint}&region={region}" \
              "&tenant_id={tenant_id}".format(**self.params)

        storage = storagelib.get_storage(uri)

        temp_directory = create_temp_nested_directory_with_files()
        storage.load_from_directory(temp_directory["temp_directory"]["path"])

        self._assert_default_login_correct(mock_create_context)

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

    @mock.patch("pyrax.create_context")
    def test_swift_delete(self, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.delete()

        self._assert_default_login_correct(mock_create_context)
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
    def test_swift_uses_servicenet_when_requested(self, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s&public=False" % self.params
        storage = storagelib.get_storage(uri)
        storage.delete()

        self._assert_default_login_correct(mock_create_context, public=False)
        mock_swift.delete_object.assert_called_with(self.params["container"], self.params["file"])

    @mock.patch("pyrax.create_context")
    def test_swift_get_download_url(self, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s&download_url_key=%(download_url_key)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.get_download_url()

        self._assert_default_login_correct(mock_create_context)
        mock_swift.get_temp_url.assert_called_with(
            self.params["container"], self.params["file"],
            seconds=60, method="GET", key="super_secret_key")

    @mock.patch("pyrax.create_context")
    def test_swift_get_download_url_without_temp_url_key(self, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.get_download_url()

        self._assert_default_login_correct(mock_create_context)
        mock_swift.get_temp_url.assert_called_with(
            self.params["container"], self.params["file"], seconds=60, method="GET", key=None)

    @mock.patch("pyrax.create_context")
    def test_swift_get_download_url_with_override(self, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s&download_url_key=%(download_url_key)s" % self.params
        storage = storagelib.get_storage(uri)

        storage.get_download_url(key="NOT-THE-URI-KEY")

        self._assert_default_login_correct(mock_create_context)
        mock_swift.get_temp_url.assert_called_with(
            self.params["container"], self.params["file"],
            seconds=60, method="GET", key="NOT-THE-URI-KEY")

    @mock.patch("pyrax.create_context")
    def test_swift_get_download_url_with_non_default_expiration(self, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.get_download_url(seconds=10 * 60)

        self._assert_default_login_correct(mock_create_context)
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
        class MyStorageClass(storagelib.storage.SwiftStorage):
            pass

        self.assertIn(self.scheme, storagelib.storage._STORAGE_TYPES)

        uri = "{0}://username:password@containter/object?region=region-a".format(self.scheme)
        store_obj = storagelib.get_storage(uri)
        self.assertIsInstance(store_obj, MyStorageClass)

    @mock.patch("pyrax.create_context")
    def test_register_allows_override_of_auth_url(self, mock_create_context):
        @storagelib.register_swift_protocol(scheme=self.scheme, auth_endpoint=self.auth_endpoint)
        class MyStorageClass(storagelib.storage.SwiftStorage):
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


class TestCloudfilesStorage(TestCase):
    def _assert_login_correct(self, mock_create_context, username, password, region, public):
        mock_context = mock_create_context.return_value
        mock_create_context.assert_called_with("rackspace", username=username, password=password)
        mock_context.authenticate.assert_called_with()
        mock_context.get_client.assert_called_with("cloudfiles", region, public=public)

    @mock.patch("pyrax.create_context")
    def test_cloudfilesstorage_authenticate_with_defaults(self, mock_create_context):
        uri = "cloudfiles://{username}:{api_key}@{container}/{file}".format(
            username="username", api_key="apikey", container="container", file="file.txt")

        storage = storagelib.get_storage(uri)
        storage.delete()

        self.assertEqual(None, storage.download_url_key)
        self._assert_login_correct(
            mock_create_context, username="username", password="apikey", region="DFW", public=True)

    @mock.patch("pyrax.create_context")
    def test_rackspace_authenticate_with_download_url_key(self, mock_create_context):
        uri = "cloudfiles://{username}:{api_key}@{container}/{file}?download_url_key={key}".format(
            username="username", api_key="apikey", container="container", file="file.txt",
            key="super_secret_key")

        storage = storagelib.get_storage(uri)
        storage.delete()

        self.assertEqual("super_secret_key", storage.download_url_key)
        self._assert_login_correct(
            mock_create_context, username="username", password="apikey", region="DFW", public=True)

    @mock.patch("pyrax.create_context")
    def test_cloudfilesstorage_authenticate_with_public_false(self, mock_create_context):
        uri = "cloudfiles://{username}:{api_key}@{container}/{file}?public=False".format(
            username="username", api_key="apikey", container="container", file="file.txt")

        storage = storagelib.get_storage(uri)
        storage.delete()

        self._assert_login_correct(
            mock_create_context, username="username", password="apikey", region="DFW",
            public=False)

    @mock.patch("pyrax.create_context")
    def test_cloudfilesstorage_authenticate_with_region(self, mock_create_context):
        uri = "cloudfiles://{username}:{api_key}@{container}/{file}?region=ORD".format(
            username="username", api_key="apikey", container="container", file="file.txt")

        storage = storagelib.get_storage(uri)
        storage.delete()

        self._assert_login_correct(
            mock_create_context, username="username", password="apikey", region="ORD",
            public=True)

    @mock.patch("pyrax.create_context")
    def test_cloudfilesstorage_get_download_url_sets_default_exp_time(self, mock_create_context):
        mock_cloudfiles = mock_create_context.return_value.get_client.return_value
        mock_cloudfiles.get_temp_url.return_value = "http://fake.download.url"
        uri = "cloudfiles://username:apikey@container/file.txt?region=ORD"

        storage = storagelib.get_storage(uri)
        download_url = storage.get_download_url()

        mock_cloudfiles.get_temp_url.assert_called_with(
            "container", "file.txt", seconds=60, method="GET")
        self.assertEqual("http://fake.download.url", download_url)

    @mock.patch("pyrax.create_context")
    def test_cloudfilesstorage_get_download_url_sets_custom_exp_time(self, mock_create_context):
        mock_cloudfiles = mock_create_context.return_value.get_client.return_value
        mock_cloudfiles.get_temp_url.return_value = "http://fake.download.url"
        uri = "cloudfiles://username:apikey@container/file.txt?region=ORD"

        storage = storagelib.get_storage(uri)
        download_url = storage.get_download_url(seconds=3000)

        mock_cloudfiles.get_temp_url.assert_called_with(
            "container", "file.txt", seconds=3000, method="GET")
        self.assertEqual("http://fake.download.url", download_url)

    @mock.patch("pyrax.create_context")
    def test_cloudfilesstorage_get_download_url_returns_url(self, mock_create_context):
        mock_cloudfiles = mock_create_context.return_value.get_client.return_value
        mock_cloudfiles.get_temp_url.return_value = "http://fake.download.url"
        uri = "cloudfiles://username:apikey@container/file.txt?region=ORD"

        storage = storagelib.get_storage(uri)
        download_url = storage.get_download_url()

        self.assertEqual("http://fake.download.url", download_url)

    @mock.patch("pyrax.create_context")
    def test_cloudfilesstorage_get_download_url_returns_urlencoded_response(self,
                                                                            mock_create_context):
        mock_cloudfiles = mock_create_context.return_value.get_client.return_value
        mock_cloudfiles.get_temp_url.return_value = "http://fake.download.url/with spaces in name"
        uri = "cloudfiles://username:apikey@container/file.txt?region=ORD"

        storage = storagelib.get_storage(uri)
        download_url = storage.get_download_url()

        self.assertEqual("http://fake.download.url/with%20spaces%20in%20name", download_url)


class TestFTPStorage(TestCase):
    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_save_to_filename(self, mock_ftp_class):
        temp_output = tempfile.NamedTemporaryFile()

        mock_results = ["foo", "bar"]

        def mock_retrbinary(command, callback):
            for chunk in mock_results:
                callback(chunk)

            return "226"

        mock_ftp = mock_ftp_class.return_value
        mock_ftp.retrbinary.side_effect = mock_retrbinary

        storage = storagelib.get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

        storage.save_to_filename(temp_output.name)

        mock_ftp_class.assert_called_with(timeout=storagelib.storage.DEFAULT_FTP_TIMEOUT)
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")

        mock_ftp.cwd.assert_called_with("some/dir")
        self.assertEqual(1, mock_ftp.retrbinary.call_count)
        self.assertEqual("RETR file", mock_ftp.retrbinary.call_args[0][0])

        with open(temp_output.name) as output_fp:
            self.assertEqual("foobar", output_fp.read())

    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_save_to_file(self, mock_ftp_class):
        out_file = StringIO()

        mock_results = ["foo", "bar"]

        def mock_retrbinary(command, callback):
            for chunk in mock_results:
                callback(chunk)

            return "226"

        mock_ftp = mock_ftp_class.return_value
        mock_ftp.retrbinary.side_effect = mock_retrbinary

        storage = storagelib.get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

        storage.save_to_file(out_file)

        mock_ftp_class.assert_called_with(timeout=storagelib.storage.DEFAULT_FTP_TIMEOUT)
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")

        mock_ftp.cwd.assert_called_with("some/dir")
        self.assertEqual(1, mock_ftp.retrbinary.call_count)
        self.assertEqual("RETR file", mock_ftp.retrbinary.call_args[0][0])

        self.assertEqual("foobar", out_file.getvalue())

    @mock.patch("ftplib.FTP", autospec=True)
    def test_save_to_file_with_specific_port(self, mock_ftp_class):
        out_file = StringIO()

        mock_results = ["foo", "bar"]

        def mock_retrbinary(command, callback):
            for chunk in mock_results:
                callback(chunk)

            return "226"

        mock_ftp = mock_ftp_class.return_value
        mock_ftp.retrbinary.side_effect = mock_retrbinary

        storage = storagelib.get_storage("ftp://user:password@ftp.foo.com:12345/some/dir/file")

        storage.save_to_file(out_file)

        mock_ftp_class.assert_called_with(timeout=storagelib.storage.DEFAULT_FTP_TIMEOUT)
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=12345)
        mock_ftp.login.assert_called_with("user", "password")

        mock_ftp.cwd.assert_called_with("some/dir")
        self.assertEqual(1, mock_ftp.retrbinary.call_count)
        self.assertEqual("RETR file", mock_ftp.retrbinary.call_args[0][0])

        self.assertEqual("foobar", out_file.getvalue())

    @mock.patch("os.chdir")
    @mock.patch("os.makedirs")
    @mock.patch("os.path.exists", return_value=False)
    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_save_to_directory_creates_destination_directory_if_needed(
            self, mock_ftp_class, mock_path_exists, mock_makedirs, mock_chdir):
        mock_ftp = mock_ftp_class.return_value
        mock_ftp.pwd.return_value = "some/dir/file"

        # no files or folders
        mock_ftp.retrlines.side_effect = create_mock_ftp_directory_listing([])

        mock_path_exists.return_value = False

        storage = storagelib.get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

        storage.save_to_directory("/cat/pants")

        mock_ftp_class.assert_called_with(timeout=storagelib.storage.DEFAULT_FTP_TIMEOUT)
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")

        mock_ftp.cwd.assert_has_calls([
            mock.call("/some/dir/file"),
        ])

        mock_makedirs.assert_has_calls([
            mock.call("/cat/pants"),
        ])

        mock_chdir.assert_has_calls([
            mock.call("/cat/pants"),
        ])

        mock_ftp.retrbinary.assert_not_called()

    @mock.patch("__builtin__.open", autospec=True)
    @mock.patch("os.chdir")
    @mock.patch("os.makedirs")
    @mock.patch("os.path.exists", return_value=False)
    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_save_to_directory_downloads_nested_files(
            self, mock_ftp_class, mock_path_exists, mock_makedirs, mock_chdir, mock_open):
        mock_ftp = mock_ftp_class.return_value
        mock_ftp.pwd.return_value = "some/place/special"
        expected_calls = [
            mock.call("/cat/pants"),
            mock.call("/cat/pants/dir1"),
            mock.call("/cat/pants/dir1/dir with spaces"),
            mock.call("/cat/pants/dir2"),
            mock.call("/cat/pants/dir2/dir4"),
        ]

        mock_ftp.retrlines.side_effect = create_mock_ftp_directory_listing([
            # root
            [
                "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir1",
                "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir2",
                "-rwxrwxr-x 3 test test 4.0K Apr  9 10:54 file1",
                "-rwxrwxr-x 3 test test 4.0K Apr  9 10:54 file2",
            ],
            # dir1
            [
                "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir with spaces",
                "-rwxrwxr-x 3 test test 4.0K Apr  9 10:54 file3",
            ],
            # dir with spaces
            [
                "-rwxrwxr-x 3 test test 4.0K Apr  9 10:54 file with spaces"
            ],
            # dir2
            [
                "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir4",
            ],
        ])

        storage = storagelib.get_storage("ftp://user:password@ftp.foo.com/some/place/special")

        storage.save_to_directory("/cat/pants")

        mock_ftp_class.assert_called_with(timeout=storagelib.storage.DEFAULT_FTP_TIMEOUT)
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")

        self.assertEqual(5, mock_ftp.cwd.call_count)
        mock_ftp.cwd.assert_has_calls([
            mock.call("/some/place/special"),
            mock.call("some/place/special/dir1"),
            mock.call("some/place/special/dir1/dir with spaces"),
            mock.call("some/place/special/dir2"),
            mock.call("some/place/special/dir2/dir4"),
        ])

        self.assertEqual(5, mock_makedirs.call_count)
        mock_makedirs.assert_has_calls(expected_calls)

        self.assertEqual(5, mock_chdir.call_count)
        mock_chdir.assert_has_calls(expected_calls)

        self.assertEqual(4, mock_open.call_count)
        mock_open.assert_has_calls([
            mock.call("/cat/pants/file1", "wb"),
            mock.call("/cat/pants/file2", "wb"),
            mock.call("/cat/pants/dir1/file3", "wb"),
            mock.call("/cat/pants/dir1/dir with spaces/file with spaces", "wb"),
        ], any_order=True)

        self.assertEqual(4, mock_ftp.retrbinary.call_count)
        mock_ftp.retrbinary.assert_has_calls([
            mock.call("RETR file1", callback=mock_open.return_value.__enter__.return_value.write),
            mock.call("RETR file2", callback=mock_open.return_value.__enter__.return_value.write),
            mock.call("RETR file3", callback=mock_open.return_value.__enter__.return_value.write),
            mock.call(
                "RETR file with spaces",
                callback=mock_open.return_value.__enter__.return_value.write),
        ])

        mock_ftp.storbinary.assert_not_called()

    @mock.patch("__builtin__.open", autospec=True)
    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_load_from_filename(self, mock_ftp_class, mock_open):
        mock_ftp = mock_ftp_class.return_value

        storage = storagelib.get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

        storage.load_from_filename("some_file")

        mock_ftp_class.assert_called_with(timeout=storagelib.storage.DEFAULT_FTP_TIMEOUT)
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")

        mock_ftp.cwd.assert_called_with("some/dir")

        mock_open.assert_called_with("some_file", "rb")
        mock_ftp.storbinary.assert_called_with(
            "STOR file", mock_open.return_value.__enter__.return_value)

    @mock.patch("__builtin__.open", autospec=True)
    @mock.patch("ftplib.FTP", autospec=True)
    def test_load_from_filename_with_specific_port(self, mock_ftp_class, mock_open):
        mock_ftp = mock_ftp_class.return_value

        storage = storagelib.get_storage("ftp://user:password@ftp.foo.com:12345/some/dir/file")

        storage.load_from_filename("some_file")

        mock_ftp_class.assert_called_with(timeout=storagelib.storage.DEFAULT_FTP_TIMEOUT)
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=12345)
        mock_ftp.login.assert_called_with("user", "password")

        mock_ftp.cwd.assert_called_with("some/dir")

        mock_open.assert_called_with("some_file", "rb")
        mock_ftp.storbinary.assert_called_with(
            "STOR file", mock_open.return_value.__enter__.return_value)

    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_load_from_file(self, mock_ftp_class):
        mock_ftp = mock_ftp_class.return_value
        in_file = StringIO("foobar")

        storage = storagelib.get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

        storage.load_from_file(in_file)

        mock_ftp_class.assert_called_with(timeout=storagelib.storage.DEFAULT_FTP_TIMEOUT)
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")

        mock_ftp.cwd.assert_called_with("some/dir")

        mock_ftp.storbinary.assert_called_with("STOR file", in_file)

    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_load_from_directory_creates_directories_from_storage_URI_if_not_present(
            self, mock_ftp_class):
        mock_ftp = mock_ftp_class.return_value

        mock_ftp.retrlines.return_value = []

        storage = storagelib.get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

        # empty folder
        storage.load_from_directory(tempfile.mkdtemp())

        mock_ftp_class.assert_called_with(timeout=storagelib.storage.DEFAULT_FTP_TIMEOUT)
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")

        mock_ftp.mkd.assert_has_calls([
            mock.call("some"),
            mock.call("dir"),
            mock.call("file")
        ])

    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_load_from_directory_does_not_create_dirs_from_storage_URI_if_present(
            self, mock_ftp_class):
        mock_ftp = mock_ftp_class.return_value

        mock_ftp.retrlines.side_effect = create_mock_ftp_directory_listing([
            "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 some",
            "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir",
        ])

        storage = storagelib.get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

        # empty folder
        storage.load_from_directory(tempfile.mkdtemp())

        mock_ftp_class.assert_called_with(timeout=storagelib.storage.DEFAULT_FTP_TIMEOUT)
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")
        self.assertEqual(1, mock_ftp.mkd.call_count)
        mock_ftp.mkd.assert_has_calls([mock.call("file")])
        mock_ftp.cwd.assert_has_calls([
            mock.call("some"),
            mock.call("dir"),
            mock.call("file")
        ])

    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_load_from_directory_create_dirs_from_load_directory(
            self, mock_ftp_class):
        mock_ftp = mock_ftp_class.return_value
        mock_ftp.pwd.return_value = "pwd_return_value"
        temp_directory = create_temp_nested_directory_with_files()

        directory_listings = [
            "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 some",
            "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir",
        ]

        def get_directory_listing(_, callback):
            if len(directory_listings):
                return callback(directory_listings.pop(0))

        mock_ftp.retrlines.side_effect = get_directory_listing

        storage = storagelib.get_storage("ftp://user:password@ftp.foo.com/some/dir")

        # empty folder
        storage.load_from_directory(temp_directory["temp_directory"]["path"])

        mock_ftp_class.assert_called_with(timeout=storagelib.storage.DEFAULT_FTP_TIMEOUT)
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")

        mock_ftp.mkd.assert_has_calls([
            mock.call(temp_directory["nested_temp_directory"]["name"])
        ])

        mock_ftp.cwd.assert_has_calls([
            mock.call("some"),
            mock.call("dir"),
            mock.call("/some/dir"),
            mock.call(temp_directory["nested_temp_directory"]["name"]),
            mock.call("pwd_return_value"),
            mock.call("/some/dir/" + temp_directory["nested_temp_directory"]["name"])
        ])

    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_load_from_directory_does_not_create_existing_dirs_from_load_directory(
            self, mock_ftp_class):
        mock_ftp = mock_ftp_class.return_value
        mock_ftp.pwd.return_value = "pwd_return_value"
        temp_directory = create_temp_nested_directory_with_files()

        directory_listings = [
            "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 some",
            "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir",
            "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 {0}".format(
                temp_directory["nested_temp_directory"]["name"]),
        ]

        def get_directory_listing(_, callback):
            if len(directory_listings):
                return callback(directory_listings.pop(0))

        mock_ftp.retrlines.side_effect = get_directory_listing

        storage = storagelib.get_storage("ftp://user:password@ftp.foo.com/some/dir")

        # empty folder
        storage.load_from_directory(temp_directory["temp_directory"]["path"])

        mock_ftp_class.assert_called_with(timeout=storagelib.storage.DEFAULT_FTP_TIMEOUT)
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")
        mock_ftp.mkd.assert_not_called()

        mock_ftp.cwd.assert_has_calls([
            mock.call("some"),
            mock.call("dir"),
            mock.call("/some/dir"),
            mock.call(temp_directory["nested_temp_directory"]["name"]),
            mock.call("pwd_return_value"),
            mock.call("/some/dir/" + temp_directory["nested_temp_directory"]["name"])
        ])

    @mock.patch("__builtin__.open", autospec=True)
    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_load_from_directory_creates_files_from_local_source_directory(
            self, mock_ftp_class, mock_open):
        mock_ftp = mock_ftp_class.return_value
        mock_ftp.pwd.return_value = "pwd_return_value"
        mock_open_return = mock_open.return_value.__enter__.return_value

        temp_directory = create_temp_nested_directory_with_files()

        directory_listings = [
            "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 {0}".format(
                temp_directory["nested_temp_directory"]["name"]),
        ]

        def get_directory_listing(_, callback):
            if len(directory_listings):
                return callback(directory_listings.pop(0))

        mock_ftp.retrlines.side_effect = get_directory_listing

        storage = storagelib.get_storage("ftp://user:password@ftp.foo.com/dir")

        # empty folder
        storage.load_from_directory(temp_directory["temp_directory"]["path"])

        mock_ftp_class.assert_called_with(timeout=storagelib.storage.DEFAULT_FTP_TIMEOUT)
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")

        mock_ftp.storbinary.assert_has_calls([
            mock.call(
                "STOR {0}".format(temp_directory["temp_input_one"]["name"]), mock_open_return),
            mock.call(
                "STOR {0}".format(temp_directory["temp_input_two"]["name"]), mock_open_return),
            mock.call(
                "STOR {0}".format(temp_directory["nested_temp_input"]["name"]), mock_open_return)
        ], any_order=True)

        mock_ftp.cwd.assert_has_calls([
            mock.call("dir"),
            mock.call("/dir"),
            mock.call(temp_directory["nested_temp_directory"]["name"]),
            mock.call("pwd_return_value"),
            mock.call("/dir/" + temp_directory["nested_temp_directory"]["name"])
        ])

    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_delete(self, mock_ftp_class):
        mock_ftp = mock_ftp_class.return_value

        storage = storagelib.get_storage("ftp://user:password@ftp.foo.com/some/dir/file")
        storage.delete()

        mock_ftp_class.assert_called_with(timeout=storagelib.storage.DEFAULT_FTP_TIMEOUT)
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")

        mock_ftp.cwd.assert_called_with("some/dir")
        mock_ftp.delete.assert_called_with("file")

    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_delete_directory(self, mock_ftp_class):
        mock_ftp = mock_ftp_class.return_value

        mock_ftp.pwd.return_value = "some/dir/file"

        mock_ftp.retrlines.side_effect = create_mock_ftp_directory_listing([
            # root
            [
                "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir1",
                "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir2",
                "-rwxrwxr-x 3 test test 4.0K Apr  9 10:54 file1",
                "-rwxrwxr-x 3 test test 4.0K Apr  9 10:54 file2",
            ],
            # dir1
            [
                "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir with spaces",
                "-rwxrwxr-x 3 test test 4.0K Apr  9 10:54 file3",
            ],
            # dir with spaces
            [
                "-rwxrwxr-x 3 test test 4.0K Apr  9 10:54 file with spaces"
            ],
            # dir2
            [
                "drwxrwxr-x 3 test test 4.0K Apr  9 10:54 dir4",
            ],
        ])

        storage = storagelib.get_storage("ftp://user:password@ftp.foo.com/some/dir/file")
        storage.delete_directory()

        mock_ftp_class.assert_called_with(timeout=storagelib.storage.DEFAULT_FTP_TIMEOUT)
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")

        mock_ftp.cwd.assert_has_calls([
            mock.call("/some/dir/file"),
            mock.call("some/dir/file/dir1"),
            mock.call("some/dir/file/dir1/dir with spaces"),
            mock.call("some/dir/file/dir2"),
            mock.call("some/dir/file/dir2/dir4")
        ])
        self.assertEqual(5, mock_ftp.cwd.call_count)

        mock_ftp.delete.assert_has_calls([
            mock.call("/some/dir/file/dir1/dir with spaces/file with spaces"),
            mock.call("/some/dir/file/dir1/file3"),
            mock.call("/some/dir/file/file2"),
            mock.call("/some/dir/file/file1")
        ], any_order=True)
        self.assertEqual(4, mock_ftp.delete.call_count)

        mock_ftp.rmd.assert_has_calls([
            mock.call("/some/dir/file/dir2/dir4"),
            mock.call("/some/dir/file/dir2"),
            mock.call("/some/dir/file/dir1/dir with spaces"),
            mock.call("/some/dir/file/dir1"),
            mock.call("/some/dir/file")
        ])
        self.assertEqual(5, mock_ftp.rmd.call_count)

    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_get_download_url(self, mock_ftp_class):
        download_url_base = urllib.quote_plus("http://hostname/path/to/")

        ftpuri = "ftp://user:password@ftp.foo.com/some/dir/file.txt?download_url_base={0}".format(
            download_url_base)

        storage = storagelib.get_storage(ftpuri)
        temp_url = storage.get_download_url()

        mock_ftp_class.assert_not_called()
        self.assertEqual(temp_url, "http://hostname/path/to/file.txt")

    @mock.patch("ftplib.FTP", autospec=True)
    def test_ftp_get_download_url_returns_none_with_empty_base(self, mock_ftp_class):
        ftpuri = "ftp://user:password@ftp.foo.com/some/dir/file.txt"

        storage = storagelib.get_storage(ftpuri)

        with self.assertRaises(DownloadUrlBaseUndefinedError):
            storage.get_download_url()

        mock_ftp_class.assert_not_called()


class TestFTPSStorage(TestCase):
    @mock.patch("ftplib.FTP_TLS", autospec=True)
    def test_ftps_scheme_connects_using_ftp_tls_class(self, mock_ftp_tls_class):
        temp_output = tempfile.NamedTemporaryFile()

        mock_results = ["foo", "bar"]

        def mock_retrbinary(command, callback):
            for chunk in mock_results:
                callback(chunk)

            return "226"

        mock_ftp = mock_ftp_tls_class.return_value
        mock_ftp.retrbinary.side_effect = mock_retrbinary

        storage = storagelib.get_storage("ftps://user:password@ftp.foo.com/some/dir/file")

        storage.save_to_filename(temp_output.name)

        mock_ftp_tls_class.assert_called_with(timeout=storagelib.storage.DEFAULT_FTP_TIMEOUT)
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")
        mock_ftp.prot_p.assert_called_with()


class TestS3Storage(TestCase):
    def test_s3storage_init_sets_correct_keyname(self):
        storage = storagelib.get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        self.assertEqual("some/file", storage._keyname)

    @mock.patch("boto3.session.Session", autospec=True)
    def test_handles_urlencoded_keys(self, mock_session_class):
        encoded_key = urllib.quote("access/key", safe="")
        encoded_secret = urllib.quote("access/secret", safe="")

        storage = storagelib.get_storage(
            "s3://{0}:{1}@bucket/some/file?region=US_EAST".format(encoded_key, encoded_secret))

        storage._connect()

        mock_session_class.assert_called_with(
            aws_access_key_id="access/key",
            aws_secret_access_key="access/secret",
            region_name="US_EAST")

    @mock.patch("boto3.session.Session", autospec=True)
    def test_load_from_file(self, mock_session_class):
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value

        mock_file = mock.Mock()

        storage = storagelib.get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        storage.load_from_file(mock_file)

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name="US_EAST")

        mock_session.client.assert_called_with("s3")

        mock_s3.put_object.assert_called_with(Bucket="bucket", Key="some/file", Body=mock_file)

    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    @mock.patch("boto3.session.Session", autospec=True)
    def test_load_from_filename(self, mock_session_class, mock_transfer_class):
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value
        mock_transfer = mock_transfer_class.return_value

        storage = storagelib.get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        storage.load_from_filename("source/file")

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name="US_EAST")

        mock_session.client.assert_called_with("s3")

        mock_transfer_class.assert_called_with(mock_s3)

        mock_transfer.upload_file.assert_called_with("source/file", "bucket", "some/file")

    @mock.patch("boto3.session.Session", autospec=True)
    def test_save_to_file(self, mock_session_class):
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value

        mock_body = mock.Mock()
        mock_body.read.side_effect = [b"some", b"file", b"contents", None]
        mock_s3.get_object.return_value = {
            "Body": mock_body
        }

        mock_file = mock.Mock()

        storage = storagelib.get_storage(
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

    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    @mock.patch("boto3.session.Session", autospec=True)
    def test_save_to_filename(self, mock_session_class, mock_transfer_class):
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value

        mock_transfer = mock_transfer_class.return_value

        storage = storagelib.get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        storage.save_to_filename("destination/file")

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name="US_EAST")

        mock_session.client.assert_called_with("s3")

        mock_transfer_class.assert_called_with(mock_s3)
        mock_transfer.download_file.assert_called_with("bucket", "some/file", "destination/file")

    @mock.patch("os.makedirs")
    @mock.patch("os.path.exists")
    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    @mock.patch("boto3.session.Session", autospec=True)
    def test_save_to_directory(
            self, mock_session_class, mock_transfer_class, mock_path_exists, mock_makedirs):
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
        path_mock_path_exists_calls = []

        def mock_path_exists_side_effect(path):
            if any(path in x for x in path_mock_path_exists_calls):
                return True
            else:
                path_mock_path_exists_calls.append(path)
                return False

        mock_path_exists.side_effect = mock_path_exists_side_effect

        storage = storagelib.get_storage(
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

    @mock.patch("boto3.s3.transfer.S3Transfer", autospec=True)
    @mock.patch("boto3.session.Session", autospec=True)
    def test_load_from_directory(self, mock_session_class, mock_transfer_class):
        mock_session = mock_session_class.return_value
        mock_s3_client = mock_session.client.return_value

        temp_directory = create_temp_nested_directory_with_files()

        storage = storagelib.get_storage(
            "s3://access_key:access_secret@bucket/dir?region=US_EAST")

        storage.load_from_directory(temp_directory["temp_directory"]["path"])

        mock_s3_client.upload_file.assert_has_calls([
            mock.call(
                temp_directory["temp_input_two"]["path"], "bucket",
                os.path.join("dir", temp_directory["temp_input_two"]["name"])),
            mock.call(
                temp_directory["temp_input_one"]["path"], "bucket",
                os.path.join("dir", temp_directory["temp_input_one"]["name"])),
            mock.call(
                temp_directory["nested_temp_input"]["path"], "bucket",
                os.path.join(
                    "dir", temp_directory["nested_temp_directory"]["name"],
                    temp_directory["nested_temp_input"]["name"]))
        ], any_order=True)

    @mock.patch("boto3.session.Session", autospec=True)
    def test_delete(self, mock_session_class):
        mock_session = mock_session_class.return_value
        mock_s3 = mock_session.client.return_value

        storage = storagelib.get_storage(
            "s3://access_key:access_secret@bucket/some/file?region=US_EAST")

        storage.delete()

        mock_session_class.assert_called_with(
            aws_access_key_id="access_key",
            aws_secret_access_key="access_secret",
            region_name="US_EAST")

        mock_session.client.assert_called_with("s3")

        mock_s3.delete_object.assert_called_with(Bucket="bucket", Key="some/file")

    @mock.patch("boto3.session.Session", autospec=True)
    def test_delete_directory(self, mock_session_class):
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

        storage = storagelib.get_storage(
            "s3://access_key:access_secret@bucket/some/dir")

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
    def test_get_download_url_calls_boto_generate_presigned_url_with_correct_data(
            self, mock_session_class):
        mock_session = mock_session_class.return_value
        url = "s3://access_key:access_secret@some_bucket/"
        key = "some/file"
        mock_session.client.return_value.generate_presigned_url.return_value = "".join(
            ["http://fake.url/", key])

        storage = storagelib.get_storage("".join([url, key, "?region=US_EAST"]))
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
            self, mock_session_class):
        mock_session = mock_session_class.return_value
        url = "s3://access_key:access_secret@some_bucket/"
        key = "some/file"
        mock_session.client.return_value.generate_presigned_url.return_value = "".join(
            ["http://fake.url/", key])

        storage = storagelib.get_storage("".join([url, key, "?region=US_EAST"]))
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
