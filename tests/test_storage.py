import mock
import os.path
import storage as storagelib
from storage.storage import DownloadUrlBaseUndefinedError
import tempfile
import urllib
from unittest import TestCase
from StringIO import StringIO


EXPECTED_CHUNK_SIZE = 32 * 1024 * 1024


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

        self.assertEqual("http://host:123/path/to/{}".format(os.path.basename(temp_input.name)),
            temp_url)

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

        self.assertEqual("http://host:123/path/to/{}".format(os.path.basename(temp_input.name)),
            temp_url)

        temp_url = out_storage.get_download_url(key="secret")

        self.assertEqual("http://host:123/path/to/{}".format(os.path.basename(temp_input.name)),
            temp_url)

    def test_local_storage_get_download_url_returns_none_on_empty_base(self):
        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write("FOOBAR")
        temp_input.flush()

        # blank download_url_base
        storage_uri = "file://{fpath}?download_url_base=".format(fpath=temp_input.name)

        out_storage = storagelib.get_storage(storage_uri)

        with self.assertRaises(DownloadUrlBaseUndefinedError):
            temp_url = out_storage.get_download_url()

        # no download_url_base
        storage_uri = "file://{fpath}".format(fpath=temp_input.name)
        out_storage = storagelib.get_storage(storage_uri)

        with self.assertRaises(DownloadUrlBaseUndefinedError):
            temp_url = out_storage.get_download_url()


class TestSwiftStorage(TestCase):

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

        self._assert_login_correct(mock_create_context, username=self.params["username"],
                                   password=self.params["password"], region=self.params["region"],
                                   public=True, tenant_id=self.params["tenant_id"],
                                   api_key=self.params["api_key"])
        mock_swift.fetch_object.assert_called_with(self.params["container"],
                                                   self.params["file"],
                                                   chunk_size=EXPECTED_CHUNK_SIZE)

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

        self._assert_login_correct(mock_create_context, username=self.params["username"],
                                   password=self.params["password"], region=self.params["region"],
                                   public=True, tenant_id=self.params["tenant_id"])
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

        self._assert_login_correct(mock_create_context, username=self.params["username"],
                                   password=self.params["password"], region=self.params["region"],
                                   tenant_id=self.params["tenant_id"], public=True)
        mock_swift.fetch_object.assert_called_with(self.params["container"], self.params["file"],
                                                   chunk_size=EXPECTED_CHUNK_SIZE)

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

        self._assert_login_correct(mock_create_context, username=self.params["username"],
                                   password=self.params["password"], region=self.params["region"],
                                   tenant_id=self.params["tenant_id"], public=True)
        mock_swift.fetch_object.assert_called_with(self.params["container"], self.params["file"],
                                                   chunk_size=EXPECTED_CHUNK_SIZE)

        self.assertEqual("foobar", out_file.getvalue())

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

        self._assert_login_correct(mock_create_context, username=self.params["username"],
                                   password=self.params["password"], region=self.params["region"],
                                   tenant_id=self.params["tenant_id"], public=True)
        mock_swift.upload_file.assert_called_with(self.params["container"], temp_input.name,
                                                  self.params["file"])

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
        mock_swift.upload_file.assert_called_with(self.params["container"], temp_input.name,
                                                  self.params["file"], content_type="video/mp4")

    @mock.patch("pyrax.create_context")
    def test_swift_load_from_file(self, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        mock_input = mock.Mock()

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.load_from_file(mock_input)

        self._assert_login_correct(mock_create_context, username=self.params["username"],
                                   password=self.params["password"], region=self.params["region"],
                                   tenant_id=self.params["tenant_id"], public=True)
        mock_swift.upload_file.assert_called_with(self.params["container"], mock_input,
                                                  self.params["file"])

    @mock.patch("pyrax.create_context")
    def test_swift_delete(self, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.delete()

        self._assert_login_correct(mock_create_context, username=self.params["username"],
                                   password=self.params["password"], region=self.params["region"],
                                   tenant_id=self.params["tenant_id"], public=True)
        mock_swift.delete_object.assert_called_with(self.params["container"], self.params["file"])

    @mock.patch("pyrax.create_context")
    def test_swift_uses_servicenet_when_requested(self, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s&public=False" % self.params
        storage = storagelib.get_storage(uri)
        storage.delete()

        self._assert_login_correct(mock_create_context, username=self.params["username"],
                                   password=self.params["password"], region=self.params["region"],
                                   tenant_id=self.params["tenant_id"], public=False)
        mock_swift.delete_object.assert_called_with(self.params["container"], self.params["file"])

    @mock.patch("pyrax.create_context")
    def test_swift_get_download_url(self, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s&download_url_key=%(download_url_key)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.get_download_url()

        self._assert_login_correct(mock_create_context, username=self.params["username"],
            password=self.params["password"], region=self.params["region"],
            tenant_id=self.params["tenant_id"], public=True)
        mock_swift.get_temp_url.assert_called_with(self.params["container"], self.params["file"],
            seconds=60, method="GET", key="super_secret_key")

    @mock.patch("pyrax.create_context")
    def test_swift_get_download_url_without_temp_url_key(self, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.get_download_url()

        self._assert_login_correct(mock_create_context, username=self.params["username"],
            password=self.params["password"], region=self.params["region"],
            tenant_id=self.params["tenant_id"], public=True)
        mock_swift.get_temp_url.assert_called_with(self.params["container"], self.params["file"],
            seconds=60, method="GET", key=None)

    @mock.patch("pyrax.create_context")
    def test_swift_get_download_url_with_override(self, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s&download_url_key=%(download_url_key)s" % self.params
        storage = storagelib.get_storage(uri)

        storage.get_download_url(key="NOT-THE-URI-KEY")

        self._assert_login_correct(mock_create_context, username=self.params["username"],
            password=self.params["password"], region=self.params["region"],
            tenant_id=self.params["tenant_id"], public=True)
        mock_swift.get_temp_url.assert_called_with(self.params["container"], self.params["file"],
            seconds=60, method="GET", key="NOT-THE-URI-KEY")

    @mock.patch("pyrax.create_context")
    def test_swift_get_download_url_with_non_default_expiration(self, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" \
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.get_download_url(seconds=10*60)

        self._assert_login_correct(mock_create_context, username=self.params["username"],
            password=self.params["password"], region=self.params["region"],
            tenant_id=self.params["tenant_id"], public=True)
        mock_swift.get_temp_url.assert_called_with(self.params["container"], self.params["file"],
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


class TestRackspaceStorage(TestCase):
    def _assert_login_correct(self, mock_create_context, username, password, region, public):
        mock_context = mock_create_context.return_value
        mock_create_context.assert_called_with("rackspace", username=username, password=password)
        mock_context.authenticate.assert_called_with()
        mock_context.get_client.assert_called_with("cloudfiles", region, public=public)

    @mock.patch("pyrax.create_context")
    def test_rackspace_authenticate_with_defaults(self, mock_create_context):
        uri = "cloudfiles://{username}:{api_key}@{container}/{file}".format(
            username="username", api_key="apikey", container="container", file="file.txt")

        storage = storagelib.get_storage(uri)
        storage.delete()

        self._assert_login_correct(
            mock_create_context, username="username", password="apikey", region="DFW", public=True)

    @mock.patch("pyrax.create_context")
    def test_rackspace_authenticate_with_public_false(self, mock_create_context):
        uri = "cloudfiles://{username}:{api_key}@{container}/{file}?public=False".format(
            username="username", api_key="apikey", container="container", file="file.txt")

        storage = storagelib.get_storage(uri)
        storage.delete()

        self._assert_login_correct(
            mock_create_context, username="username", password="apikey", region="DFW",
            public=False)

    @mock.patch("pyrax.create_context")
    def test_rackspace_authenticate_with_region(self, mock_create_context):
        uri = "cloudfiles://{username}:{api_key}@{container}/{file}?region=ORD".format(
            username="username", api_key="apikey", container="container", file="file.txt")

        storage = storagelib.get_storage(uri)
        storage.delete()

        self._assert_login_correct(
            mock_create_context, username="username", password="apikey", region="ORD",
            public=True)


class TestHPCloudStorage(TestCase):

    def setUp(self):
        self.params = {
            "username": "user",
            "password": "password",
            "container": "container",
            "file": "file",
            "region": "region",
            "tenant_id": "1234567890",
            "api_key": "0987654321",
            "public": True
        }

    def test_hpcloud_scheme(self):
        # make sure "hpcloud" scheme is register appropriately
        self.assertIn("hpcloud", storagelib.storage._STORAGE_TYPES)

        # use uri without specifying auth_endpoint
        uri = "hpcloud://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "region=%(region)s&api_key=%(api_key)s&tenant_id=%(tenant_id)s" % self.params

        storage = storagelib.get_storage(uri)

        self.assertIsInstance(storage, storagelib.storage.HPCloudStorage)

    @mock.patch("pyrax.create_context")
    def test_hp_cloud_uses_default_auth_endpoint(self, mock_create_context):
        uri = "hpcloud://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "region=%(region)s&api_key=%(api_key)s&tenant_id=%(tenant_id)s" % self.params

        storage = storagelib.get_storage(uri)

        storage._authenticate()

        self.assertEqual(
            "https://region-a.geo-1.identity.hpcloudsvc.com:35357/v2.0/",
            mock_create_context.return_value.auth_endpoint)


class TestFTPStorage(TestCase):
    @mock.patch("ftplib.FTP", autospec=True)
    def test_save_to_filename(self, mock_ftp_class):
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

        mock_ftp_class.assert_called_with()
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")

        mock_ftp.cwd.assert_called_with("some/dir")
        self.assertEqual(1, mock_ftp.retrbinary.call_count)
        self.assertEqual("RETR file", mock_ftp.retrbinary.call_args[0][0])

        with open(temp_output.name) as output_fp:
            self.assertEqual("foobar", output_fp.read())

    @mock.patch("ftplib.FTP", autospec=True)
    def test_save_to_file(self, mock_ftp_class):
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

        mock_ftp_class.assert_called_with()
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")

        mock_ftp.cwd.assert_called_with("some/dir")
        self.assertEqual(1, mock_ftp.retrbinary.call_count)
        self.assertEqual("RETR file", mock_ftp.retrbinary.call_args[0][0])

        self.assertEqual("foobar", out_file.getvalue())

    @mock.patch("__builtin__.open", autospec=True)
    @mock.patch("ftplib.FTP", autospec=True)
    def test_load_from_filename(self, mock_ftp_class, mock_open):
        mock_ftp = mock_ftp_class.return_value

        storage = storagelib.get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

        storage.load_from_filename("some_file")

        mock_ftp_class.assert_called_with()
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")

        mock_ftp.cwd.assert_called_with("some/dir")

        mock_open.assert_called_with("some_file", "rb")
        mock_ftp.storbinary.assert_called_with(
            "STOR file", mock_open.return_value.__enter__.return_value)

    @mock.patch("ftplib.FTP", autospec=True)
    def test_load_from_file(self, mock_ftp_class):
        mock_ftp = mock_ftp_class.return_value
        in_file = StringIO("foobar")

        storage = storagelib.get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

        storage.load_from_file(in_file)

        mock_ftp_class.assert_called_with()
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")

        mock_ftp.cwd.assert_called_with("some/dir")

        mock_ftp.storbinary.assert_called_with("STOR file", in_file)

    @mock.patch("ftplib.FTP", autospec=True)
    def test_delete(self, mock_ftp_class):
        mock_ftp = mock_ftp_class.return_value

        storage = storagelib.get_storage("ftp://user:password@ftp.foo.com/some/dir/file")
        storage.delete()

        mock_ftp_class.assert_called_with()
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")

        mock_ftp.cwd.assert_called_with("some/dir")
        mock_ftp.delete.assert_called_with("file")

    @mock.patch("ftplib.FTP", autospec=True)
    def test_get_download_url(self, mock_ftp_class):
        mock_ftp = mock_ftp_class.return_value

        download_url_base = urllib.quote_plus("http://hostname/path/to/")

        ftpuri = "ftp://user:password@ftp.foo.com/some/dir/file.txt?download_url_base={0}".format(
            download_url_base)

        storage = storagelib.get_storage(ftpuri)
        temp_url = storage.get_download_url()

        self.assertFalse(mock_ftp_class.called)
        self.assertEqual(temp_url, "http://hostname/path/to/file.txt")

    @mock.patch("ftplib.FTP", autospec=True)
    def test_get_download_url_returns_none_with_empty_base(self, mock_ftp_class):
        mock_ftp = mock_ftp_class.return_value

        ftpuri = "ftp://user:password@ftp.foo.com/some/dir/file.txt"

        storage = storagelib.get_storage(ftpuri)

        with self.assertRaises(DownloadUrlBaseUndefinedError):
            temp_url = storage.get_download_url()

        self.assertFalse(mock_ftp_class.called)


class TestFTPSStorage(TestCase):
    @mock.patch("ftplib.FTP_TLS", autospec=True)
    def test_save_to_filename(self, mock_ftp_class):
        temp_output = tempfile.NamedTemporaryFile()

        mock_results = ["foo", "bar"]

        def mock_retrbinary(command, callback):
            for chunk in mock_results:
                callback(chunk)

            return "226"

        mock_ftp = mock_ftp_class.return_value
        mock_ftp.retrbinary.side_effect = mock_retrbinary

        storage = storagelib.get_storage("ftps://user:password@ftp.foo.com/some/dir/file")

        storage.save_to_filename(temp_output.name)

        mock_ftp_class.assert_called_with()
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")
        mock_ftp.prot_p.assert_called_with()

        mock_ftp.cwd.assert_called_with("some/dir")
        self.assertEqual(1, mock_ftp.retrbinary.call_count)
        self.assertEqual("RETR file", mock_ftp.retrbinary.call_args[0][0])

        with open(temp_output.name) as output_fp:
            self.assertEqual("foobar", output_fp.read())

    @mock.patch("ftplib.FTP_TLS", autospec=True)
    def test_save_to_file(self, mock_ftp_class):
        out_file = StringIO()

        mock_results = ["foo", "bar"]

        def mock_retrbinary(command, callback):
            for chunk in mock_results:
                callback(chunk)

            return "226"

        mock_ftp = mock_ftp_class.return_value
        mock_ftp.retrbinary.side_effect = mock_retrbinary

        storage = storagelib.get_storage("ftps://user:password@ftp.foo.com/some/dir/file")

        storage.save_to_file(out_file)

        mock_ftp_class.assert_called_with()
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")
        mock_ftp.prot_p.assert_called_with()

        mock_ftp.cwd.assert_called_with("some/dir")
        self.assertEqual(1, mock_ftp.retrbinary.call_count)
        self.assertEqual("RETR file", mock_ftp.retrbinary.call_args[0][0])

        self.assertEqual("foobar", out_file.getvalue())

    @mock.patch("__builtin__.open", autospec=True)
    @mock.patch("ftplib.FTP_TLS", autospec=True)
    def test_load_from_filename(self, mock_ftp_class, mock_open):
        mock_ftp = mock_ftp_class.return_value

        storage = storagelib.get_storage("ftps://user:password@ftp.foo.com/some/dir/file")

        storage.load_from_filename("some_file")

        mock_ftp_class.assert_called_with()
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")
        mock_ftp.prot_p.assert_called_with()

        mock_ftp.cwd.assert_called_with("some/dir")

        mock_open.assert_called_with("some_file", "rb")
        mock_ftp.storbinary.assert_called_with(
            "STOR file", mock_open.return_value.__enter__.return_value)

    @mock.patch("ftplib.FTP_TLS", autospec=True)
    def test_load_from_file(self, mock_ftp_class):
        mock_ftp = mock_ftp_class.return_value
        in_file = StringIO("foobar")

        storage = storagelib.get_storage("ftps://user:password@ftp.foo.com/some/dir/file")

        storage.load_from_file(in_file)

        mock_ftp_class.assert_called_with()
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")
        mock_ftp.prot_p.assert_called_with()

        mock_ftp.cwd.assert_called_with("some/dir")

        mock_ftp.storbinary.assert_called_with("STOR file", in_file)

    @mock.patch("ftplib.FTP_TLS", autospec=True)
    def test_delete(self, mock_ftp_class):
        mock_ftp = mock_ftp_class.return_value

        storage = storagelib.get_storage("ftps://user:password@ftp.foo.com/some/dir/file")
        storage.delete()

        mock_ftp_class.assert_called_with()
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")
        mock_ftp.prot_p.assert_called_with()

        mock_ftp.cwd.assert_called_with("some/dir")
        mock_ftp.delete.assert_called_with("file")
