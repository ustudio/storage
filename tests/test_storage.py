import mock
import storage as storagelib
import tempfile
from unittest import TestCase
from StringIO import StringIO


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
            "public": True
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
              "&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.save_to_filename(temp_output.name)

        self._assert_login_correct(mock_create_context, username=self.params["username"],
                                   password=self.params["password"], region=self.params["region"],
                                   public=True, tenant_id=self.params["tenant_id"],
                                   api_key=self.params["api_key"])
        mock_swift.fetch_object.assert_called_with(self.params["container"],
                                                   self.params["file"], chunk_size=4096)

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
                                                   self.params["file"], chunk_size=4096)

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
        with self.assertRaises(storagelib.storage.InvalidStorageUri) as e:
            storage.save_to_filename(temp_output.name)

        # uri with missing region... should fail.
        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&tenant_id=%(tenant_id)s" % self.params
        storage = storagelib.get_storage(uri)
        with self.assertRaises(storagelib.storage.InvalidStorageUri) as e:
            storage.save_to_filename(temp_output.name)

        # uri with missing tenant_id... should fail.
        uri = "swift://%(username)s:%(password)s@%(container)s/%(file)s?" \
              "auth_endpoint=%(auth_endpoint)s&region=%(region)s" % self.params
        storage = storagelib.get_storage(uri)
        with self.assertRaises(storagelib.storage.InvalidStorageUri) as e:
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
                                                   chunk_size=4096)

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
                                                   chunk_size=4096)

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

    def test_register_swift_protocol_only_wraps_swiftstorage_subclasses(self):

        # wrapped class must be an instance of SwiftStorage
        with self.assertRaises(Exception) as e:
            @storagelib.register_swift_protocol(scheme=self.scheme,
                                                auth_endpoint=self.auth_endpoint)
            class NonStorageClass(object):
                pass


class TestRackspaceStorage(TestCase):

    def setUp(self):
        self.params = {
            "username": "user",
            "password": "password",
            "container": "container",
            "file": "file",
            "region": "DFW",
            "api_key": "0987654321",
            "public": True
        }

    def _assert_login_correct(self, mock_create_context, username, password, region, public):
        mock_context = mock_create_context.return_value
        mock_create_context.assert_called_with("rackspace", username=username, password=password)
        mock_context.authenticate.assert_called_with()
        mock_context.get_client.assert_called_with("cloudfiles", region, public=public)

    @mock.patch("pyrax.create_context")
    def test_rackspace_authenticate(self, mock_create_context):
        mock_swift = mock_create_context.return_value.get_client.return_value

        uri = "cloudfiles://%(username)s:%(api_key)s@%(container)s/%(file)s" % self.params
        storage = storagelib.get_storage(uri)
        storage.delete()

        self._assert_login_correct(mock_create_context, username=self.params["username"],
                                   password=self.params["api_key"], region=self.params["region"],
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
            "auth_endpoint": "http://identity.server.com:1234/v2/",
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
