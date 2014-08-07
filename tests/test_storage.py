import mock
import tempfile
from unittest import TestCase
from storage import get_storage
from StringIO import StringIO


class TestLocalStorage(TestCase):

    def test_local_storage_save_to_filename(self):
        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write("FOOBAR")
        temp_input.flush()

        temp_output = tempfile.NamedTemporaryFile()

        storage = get_storage("file://%s" % (temp_input.name))
        storage.save_to_filename(temp_output.name)

        with open(temp_output.name) as temp_output_fp:
            self.assertEqual("FOOBAR", temp_output_fp.read())

    @mock.patch("os.makedirs", autospec=True)
    def test_local_storage_load_from_filename(self, mock_makedirs):
        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write("FOOBAR")
        temp_input.flush()

        temp_output = tempfile.NamedTemporaryFile()
        storage = get_storage("file://%s" % (temp_output.name))
        storage.load_from_filename(temp_input.name)

        self.assertEqual(0, mock_makedirs.call_count)

        with open(temp_output.name) as temp_output_fp:
            self.assertEqual("FOOBAR", temp_output_fp.read())

    @mock.patch("shutil.copy", autospec=True)
    @mock.patch("os.makedirs", autospec=True)
    @mock.patch("os.path.exists", autospec=True)
    def test_load_from_file_creates_intermediate_dirs(self, mock_exists, mock_makedirs, mock_copy):
        mock_exists.return_value = False

        storage = get_storage("file:///foo/bar/file")
        storage.load_from_filename("input_file")

        mock_exists.assert_called_with("/foo/bar")
        mock_makedirs.assert_called_with("/foo/bar")
        mock_copy.assert_called_with("input_file", "/foo/bar/file")

    @mock.patch("os.remove", autospec=True)
    def test_local_storage_delete(self, mock_remove):
        storage = get_storage("file:///folder/file")
        storage.delete()

        mock_remove.assert_called_with("/folder/file")

    def test_local_storage_save_to_file(self):
        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write("FOOBAR")
        temp_input.flush()

        out_file = StringIO()

        storage = get_storage("file://%s" % (temp_input.name))
        storage.save_to_file(out_file)

        self.assertEqual("FOOBAR", out_file.getvalue())

    def test_local_storage_load_from_file(self):
        in_file = StringIO("foobar")
        temp_output = tempfile.NamedTemporaryFile()

        storage = get_storage("file://{0}".format(temp_output.name))
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

        out_storage = get_storage("file:///foobar/is/out")
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

        out_storage = get_storage("file:///foobar/is/out")
        out_storage.load_from_file(in_file)

        mock_exists.assert_called_with("/foobar/is")
        self.assertEqual(0, mock_makedirs.call_count)


class TestRackspaceStorage(TestCase):
    def _assert_login_correct(self, mock_create_context, username, password, region, public):
        mock_context = mock_create_context.return_value
        mock_create_context.assert_called_with("rackspace", username=username, password=password)
        mock_context.authenticate.assert_called_with()
        mock_context.get_client.assert_called_with("cloudfiles", region, public=public)

    @mock.patch("pyrax.create_context")
    def test_rackspace_save_to_filename(self, mock_create_context):
        mock_context = mock_create_context.return_value
        mock_cloudfiles = mock_context.get_client.return_value

        temp_output = tempfile.NamedTemporaryFile()
        mock_cloudfiles.fetch_object.return_value = ["FOOBAR", ]

        storage = get_storage("cloudfiles://user:key@container/file")
        storage.save_to_filename(temp_output.name)

        self._assert_login_correct(mock_create_context, "user", "key", "DFW", public=True)
        mock_cloudfiles.fetch_object.assert_called_with("container", "file", chunk_size=4096)

        with open(temp_output.name) as output_fp:
            self.assertEqual("FOOBAR", output_fp.read())

    @mock.patch("pyrax.create_context")
    def test_rackspace_save_to_file(self, mock_create_context):
        mock_context = mock_create_context.return_value
        mock_cloudfiles = mock_context.get_client.return_value

        mock_cloudfiles.fetch_object.return_value = iter(["foo", "bar"])

        out_file = StringIO()

        storage = get_storage("cloudfiles://user:key@container/file")
        storage.save_to_file(out_file)

        self._assert_login_correct(mock_create_context, "user", "key", "DFW", public=True)
        mock_cloudfiles.fetch_object.assert_called_with("container", "file", chunk_size=4096)

        self.assertEqual("foobar", out_file.getvalue())

    @mock.patch("pyrax.create_context")
    def test_rackspace_load_from_filename(self, mock_create_context):
        mock_cloudfiles = mock_create_context.return_value.get_client.return_value

        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write("FOOBAR")
        temp_input.flush()

        storage = get_storage("cloudfiles://user:key@container/file")
        storage.load_from_filename(temp_input.name)

        self._assert_login_correct(mock_create_context, "user", "key", "DFW", public=True)
        mock_cloudfiles.upload_file.assert_called_with("container", temp_input.name, "file")

    @mock.patch("pyrax.create_context")
    def test_rackspace_load_from_file(self, mock_create_context):
        mock_cloudfiles = mock_create_context.return_value.get_client.return_value

        mock_input = mock.Mock()

        storage = get_storage("cloudfiles://user:key@container/file")
        storage.load_from_file(mock_input)

        self._assert_login_correct(mock_create_context, "user", "key", "DFW", public=True)
        mock_cloudfiles.upload_file.assert_called_with("container", mock_input, "file")

    @mock.patch("pyrax.create_context")
    def test_rackspace_delete(self, mock_create_context):
        mock_cloudfiles = mock_create_context.return_value.get_client.return_value

        storage = get_storage("cloudfiles://user:key@container/file")
        storage.delete()

        self._assert_login_correct(mock_create_context, "user", "key", "DFW", public=True)
        mock_cloudfiles.delete_object.assert_called_with("container", "file")

    @mock.patch("pyrax.create_context")
    def test_rackspace_uses_servicenet_when_requested(self, mock_create_context):
        mock_cloudfiles = mock_create_context.return_value.get_client.return_value

        storage = get_storage("cloudfiles://user:key@container/file?public=False")
        storage.delete()

        self._assert_login_correct(mock_create_context, "user", "key", "DFW", public=False)
        mock_cloudfiles.delete_object.assert_called_with("container", "file")


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

        storage = get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

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

        storage = get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

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

        storage = get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

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

        storage = get_storage("ftp://user:password@ftp.foo.com/some/dir/file")

        storage.load_from_file(in_file)

        mock_ftp_class.assert_called_with()
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")

        mock_ftp.cwd.assert_called_with("some/dir")

        mock_ftp.storbinary.assert_called_with("STOR file", in_file)

    @mock.patch("ftplib.FTP", autospec=True)
    def test_delete(self, mock_ftp_class):
        mock_ftp = mock_ftp_class.return_value

        storage = get_storage("ftp://user:password@ftp.foo.com/some/dir/file")
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

        storage = get_storage("ftps://user:password@ftp.foo.com/some/dir/file")

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

        storage = get_storage("ftps://user:password@ftp.foo.com/some/dir/file")

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

        storage = get_storage("ftps://user:password@ftp.foo.com/some/dir/file")

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

        storage = get_storage("ftps://user:password@ftp.foo.com/some/dir/file")

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

        storage = get_storage("ftps://user:password@ftp.foo.com/some/dir/file")
        storage.delete()

        mock_ftp_class.assert_called_with()
        mock_ftp.connect.assert_called_with("ftp.foo.com", port=21)
        mock_ftp.login.assert_called_with("user", "password")
        mock_ftp.prot_p.assert_called_with()

        mock_ftp.cwd.assert_called_with("some/dir")
        mock_ftp.delete.assert_called_with("file")
