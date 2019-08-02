import os
from io import BytesIO

import tempfile
from unittest import mock, TestCase
from urllib.parse import quote_plus

import storage as storagelib
from storage.storage import DownloadUrlBaseUndefinedError

from tests.helpers import create_temp_nested_directory_with_files, TempDirectory


class TestLocalStorage(TestCase):

    def setUp(self):
        super().setUp()
        self.temp_directory = None

    def tearDown(self):
        super().tearDown()
        if self.temp_directory is not None:
            self.temp_directory["temp_directory"]["object"].cleanup()

    def test_local_storage_save_to_filename(self):
        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write(b"FOOBAR")
        temp_input.flush()

        temp_output = tempfile.NamedTemporaryFile()

        storage = storagelib.get_storage("file://%s" % (temp_input.name))
        storage.save_to_filename(temp_output.name)

        with open(temp_output.name, "rb") as temp_output_fp:
            self.assertEqual(b"FOOBAR", temp_output_fp.read())

    @mock.patch("os.makedirs", autospec=True)
    def test_local_storage_load_from_filename(self, mock_makedirs):
        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write(b"FOOBAR")
        temp_input.flush()

        temp_output = tempfile.NamedTemporaryFile()
        storage = storagelib.get_storage("file://%s" % (temp_output.name))
        storage.load_from_filename(temp_input.name)

        self.assertEqual(0, mock_makedirs.call_count)

        with open(temp_output.name, "rb") as temp_output_fp:
            self.assertEqual(b"FOOBAR", temp_output_fp.read())

    def test_local_storage_save_to_directory(self):
        self.temp_directory = create_temp_nested_directory_with_files()

        storage = storagelib.get_storage(
            "file://{0}".format(self.temp_directory["temp_directory"]["path"]))

        with TempDirectory() as temp_output:
            temp_output_dir = temp_output.name
            destination_directory_path = os.path.join(temp_output_dir, "tmp")
            storage.save_to_directory(destination_directory_path)

            destination_input_one_path = os.path.join(
                destination_directory_path,
                self.temp_directory["temp_input_one"]["name"])
            destination_input_two_path = os.path.join(
                destination_directory_path,
                self.temp_directory["temp_input_two"]["name"])
            nested_temp_input_path = os.path.join(
                destination_directory_path,
                self.temp_directory["nested_temp_directory"]["name"],
                self.temp_directory["nested_temp_input"]["name"])

            self.assertTrue(os.path.exists(destination_input_one_path))
            self.assertTrue(os.path.exists(destination_input_two_path))
            self.assertTrue(os.path.exists(nested_temp_input_path))

            with open(destination_input_one_path, "rb") as temp_output_fp:
                self.assertEqual(b"FOO", temp_output_fp.read())

            with open(destination_input_two_path, "rb") as temp_output_fp:
                self.assertEqual(b"BAR", temp_output_fp.read())

            with open(nested_temp_input_path, "rb") as temp_output_fp:
                self.assertEqual(b"FOOBAR", temp_output_fp.read())

    def test_local_storage_load_from_directory(self):
        self.temp_directory = create_temp_nested_directory_with_files()

        with TempDirectory() as temp_output:
            temp_output_dir = temp_output.name
            storage = storagelib.get_storage("file://{0}/{1}".format(temp_output_dir, "tmp"))

            storage.load_from_directory(self.temp_directory["temp_directory"]["path"])

            destination_directory_path = os.path.join(
                temp_output_dir, "tmp")
            destination_input_one_path = os.path.join(
                temp_output_dir, destination_directory_path,
                self.temp_directory["temp_input_one"]["name"])
            destination_input_two_path = os.path.join(
                temp_output_dir, destination_directory_path,
                self.temp_directory["temp_input_two"]["name"])
            nested_temp_input_path = os.path.join(
                temp_output_dir, destination_directory_path,
                self.temp_directory["nested_temp_directory"]["path"],
                self.temp_directory["nested_temp_input"]["name"])

            self.assertTrue(os.path.exists(destination_input_one_path))
            self.assertTrue(os.path.exists(destination_input_two_path))
            self.assertTrue(os.path.exists(nested_temp_input_path))

            with open(destination_input_one_path, "rb") as temp_output_fp:
                self.assertEqual(b"FOO", temp_output_fp.read())

            with open(destination_input_two_path, "rb") as temp_output_fp:
                self.assertEqual(b"BAR", temp_output_fp.read())

            with open(nested_temp_input_path, "rb") as temp_output_fp:
                self.assertEqual(b"FOOBAR", temp_output_fp.read())

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
        self.temp_directory = create_temp_nested_directory_with_files()

        storage = storagelib.get_storage(
            "file://{0}".format(self.temp_directory["temp_directory"]["path"]))
        storage.delete_directory()

        self.assertFalse(mock_remove.called)
        mock_rmtree.assert_called_once_with(self.temp_directory["temp_directory"]["path"], True)

    def test_local_storage_save_to_file(self):
        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write(b"FOOBAR")
        temp_input.flush()

        out_file = BytesIO()

        storage = storagelib.get_storage("file://%s" % (temp_input.name))
        storage.save_to_file(out_file)

        self.assertEqual(b"FOOBAR", out_file.getvalue())

    def test_local_storage_load_from_file(self):
        in_file = BytesIO(b"foobar")
        temp_output = tempfile.NamedTemporaryFile()

        storage = storagelib.get_storage("file://{0}".format(temp_output.name))
        storage.load_from_file(in_file)

        with open(temp_output.name, "rb") as temp_output_fp:
            self.assertEqual(b"foobar", temp_output_fp.read())

    @mock.patch("os.makedirs")
    @mock.patch("os.path.exists")
    @mock.patch("builtins.open")
    def test_load_from_file_creates_dirs_if_not_present(
            self, mock_open, mock_exists, mock_makedirs):
        mock_exists.return_value = False
        in_file = BytesIO(b"foobar")

        mock_file = mock_open.return_value.__enter__.return_value
        mock_file.read.side_effect = ["FOOBAR", None]

        out_storage = storagelib.get_storage("file:///foobar/is/out")
        out_storage.load_from_file(in_file)

        mock_open.assert_has_calls([
            mock.call("/foobar/is/out", "wb")
        ])

        mock_exists.assert_called_with("/foobar/is")
        mock_makedirs.assert_called_with("/foobar/is")
        mock_file.write.assert_called_with(b"foobar")
        self.assertEqual(1, mock_open.return_value.__exit__.call_count)

    @mock.patch("os.makedirs")
    @mock.patch("os.path.exists")
    @mock.patch("builtins.open")
    def test_load_from_file_does_not_create_dirs_if_present(
            self, mock_open, mock_exists, mock_makedirs):
        mock_exists.return_value = True
        in_file = BytesIO(b"foobar")

        out_storage = storagelib.get_storage("file:///foobar/is/out")
        out_storage.load_from_file(in_file)

        mock_exists.assert_called_with("/foobar/is")
        self.assertEqual(0, mock_makedirs.call_count)

    def test_local_storage_get_download_url(self):
        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write(b"FOOBAR")
        temp_input.flush()

        download_url_base = "http://host:123/path/to/"
        download_url_base_encoded = quote_plus(download_url_base)

        storage_uri = "file://{fpath}?download_url_base={download_url_base}".format(
            fpath=temp_input.name,
            download_url_base=download_url_base_encoded)

        out_storage = storagelib.get_storage(storage_uri)
        temp_url = out_storage.get_download_url()

        self.assertEqual(
            "http://host:123/path/to/{}".format(os.path.basename(temp_input.name)), temp_url)

    def test_local_storage_get_download_url_ignores_args(self):
        temp_input = tempfile.NamedTemporaryFile()
        temp_input.write(b"FOOBAR")
        temp_input.flush()

        download_url_base = "http://host:123/path/to/"
        download_url_base_encoded = quote_plus(download_url_base)

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
        temp_input.write(b"FOOBAR")
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
